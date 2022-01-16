#pragma once

#include <vector>
#include <fmt/format.h>

#include "diskkv/region.hpp"

#include "ucsb/core/types.hpp"
#include "ucsb/core/db.hpp"
#include "ucsb/core/helper.hpp"

namespace unum {

namespace fs = ucsb::fs;

using key_t = ucsb::key_t;
using keys_spanc_t = ucsb::keys_spanc_t;
using value_span_t = ucsb::value_span_t;
using value_spanc_t = ucsb::value_spanc_t;
using values_spanc_t = ucsb::values_spanc_t;
using value_lengths_spanc_t = ucsb::value_lengths_spanc_t;
using bulk_metadata_t = ucsb::bulk_metadata_t;
using operation_status_t = ucsb::operation_status_t;
using operation_result_t = ucsb::operation_result_t;

using fingerprint_t = key_t;
using region_t = region_gt<key_t, data_source_t::unfixed_size_k>;

/**
 * @brief UnumDB wrapper for the UCSB benchmark.
 */
struct unumdb_t : public ucsb::db_t {
  public:
    inline unumdb_t() : region_("", region_config_t()) {}
    inline ~unumdb_t() { close(); }

    void set_config(fs::path const& config_path, fs::path const& dir_path) override;
    bool open() override;
    bool close() override;
    void destroy() override;

    operation_result_t insert(key_t key, value_spanc_t value) override;
    operation_result_t update(key_t key, value_spanc_t value) override;
    operation_result_t remove(key_t key) override;

    operation_result_t read(key_t key, value_span_t value) const override;
    operation_result_t batch_insert(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) override;
    operation_result_t batch_read(keys_spanc_t keys) const override;

    bulk_metadata_t prepare_bulk_import_data(keys_spanc_t keys,
                                             values_spanc_t values,
                                             value_lengths_spanc_t sizes) const override;
    operation_result_t bulk_import(bulk_metadata_t const& metadata) override;

    operation_result_t range_select(key_t key, size_t length, value_span_t single_value) const override;
    operation_result_t scan(value_span_t single_value) const override;

    size_t size_on_disk() const override;

  private:
    struct db_config_t {
        region_config_t region_config;
        string_t io_device;
        size_t uring_max_files_count = 0;
        size_t uring_queue_depth = 0;
    };

    bool load_config(db_config_t& db_config);

    fs::path config_path_;
    fs::path dir_path_;

    region_t region_;
    mutable dbuffer_t batch_buffer_;
};

void unumdb_t::set_config(fs::path const& config_path, fs::path const& dir_path) {
    config_path_ = config_path;
    dir_path_ = dir_path;
}

bool unumdb_t::open() {
    db_config_t db_config;
    if (!load_config(db_config))
        return false;

    if (db_config.io_device == string_t("libc"))
        init_file_io_by_libc(dir_path_.c_str());
    else if (db_config.io_device == string_t("pulling"))
        init_file_io_by_pulling(dir_path_.c_str(), db_config.uring_queue_depth);
    else if (db_config.io_device == string_t("polling"))
        init_file_io_by_polling(dir_path_.c_str(), db_config.uring_max_files_count, db_config.uring_queue_depth);
    else
        return false;

    region_ = region_t("Kovkas", db_config.region_config);

    return true;
}

bool unumdb_t::close() {
    if (!region_.name().empty())
        region_.flush();
    region_ = region_t("", region_config_t());
    return true;
}

void unumdb_t::destroy() {
    region_.destroy();
}

operation_result_t unumdb_t::insert(key_t key, value_spanc_t value) {
    citizen_view_t citizen {reinterpret_cast<byte_t const*>(value.data()), value.size()};
    region_.insert(key, citizen);
    return {1, operation_status_t::ok_k};
}

operation_result_t unumdb_t::update(key_t key, value_spanc_t value) {

    citizen_location_t location;
    region_.find(key, location);
    if (!location)
        return {1, operation_status_t::not_found_k};

    citizen_view_t citizen {reinterpret_cast<byte_t const*>(value.data()), value.size()};
    region_.insert(key, citizen);
    return {1, operation_status_t::ok_k};
}

operation_result_t unumdb_t::remove(key_t key) {
    region_.remove(key);
    return {1, operation_status_t::ok_k};
}

operation_result_t unumdb_t::read(key_t key, value_span_t value) const {
    citizen_location_t location;
    region_.find(key, location);
    if (!location)
        return {1, operation_status_t::not_found_k};

    countdown_t countdown;
    notifier_t read_notifier(countdown);
    citizen_span_t citizen {reinterpret_cast<byte_t*>(value.data()), value.size()};
    region_.select<caching_t::io_k>(location, citizen, read_notifier);
    if (!countdown.wait())
        return {0, operation_status_t::error_k};

    return {1, operation_status_t::ok_k};
}

operation_result_t unumdb_t::batch_insert(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) {

    // Warning: Region takes non const argument because it does inplace sorting
    span_gt<fingerprint_t> fingerprints {
        const_cast<fingerprint_t*>(reinterpret_cast<fingerprint_t const*>(keys.data())),
        keys.size()};
    span_bytes_t citizens {const_cast<byte_t*>(reinterpret_cast<byte_t const*>(values.data())), values.size()};
    span_gt<citizen_size_t> citizen_sizes {
        const_cast<citizen_size_t*>(reinterpret_cast<citizen_size_t const*>(sizes.data())),
        sizes.size()};
    region_.insert(fingerprints, citizens, citizen_sizes, ds_info_t::sorted_k);

    return {keys.size(), operation_status_t::ok_k};
}

operation_result_t unumdb_t::batch_read(keys_spanc_t keys) const {
    size_t batch_size = keys.size();
    darray_gt<fingerprint_t> fingerprints;
    fingerprints.reserve(batch_size);
    for (const auto& key : keys)
        fingerprints.push_back(key);
    size_t batch_buffer_size = 0;
    darray_gt<citizen_location_t> locations(batch_size);
    region_.find(fingerprints.view(), locations.span(), batch_buffer_size);

    if (!batch_buffer_size)
        return {batch_size, operation_status_t::not_found_k};

    if (batch_buffer_size > batch_buffer_.size())
        batch_buffer_ = dbuffer_t(batch_buffer_size);
    countdown_t countdown(locations.size());
    notifier_t notifier(countdown);
    region_.select(locations.view(), {batch_buffer_.span()}, notifier);
    if (!countdown.wait())
        return {0, operation_status_t::error_k};

    return {batch_size, operation_status_t::ok_k};
}

bulk_metadata_t unumdb_t::prepare_bulk_import_data(keys_spanc_t keys,
                                                   values_spanc_t values,
                                                   value_lengths_spanc_t sizes) const {
    bulk_metadata_t bulk_metadata;
    std::string file_path = fmt::format("/tmp/unumdb_tmp");

    bulk_metadata.files.insert(file_path);
    auto building = region_t::building_constructor_t::build("tmp", {file_path.data(), file_path.size()}, {});

    return bulk_metadata;
}

operation_result_t unumdb_t::bulk_import(bulk_metadata_t const& metadata) {
    for (auto const& file_path : metadata.files)
        region_.import({file_path.data(), file_path.size()});

    return {metadata.files.size(), operation_status_t::ok_k};
}

operation_result_t unumdb_t::range_select(key_t key, size_t length, value_span_t single_value) const {
    countdown_t countdown;
    citizen_span_t citizen {reinterpret_cast<byte_t*>(single_value.data()), single_value.size()};
    size_t selected_records_count = 0;
    auto it = region_.find(key);
    for (size_t i = 0; it != region_.end() && i < length; ++i, ++it) {
        if (!it.is_removed()) {
            countdown.reset(1);
            it.get(citizen, countdown);
            countdown.wait();
            ++selected_records_count;
        }
    }
    return {selected_records_count, operation_status_t::ok_k};
}

operation_result_t unumdb_t::scan(value_span_t single_value) const {
    countdown_t countdown;
    citizen_span_t citizen {reinterpret_cast<byte_t*>(single_value.data()), single_value.size()};
    size_t scanned_records_count = 0;
    auto it = region_.begin<caching_t::ram_k>();
    for (; it != region_.end<caching_t::ram_k>(); ++it) {
        if (!it.is_removed()) {
            countdown.reset(1);
            it.get(citizen, countdown);
            countdown.wait();
            ++scanned_records_count;
        }
    }
    return {scanned_records_count, operation_status_t::ok_k};
}

size_t unumdb_t::size_on_disk() const {
    return ucsb::size_on_disk(dir_path_);
}

bool unumdb_t::load_config(db_config_t& db_config) {
    if (!fs::exists(config_path_.c_str()))
        return false;

    std::ifstream i_config(config_path_);
    nlohmann::json j_config;
    i_config >> j_config;

    db_config.region_config.country.fixed_citizen_size = 0;
    db_config.region_config.country.migration_capacity = j_config["migration_capacity"].get<size_t>();
    db_config.region_config.country.migration_max_cnt = j_config["migration_max_cnt"].get<size_t>();

    db_config.region_config.country.city.fixed_citizen_size = 0;
    db_config.region_config.country.city.files_size_enlarge_factor =
        j_config["files_size_enlarge_factor"].get<size_t>();

    db_config.region_config.country.city.street.fixed_citizen_size = 0;
    db_config.region_config.country.city.street.max_files_cnt = j_config["max_files_cnt"].get<size_t>();
    db_config.region_config.country.city.street.files_count_enlarge_factor =
        j_config["files_count_enlarge_factor"].get<size_t>();
    db_config.region_config.country.city.street.building.fixed_citizen_size = 0;

    db_config.io_device = j_config["io_device"].get<std::string>().c_str();
    db_config.uring_max_files_count = j_config["uring_max_files_count"].get<size_t>();
    db_config.uring_queue_depth = j_config["uring_queue_depth"].get<size_t>();

    return true;
}

} // namespace unum
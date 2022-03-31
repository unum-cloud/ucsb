#pragma once

#include <vector>
#include <fmt/format.h>

#include "diskkv/region.hpp"
#include "diskkv/validator.hpp"

#include "ucsb/core/types.hpp"
#include "ucsb/core/db.hpp"
#include "ucsb/core/helper.hpp"

#include "unumdb_transaction.hpp"

namespace unum {

namespace fs = ucsb::fs;

using key_t = ucsb::key_t;
using keys_spanc_t = ucsb::keys_spanc_t;
using value_span_t = ucsb::value_span_t;
using value_spanc_t = ucsb::value_spanc_t;
using values_span_t = ucsb::values_span_t;
using values_spanc_t = ucsb::values_spanc_t;
using value_lengths_spanc_t = ucsb::value_lengths_spanc_t;
using operation_status_t = ucsb::operation_status_t;
using operation_result_t = ucsb::operation_result_t;
using transaction_t = ucsb::transaction_t;

using fingerprint_t = key_t;
using region_t = region_gt<key_t, data_source_t::unfixed_size_k>;
using building_holder_t = building_holder_gt<fingerprint_t>;

// Note: It is defined outsize of the class because object member can not be thread_local
thread_local darray_gt<building_holder_t> import_data;

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
    operation_result_t batch_read(keys_spanc_t keys, values_span_t values) const override;

    operation_result_t bulk_load(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) override;

    operation_result_t range_select(key_t key, size_t length, values_span_t values) const override;
    operation_result_t scan(key_t key, size_t length, value_span_t single_value) const override;

    void flush() override;
    size_t size_on_disk() const override;

    std::unique_ptr<transaction_t> create_transaction() override;

  private:
    struct db_config_t {
        region_config_t region_config;
        string_t io_device;
        size_t uring_max_files_count = 0;
        size_t uring_queue_depth = 0;
        darray_gt<string_t> paths;
    };

    bool load_config();

  private:
    fs::path config_path_;
    fs::path dir_path_;
    db_config_t config_;

    region_t region_;
};

void unumdb_t::set_config(fs::path const& config_path, fs::path const& dir_path) {
    config_path_ = config_path;
    dir_path_ = dir_path;
}

bool unumdb_t::open() {
    if (!load_config())
        return false;

    issues_t issues;
    if (!validator_t::validate(config_.region_config, issues))
        return false;

    for (auto const& path : config_.paths) {
        if (!fs::exists(path.c_str()))
            if (!fs::create_directory(path.c_str()))
                return false;
    }

    darray_gt<string_t> paths = config_.paths;
    if (config_.paths.empty())
        paths.push_back(dir_path_.c_str());
    if (config_.io_device == string_t("libc"))
        init_file_io_by_libc(paths);
    else if (config_.io_device == string_t("pulling"))
        init_file_io_by_pulling(paths, config_.uring_queue_depth);
    else if (config_.io_device == string_t("polling"))
        init_file_io_by_polling(paths, config_.uring_max_files_count, config_.uring_queue_depth);
    else
        return false;

    region_ = region_t("Kovkas", config_.region_config);

    // Cleanup directories
    if (!region_.population_count()) {
        for (auto const& path : config_.paths)
            ucsb::clear_directory(path.c_str());
    }

    return true;
}

bool unumdb_t::close() {
    if (raid_rand) {
        if (!region_.name().empty())
            region_.flush();
        region_ = region_t("", region_config_t());
        cleanup_file_io();
    }
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
    if (!countdown.wait(*threads_pile))
        return {0, operation_status_t::error_k};

    return {1, operation_status_t::ok_k};
}

operation_result_t unumdb_t::batch_insert(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) {

    region_.insert({keys.data(), keys.size()},
                   {reinterpret_cast<byte_t const*>(values.data()), values.size()},
                   {sizes.data(), sizes.size()});
    return {keys.size(), operation_status_t::ok_k};
}

operation_result_t unumdb_t::batch_read(keys_spanc_t keys, values_span_t values) const {

    darray_gt<fingerprint_t> fingerprints;
    fingerprints.reserve(keys.size());
    for (const auto& key : keys)
        fingerprints.push_back(key);

    size_t found_cnt = 0;
    size_t current_idx = 0;
    while (current_idx < fingerprints.size()) {
        size_t batch_size = std::min(config_.uring_queue_depth, keys.size() - current_idx);
        size_t found_buffer_size = 0;
        darray_gt<citizen_location_t> locations(batch_size);
        region_.find(fingerprints.view().subspan(current_idx, batch_size), locations.span(), found_buffer_size);

        if (found_buffer_size) {
            countdown_t countdown(batch_size);
            span_gt<byte_t> buffer_span(reinterpret_cast<byte_t*>(values.data()), found_buffer_size);
            region_.select(locations.view(), buffer_span, countdown);
            if (!countdown.wait(*threads_pile))
                return {0, operation_status_t::error_k};
            found_cnt += batch_size;
        }
        current_idx += batch_size;
    };

    if (!found_cnt)
        return {current_idx, operation_status_t::not_found_k};
    return {found_cnt, operation_status_t::ok_k};
}

operation_result_t unumdb_t::bulk_load(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) {

    static size_t building_id = 0;
    std::string file_name = fmt::format("udb_building_{}", building_id++);
    building_config_t config;
    config.capacity_bytes = values.size();
    config.elements_max_cnt = keys.size();

    // TODO: Remove const casts later
    span_gt<fingerprint_t> fingerprints {
        const_cast<fingerprint_t*>(reinterpret_cast<fingerprint_t const*>(keys.data())),
        keys.size()};
#ifdef DEV_MODE
    auto building =
        region_t::building_constructor_t::build({file_name.data(), file_name.size()},
                                                config,
                                                fingerprints,
                                                {reinterpret_cast<byte_t const*>(values.data()), values.size()},
                                                {sizes.data(), sizes.size()},
                                                ds_info_t::sorted_k);
#else
    auto building =
        region_t::building_constructor_t::build(config,
                                                fingerprints,
                                                {reinterpret_cast<byte_t const*>(values.data()), values.size()},
                                                {sizes.data(), sizes.size()},
                                                ds_info_t::sorted_k);
#endif
    auto holder = building.export_and_destroy();
    import_data.push_back(holder);

    return {keys.size(), operation_status_t::ok_k};
}

operation_result_t unumdb_t::range_select(key_t key, size_t length, values_span_t values) const {

    countdown_t countdown(0);
    notifier_t read_notifier(countdown);
    size_t selected_records_count = 0;
    size_t task_cnt = 0;
    size_t batch_size = std::min(length, config_.uring_queue_depth);

    region_.lock_shared();
    auto it = region_.find(key);
    for (size_t i = 0; it != region_.end() && i < length; ++it, ++i) {
        if (!it.is_removed()) {
            citizen_size_t citizen_size = it.size();
            citizen_span_t citizen {reinterpret_cast<byte_t*>(values.data()) + i * citizen_size, citizen_size};
            countdown.increment(1);
            it.get(citizen, countdown);
            ++task_cnt;
        }

        if ((task_cnt == batch_size) | (read_notifier.has_failed())) {
            selected_records_count += size_t(countdown.wait(*threads_pile)) * batch_size;
            batch_size = std::min(length - i + 1, config_.uring_queue_depth);
            task_cnt = 0;
        }
    }
    region_.unlock_shared();

    return {selected_records_count, operation_status_t::ok_k};
}

operation_result_t unumdb_t::scan(key_t key, size_t length, value_span_t single_value) const {

    countdown_t countdown;
    citizen_span_t citizen {reinterpret_cast<byte_t*>(single_value.data()), single_value.size()};
    size_t scanned_records_count = 0;

    region_.lock_shared();
    auto it = region_.find<caching_t::ram_k>(key);
    for (size_t i = 0; it != region_.end<caching_t::ram_k>() && i < length; ++it, ++i) {
        if (!it.is_removed()) {
            countdown.reset(1);
            it.get(citizen, countdown);
            countdown.wait(*threads_pile);
            ++scanned_records_count;
        }
    }
    region_.unlock_shared();
    return {scanned_records_count, operation_status_t::ok_k};
}

void unumdb_t::flush() {
    if (!import_data.empty()) {
        region_.import(import_data.view());
        import_data.clear();
    }
    else {
        region_.flush();
    }
}

size_t unumdb_t::size_on_disk() const {
    if (!config_.paths.empty()) {
        size_t files_size = 0;
        for (auto const& path : config_.paths) {
            if (!path.empty() && fs::exists(path.c_str()))
                files_size += ucsb::size_on_disk(path.c_str());
        }
        return files_size;
    }
    return ucsb::size_on_disk(dir_path_);
}

std::unique_ptr<transaction_t> unumdb_t::create_transaction() {
    return std::make_unique<unumdb_transaction_t>(region_.create_transaction(), config_.uring_queue_depth);
}

bool unumdb_t::load_config() {
    if (!fs::exists(config_path_))
        return false;

    std::ifstream i_config(config_path_);
    nlohmann::json j_config;
    i_config >> j_config;

    config_.region_config.default_transaction.migration_capacity =
        j_config["transaction_migration_capacity"].get<size_t>();
    config_.region_config.default_transaction.migration_max_cnt =
        j_config["transaction_migration_max_cnt"].get<size_t>();

    config_.region_config.country.threads_max_cnt = j_config["threads_max_cnt"].get<size_t>();
    config_.region_config.country.fixed_citizen_size = 0;
    config_.region_config.country.unfixed_citizen_max_size = j_config["unfixed_citizen_max_size"].get<size_t>();
    config_.region_config.country.migration_capacity = j_config["migration_capacity"].get<size_t>();
    config_.region_config.country.migration_max_cnt = j_config["migration_max_cnt"].get<size_t>();

    config_.region_config.country.city.fixed_citizen_size = config_.region_config.country.fixed_citizen_size;
    config_.region_config.country.city.street_enlarge_factor = j_config["street_enlarge_factor"].get<size_t>();

    config_.region_config.country.city.street_0.fixed_citizen_size = config_.region_config.country.fixed_citizen_size;
    config_.region_config.country.city.street_0.unfixed_citizen_max_size =
        config_.region_config.country.unfixed_citizen_max_size;
    config_.region_config.country.city.street_0.capacity_bytes = j_config["street_capacity_bytes"].get<size_t>();

    config_.region_config.country.city.street_0.building.capacity_bytes =
        config_.region_config.country.migration_capacity;
    config_.region_config.country.city.street_0.building.elements_max_cnt =
        config_.region_config.country.migration_max_cnt;
    config_.region_config.country.city.street_0.building.fixed_citizen_size =
        config_.region_config.country.fixed_citizen_size;

    config_.io_device = j_config["io_device"].get<std::string>().c_str();
    config_.uring_max_files_count = j_config["uring_max_files_count"].get<size_t>();
    config_.uring_queue_depth = j_config["uring_queue_depth"].get<size_t>();

    std::vector<std::string> paths = j_config["paths"].get<std::vector<std::string>>();
    for (auto const& path : paths) {
        if (!path.empty())
            config_.paths.push_back(path.c_str());
    }

    return true;
}

} // namespace unum
#pragma once

#include <atomic>
#include <vector>
#include <fmt/format.h>

#include "diskkv/lsm/region.hpp"
#include "diskkv/setup/validator.hpp"

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

/**
 * @brief UnumDB wrapper for the UCSB benchmark.
 */
struct unumdb_t : public ucsb::db_t {
  public:
    inline unumdb_t() : region_("", "./", region_config_t()), thread_idx_(0) {}
    inline ~unumdb_t() { close(); }

    void set_config(fs::path const& config_path, fs::path const& dir_path) override;
    bool open() override;
    bool close() override;
    void destroy() override;

    operation_result_t upsert(key_t key, value_spanc_t value) override;
    operation_result_t update(key_t key, value_spanc_t value) override;
    operation_result_t remove(key_t key) override;

    operation_result_t read(key_t key, value_span_t value) const override;
    operation_result_t batch_upsert(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) override;
    operation_result_t batch_read(keys_spanc_t keys, values_span_t values) const override;

    operation_result_t bulk_load(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) override;

    operation_result_t range_select(key_t key, size_t length, values_span_t values) const override;
    operation_result_t scan(key_t key, size_t length, value_span_t single_value) const override;

    void flush() override;
    size_t size_on_disk() const override;

    std::unique_ptr<transaction_t> create_transaction() override;

  private:
    struct db_config_t {
        user_region_config_t user_config;
        string_t io_device_name;
        size_t uring_max_files_count = 0;
        size_t uring_queue_depth = 0;
        darray_gt<string_t> paths;

        inline void clear() {
            user_config = user_region_config_t();
            io_device_name.clear();
            uring_max_files_count = 0;
            uring_queue_depth = 0;
            paths.clear();
        }
    };

    bool load_config();

  private:
    fs::path config_path_;
    fs::path dir_path_;
    db_config_t config_;

    region_t region_;

    // for every thread we use different darray to collect buildings
    darray_gt<darray_gt<building_holder_t>> import_data_;
    std::atomic_size_t thread_idx_;
};

void unumdb_t::set_config(fs::path const& config_path, fs::path const& dir_path) {
    config_path_ = config_path;
    dir_path_ = dir_path;
}

bool unumdb_t::open() {
    if (!load_config())
        return false;

    if (issues_t issues; !validator_t::validate(config_.user_config, issues))
        return false;

    countdown_t silenced_countdown(1);
    unum::io::nix::make_path(dir_path_.c_str(), silenced_countdown);
    for (auto const& path : config_.paths) {
        if (!fs::exists(path.c_str()))
            if (!fs::create_directories(path.c_str()))
                return false;
    }

    darray_gt<string_t> paths = config_.paths;
    if (config_.paths.empty())
        paths.push_back(dir_path_.c_str());
    init_file_io_by_pulling(paths, 256);
    if (config_.io_device_name == string_t("posix"))
        init_file_io_by_posix(paths);
    else if (config_.io_device_name == string_t("pulling"))
        init_file_io_by_pulling(paths, config_.uring_queue_depth);
    else
        return false;

    auto region_config = create_region_config(config_.user_config);
    region_ = region_t("Kovkas", dir_path_.c_str(), region_config);
    import_data_.resize(config_.user_config.threads_max_cnt);

    // Cleanup directories
    if (!region_.population_count()) {
        for (auto const& path : config_.paths)
            ucsb::clear_directory(path.c_str());
    }

    return true;
}

bool unumdb_t::close() {
    if (router) {
        if (!region_.name().empty())
            region_.flush();
        region_ = region_t("", "./", region_config_t());
        cleanup_file_io();
    }
    config_.clear();
    return true;
}

void unumdb_t::destroy() {
    region_.destroy();
}

operation_result_t unumdb_t::upsert(key_t key, value_spanc_t value) {
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
    region_.select(location, citizen, read_notifier);
    if (!countdown.wait(*fibers))
        return {0, operation_status_t::error_k};

    return {1, operation_status_t::ok_k};
}

operation_result_t unumdb_t::batch_upsert(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) {

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
            if (!countdown.wait(*fibers))
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

    building_config_t config;
    auto building_idx = thread_idx_.fetch_add(1);
#if dev_m
    config.city_name = string_t::format("udb_building_{}", building_idx);
    config.street_name = 0;
#endif
    config.capacity_bytes = values.size();
    config.elements_max_cnt = keys.size();

    spanc_gt<fingerprint_t> fingerprints {reinterpret_cast<fingerprint_t const*>(keys.data()), keys.size()};
    auto building =
        region_t::building_constructor_t::build(config,
                                                fingerprints,
                                                {reinterpret_cast<byte_t const*>(values.data()), values.size()},
                                                {sizes.data(), sizes.size()});
    auto holder = building.export_and_destroy();
    auto thread_idx = building_idx % import_data_.size();
    import_data_[thread_idx].push_back(holder);

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
            selected_records_count += size_t(countdown.wait(*fibers)) * batch_size;
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
    auto it = region_.find(key);
    for (size_t i = 0; it != region_.end() && i < length; ++it, ++i) {
        if (!it.is_removed()) {
            countdown.reset(1);
            it.get(citizen, countdown);
            countdown.wait(*fibers);
            ++scanned_records_count;
        }
    }
    region_.unlock_shared();
    return {scanned_records_count, operation_status_t::ok_k};
}

void unumdb_t::flush() {
    darray_gt<building_holder_t> import_data;
    import_data.reserve(thread_idx_.load());
    for (auto const& thread_import_data : import_data_)
        import_data.append(std::move(thread_import_data));

    if (!import_data.empty()) {
        std::sort(import_data.begin(), import_data.end(), [](auto const& left, auto const& right) {
            return left.schema.lower_fingerprint < right.schema.upper_fingerprint;
        });

        region_.import(import_data.view());
        import_data.clear();
        import_data_.clear();
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

    config_.user_config.txn_cache_elements_max_cnt = j_config["txn_cache_elements_max_cnt"].get<size_t>();
    config_.user_config.txn_cache_elements_capacity_bytes = j_config["txn_cache_elements_capacity_bytes"].get<size_t>();

    config_.user_config.threads_max_cnt = j_config["threads_max_cnt"].get<size_t>();
    config_.user_config.cache_elements_max_cnt = j_config["cache_elements_max_cnt"].get<size_t>();
    config_.user_config.cache_elements_capacity_bytes = j_config["cache_elements_capacity_bytes"].get<size_t>();
    config_.user_config.l0_capacity_bytes = j_config["l0_capacity_bytes"].get<size_t>();
    config_.user_config.level_enlarge_factor = j_config["level_enlarge_factor"].get<size_t>();
    config_.user_config.fixed_citizen_size = 0;
    config_.user_config.unfixed_citizen_max_size = j_config["unfixed_citizen_max_size"].get<size_t>();

#if dev_m
    config_.user_config.name = "Armenia";
#endif

    config_.io_device_name = j_config["io_device_name"].get<std::string>().c_str();
    config_.uring_max_files_count = j_config["uring_max_files_count"].get<size_t>();
    config_.uring_queue_depth = j_config["uring_queue_depth"].get<size_t>();

    std::vector<std::string> paths = j_config["paths"].get<std::vector<std::string>>();
    for (auto const& path : paths)
        if (!path.empty())
            config_.paths.push_back(path.c_str());

    return true;
}

} // namespace unum
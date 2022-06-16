#pragma once

#include <bit>
#include <atomic>
#include <memory>
#include <vector>
#include <fmt/format.h>

#include "diskkv/lsm/region.hpp"
#include "diskkv/lsm/read_ahead.hpp"
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
using region_ptr_t = std::unique_ptr<region_t>;
using building_holder_t = building_holder_gt<fingerprint_t>;

/**
 * @brief UnumDB wrapper for the UCSB benchmark.
 */
struct unumdb_t : public ucsb::db_t {
  public:
    inline unumdb_t() : thread_idx_(0) {}
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
        size_t mem_limit = 0;
        size_t gpu_mem_limit = 0;

        string_t io_device_name;
        size_t uring_max_files_count = 0;
        size_t uring_queue_depth = 0;
        darray_gt<string_t> paths;

        user_region_config_t user_config;

        inline void clear() {
            mem_limit = 0;
            gpu_mem_limit = 0;

            io_device_name.clear();
            uring_max_files_count = 0;
            uring_queue_depth = 0;
            paths.clear();

            user_config = user_region_config_t();
        }
    };

    bool load_config();

  private:
    fs::path config_path_;
    fs::path dir_path_;
    db_config_t config_;

    resources_ptr_t resources_;
    region_ptr_t region_;

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

    // Create resources
    resources_ = std::make_shared<resources_t>(config_.mem_limit, config_.gpu_mem_limit);
    resources_->fibers = std::make_shared<pool::fibers_t>(paths.size());

    if (config_.io_device_name == string_t("posix"))
        resources_->disk_router = create_posix_disk_router(paths, resources_->fibers);
    else if (config_.io_device_name == string_t("pulling"))
        resources_->disk_router = create_pulling_disk_router(paths, resources_->fibers, config_.uring_queue_depth);
    else
        return false;

    auto region_config = create_region_config(config_.user_config);
    region_ = std::make_unique<region_t>("Kovkas", dir_path_.c_str(), region_config, resources_);
    import_data_.resize(config_.user_config.threads_max_cnt);

    // Cleanup directories
    if (!region_->population_count()) {
        for (auto const& path : config_.paths)
            ucsb::clear_directory(path.c_str());
    }

    return true;
}

bool unumdb_t::close() {
    if (resources_ && region_ && !region_->name().empty())
        region_->flush();

    region_.reset();
    config_.clear();
    if (resources_) {
        resources_->disk_router.reset();
        resources_->fibers->stop_and_shutdown();
    }
    resources_.reset();

    return true;
}

void unumdb_t::destroy() {
    region_->destroy();
}

operation_result_t unumdb_t::upsert(key_t key, value_spanc_t value) {
    citizen_view_t citizen {reinterpret_cast<byte_t const*>(value.data()), value.size()};
    region_->insert(key, citizen);
    return {1, operation_status_t::ok_k};
}

operation_result_t unumdb_t::update(key_t key, value_spanc_t value) {

    citizen_location_t location;
    region_->find(key, location);
    if (!location)
        return {1, operation_status_t::not_found_k};

    citizen_view_t citizen {reinterpret_cast<byte_t const*>(value.data()), value.size()};
    region_->insert(key, citizen);
    return {1, operation_status_t::ok_k};
}

operation_result_t unumdb_t::remove(key_t key) {
    region_->remove(key);
    return {1, operation_status_t::ok_k};
}

operation_result_t unumdb_t::read(key_t key, value_span_t value) const {
    citizen_location_t location;
    region_->find(key, location);
    if (!location)
        return {1, operation_status_t::not_found_k};

    countdown_t countdown;
    notifier_t read_notifier(countdown);
    citizen_span_t citizen {reinterpret_cast<byte_t*>(value.data()), value.size()};
    region_->select(location, citizen, read_notifier);
    if (!countdown.wait(*resources_->fibers))
        return {0, operation_status_t::error_k};

    return {1, operation_status_t::ok_k};
}

operation_result_t unumdb_t::batch_upsert(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) {

    region_->insert({keys.data(), keys.size()},
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
    size_t buffer_offset = 0;
    while (current_idx < fingerprints.size()) {
        size_t batch_size = std::min(config_.uring_queue_depth, fingerprints.size() - current_idx);
        size_t found_buffer_size = 0;
        darray_gt<citizen_location_t> locations(batch_size);
        region_->find(fingerprints.view().subspan(current_idx, batch_size), locations.span(), found_buffer_size);

        if (found_buffer_size) {
            countdown_t countdown(batch_size);
            span_gt<byte_t> buffer_span(reinterpret_cast<byte_t*>(values.data()) + buffer_offset, found_buffer_size);
            region_->select(locations.view(), buffer_span, countdown);
            if (!countdown.wait(*resources_->fibers))
                return {0, operation_status_t::error_k};
            found_cnt += batch_size;
        }
        current_idx += batch_size;
        buffer_offset += found_buffer_size;
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
                                                {sizes.data(), sizes.size()},
                                                resources_);
    auto holder = building.export_and_destroy();
    auto thread_idx = building_idx % import_data_.size();
    import_data_[thread_idx].push_back(holder);

    return {keys.size(), operation_status_t::ok_k};
}

operation_result_t unumdb_t::range_select(key_t key, size_t length, values_span_t values) const {

    using read_ahead_t = read_ahead_gt<fingerprint_t, data_source_t::unfixed_size_k>;

    citizen_size_t citizen_aligned_max_size =
        ucsb::roundup_to_multiple<ucsb::values_buffer_t::alignment_k>(config_.user_config.unfixed_citizen_max_size);
    size_t const it_batch_size = std::bit_ceil(length);
    size_t const it_buffer_size = it_batch_size * citizen_aligned_max_size;

    region_->lock_shared();
    auto read_ahead_it =
        read_ahead_t {region_->find(key), it_batch_size, it_buffer_size, resources_->fibers, resources_->disk_router};
    size_t buffer_offset = 0;
    size_t scanned_records_count = 0;
    while (read_ahead_it != end_sentinel_t {} && scanned_records_count != length) {
        auto current = *read_ahead_it;
        memcpy(values.data() + buffer_offset, current.second.data(), current.second.size());
        buffer_offset += citizen_aligned_max_size;
        ++read_ahead_it;
        ++scanned_records_count;
    }

    region_->unlock_shared();
    return {scanned_records_count, operation_status_t::ok_k};
}

operation_result_t unumdb_t::scan(key_t key, size_t length, value_span_t single_value) const {

    using read_ahead_t = read_ahead_gt<fingerprint_t, data_source_t::unfixed_size_k>;

    citizen_size_t citizen_aligned_max_size =
        ucsb::roundup_to_multiple<ucsb::values_buffer_t::alignment_k>(config_.user_config.unfixed_citizen_max_size);

    size_t const it_batch_size = std::bit_ceil(std::min(length, 1024ul));
    size_t const it_buffer_size = it_batch_size * citizen_aligned_max_size;

    region_->lock_shared();
    auto read_ahead_it =
        read_ahead_t {region_->find(key), it_batch_size, it_buffer_size, resources_->fibers, resources_->disk_router};
    size_t scanned_records_count = 0;
    while (read_ahead_it != end_sentinel_t {} && scanned_records_count != length) {
        auto current = *read_ahead_it;
        memcpy(single_value.data(), current.second.data(), current.second.size());
        ++read_ahead_it;
        ++scanned_records_count;
    }

    region_->unlock_shared();
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

        region_->import(import_data.view());
        import_data.clear();
        import_data_.clear();
    }
    else {
        region_->flush();
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
    return std::make_unique<unumdb_transaction_t>(region_->create_transaction(), config_.uring_queue_depth, resources_);
}

bool unumdb_t::load_config() {
    if (!fs::exists(config_path_))
        return false;

    std::ifstream i_config(config_path_);
    nlohmann::json j_config;
    i_config >> j_config;

    // Resource limits
    config_.mem_limit = j_config["mem_limit"].get<size_t>();
    config_.gpu_mem_limit = j_config["gpu_mem_limit"].get<size_t>();

    // Disk config
    config_.io_device_name = j_config["io_device_name"].get<std::string>().c_str();
    config_.uring_max_files_count = j_config["uring_max_files_count"].get<size_t>();
    config_.uring_queue_depth = j_config["uring_queue_depth"].get<size_t>();
    std::vector<std::string> paths = j_config["paths"].get<std::vector<std::string>>();
    for (auto const& path : paths)
        if (!path.empty())
            config_.paths.push_back(path.c_str());

    // Transaction config
    config_.user_config.txn_cache_elements_max_cnt = j_config["txn_cache_elements_max_cnt"].get<size_t>();
    config_.user_config.txn_cache_elements_capacity_bytes = j_config["txn_cache_elements_capacity_bytes"].get<size_t>();

    // Region config
    config_.user_config.threads_max_cnt = j_config["threads_max_cnt"].get<size_t>();
    config_.user_config.cache_elements_max_cnt = j_config["cache_elements_max_cnt"].get<size_t>();
    config_.user_config.cache_elements_capacity_bytes = j_config["cache_elements_capacity_bytes"].get<size_t>();
    config_.user_config.l0_capacity_bytes = j_config["l0_capacity_bytes"].get<size_t>();
    config_.user_config.level_enlarge_factor = j_config["level_enlarge_factor"].get<size_t>();
    config_.user_config.fixed_citizen_size = 0;
    config_.user_config.unfixed_citizen_max_size = j_config["unfixed_citizen_max_size"].get<size_t>();

#if dev_m
    config_.user_config.name = "Kovkas";
#endif

    return true;
}

} // namespace unum
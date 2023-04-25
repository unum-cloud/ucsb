#pragma once

#include <cassert>
#include <string>
#include <fstream>
#include <streambuf>

#include <ustore/ustore.h>
#include <ustore/cpp/status.hpp>
#include <ustore/cpp/types.hpp>
#include <helpers/config_loader.hpp>

#include "src/core/db.hpp"
#include "src/core/helper.hpp"
#include "src/core/types.hpp"

#include "ustore_transaction.hpp"

namespace ucsb::ustore {

namespace fs = ucsb::fs;
namespace ustore = unum::ustore;

using key_t = ucsb::key_t;
using keys_spanc_t = ucsb::keys_spanc_t;
using value_span_t = ucsb::value_span_t;
using value_spanc_t = ucsb::value_spanc_t;
using values_span_t = ucsb::values_span_t;
using values_spanc_t = ucsb::values_spanc_t;
using value_lengths_spanc_t = ucsb::value_lengths_spanc_t;
using operation_status_t = ucsb::operation_status_t;
using operation_result_t = ucsb::operation_result_t;
using db_hints_t = ucsb::db_hints_t;
using transaction_t = ucsb::transaction_t;

struct client_t {
    ustore_database_t db = nullptr;
    ustore_arena_t memory = nullptr;

    operator bool() const noexcept { return db != nullptr; }
};

class ustore_t : public ucsb::db_t {
  public:
    inline ustore_t() = default;
    ~ustore_t() { free(); }

    void set_config(fs::path const& config_path,
                    fs::path const& main_dir_path,
                    std::vector<fs::path> const& storage_dir_paths,
                    db_hints_t const& hints) override;
    bool open(std::string& error) override;
    void close() override;

    std::string info() override;

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
    void free();
    inline void map_client() const;

    fs::path config_path_;
    fs::path main_dir_path_;
    std::vector<fs::path> storage_dir_paths_;
    db_hints_t hints_;

    std::vector<client_t> clients_;
    static thread_local client_t client_;
    static std::atomic_size_t client_index_;
    ustore_collection_t collection_ = ustore_collection_main_k;
    ustore_options_t options_ = ustore_options_default_k;
};

thread_local client_t ustore_t::client_;
std::atomic_size_t ustore_t::client_index_ = 0;

void ustore_t::set_config(fs::path const& config_path,
                          fs::path const& main_dir_path,
                          std::vector<fs::path> const& storage_dir_paths,
                          db_hints_t const& hints) {
    config_path_ = config_path;
    main_dir_path_ = main_dir_path;
    storage_dir_paths_ = storage_dir_paths;
    hints_ = hints;
}

bool ustore_t::open(std::string& error) {
    if (client_)
        return true;

    // Read config from file
    std::ifstream stream(config_path_);
    if (!stream) {
        error = strerror(errno);
        return false;
    }
    std::string str_config((std::istreambuf_iterator<char>(stream)), std::istreambuf_iterator<char>());

    // Load and overwrite
    ustore::config_t config;
    auto status = ustore::config_loader_t::load_from_json_string(str_config, config, true);
    if (!status) {
        error = status.message();
        return false;
    }

    // Resolve directory paths
    if (config.directory.empty())
        config.directory = main_dir_path_;
    if (config.data_directories.empty()) {
#if defined(USTORE_ENGINE_IS_ROCKSDB)
        for (auto const& dir : storage_dir_paths_) {
            size_t storage_size_on_disk = (hints_.records_count * hints_.value_length) / storage_dir_paths_.size();
            config.data_directories.push_back({dir, storage_size_on_disk});
        }
#else
        for (auto const& dir : storage_dir_paths_)
            config.data_directories.push_back({dir, ustore::disk_config_t::unlimited_space_k});
#endif
    }

    // Resolve engine config path
    if (config.engine.config_file_path.empty()) {
        auto configs_root = config_path_.parent_path().parent_path();
        if (configs_root.filename() != "configs") {
            error = "Invalid configs directory";
            return false;
        }
        auto config_file_path = configs_root / USTORE_ENGINE_NAME / config_path_.filename();
        if (fs::exists(config_file_path))
            config.engine.config_file_path = config_file_path;
        else {
            // Select default config if the specified doesn't exist
            config_file_path = fs::path(config_file_path).parent_path() / "default.cfg";
            if (fs::exists(config_file_path))
                config.engine.config_file_path = config_file_path;
        }
    }

    // Convert to json string
    status = ustore::config_loader_t::save_to_json_string(config, str_config);
    if (!status) {
        error = status.message();
        return false;
    }

    ustore_database_init_t init {};
    init.config = str_config.c_str();
    init.error = status.member_ptr();
    std::size_t clients_count = 1;

#if defined(USTORE_ENGINE_IS_FLIGHT_CLIENT)
    init.config = "grpc://0.0.0.0:38709";
    clients_count = hints_.threads_count;
#endif
    clients_.resize(clients_count);
    for (std::size_t i = 0; i < clients_count; ++i) {
        init.db = &clients_[i].db;
        ustore_database_init(&init);
        if (!status) {
            error = status.message();
            return status;
        }
    }
    return true;
}

void ustore_t::close() {
    client_index_.store(0);
#if !defined(USTORE_ENGINE_IS_UCSET)
    free();
#endif
}

void ustore_t::free() {
    for (std::size_t i = 0; i < clients_.size(); ++i) {
        ustore_arena_free(clients_[i].memory);
        ustore_database_free(clients_[i].db);
    }
    clients_.clear();
    client_.db = nullptr;
    client_.memory = nullptr;
}

inline void ustore_t::map_client() const {
    if (!client_) [[unlikely]] {
        std::size_t index = 0;
#if defined(USTORE_ENGINE_IS_FLIGHT_CLIENT)
        index = client_index_.fetch_add(1);
#endif
        client_ = clients_[index];
    }
}

operation_result_t ustore_t::upsert(key_t key, value_spanc_t value) {
    map_client();

    ustore::status_t status;
    ustore_key_t key_ = key;
    ustore_length_t length = value.size();
    auto value_ = make_value(value.data(), value.size());

    ustore_write_t write {};
    write.db = client_.db;
    write.error = status.member_ptr();
    write.arena = &client_.memory;
    write.options = options_;
    write.tasks_count = 1;
    write.collections = &collection_;
    write.keys = &key_;
    write.lengths = reinterpret_cast<ustore_length_t const*>(&length);
    write.values = value_.member_ptr();
    ustore_write(&write);

    return {size_t(status), status ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t ustore_t::update(key_t key, value_spanc_t value) {
    map_client();

    ustore::status_t status;
    ustore_key_t key_ = key;
    ustore_byte_t* value_ = nullptr;

    ustore_read_t read {};
    read.db = client_.db;
    read.error = status.member_ptr();
    read.arena = &client_.memory;
    read.options = options_;
    read.tasks_count = 1;
    read.collections = &collection_;
    read.keys = &key_;
    read.values = &value_;
    ustore_read(&read);
    if (!status)
        return {0, operation_status_t::not_found_k};

    return upsert(key, value);
}

operation_result_t ustore_t::remove(key_t key) {
    map_client();

    ustore::status_t status;
    ustore_key_t key_ = key;

    ustore_write_t write {};
    write.db = client_.db;
    write.error = status.member_ptr();
    write.arena = &client_.memory;
    write.options = options_;
    write.tasks_count = 1;
    write.collections = &collection_;
    write.keys = &key_;
    ustore_write(&write);

    return {status ? size_t(1) : 0, status ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t ustore_t::read(key_t key, value_span_t value) const {
    map_client();

    ustore::status_t status;
    ustore_key_t key_ = key;
    ustore_byte_t* value_ = nullptr;
    ustore_length_t* lengths = nullptr;

    ustore_read_t read {};
    read.db = client_.db;
    read.error = status.member_ptr();
    read.arena = &client_.memory;
    read.options = options_;
    read.tasks_count = 1;
    read.collections = &collection_;
    read.keys = &key_;
    read.lengths = &lengths;
    read.values = &value_;
    ustore_read(&read);
    if (!status)
        return {0, operation_status_t::error_k};
    if (lengths[0] == ustore_length_missing_k)
        return {0, operation_status_t::not_found_k};

    memcpy(value.data(), value_, lengths[0]);
    return {1, operation_status_t::ok_k};
}

operation_result_t ustore_t::batch_upsert(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) {
    map_client();

    ustore::status_t status;
    std::vector<ustore_length_t> offsets;
    offsets.reserve(sizes.size() + 1);
    offsets.push_back(0);
    for (auto size : sizes)
        offsets.push_back(offsets.back() + size);

    auto values_ = make_value(values.data(), values.size());
    ustore_write_t write {};
    write.db = client_.db;
    write.error = status.member_ptr();
    write.arena = &client_.memory;
    write.options = options_;
    write.tasks_count = keys.size();
    write.collections = &collection_;
    write.keys = reinterpret_cast<ustore_key_t const*>(keys.data());
    write.keys_stride = sizeof(ustore_key_t);
    write.offsets = offsets.data();
    write.offsets_stride = sizeof(ustore_length_t);
    write.lengths = reinterpret_cast<ustore_length_t const*>(sizes.data());
    write.lengths_stride = sizeof(ustore_length_t);
    write.values = values_.member_ptr();
    ustore_write(&write);

    return {status ? keys.size() : 0, status ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t ustore_t::batch_read(keys_spanc_t keys, values_span_t values) const {
    map_client();

    ustore::status_t status;
    ustore_octet_t* presences = nullptr;
    ustore_length_t* offsets = nullptr;
    ustore_length_t* lengths = nullptr;
    ustore_byte_t* values_ = nullptr;

    ustore_read_t read {};
    read.db = client_.db;
    read.error = status.member_ptr();
    read.arena = &client_.memory;
    read.options = options_;
    read.tasks_count = keys.size();
    read.collections = &collection_;
    read.keys = reinterpret_cast<ustore_key_t const*>(keys.data());
    read.keys_stride = sizeof(ustore_key_t);
    read.presences = &presences;
    read.offsets = &offsets;
    read.lengths = &lengths;
    read.values = &values_;
    ustore_read(&read);
    if (!status)
        return {0, operation_status_t::error_k};

    size_t offset = 0;
    size_t found_cnt = 0;
    for (size_t idx = 0; idx < keys.size(); ++idx) {
        if (lengths[idx] == ustore_length_missing_k)
            continue;
        memcpy(values.data() + offset, values_ + offsets[idx], lengths[idx]);
        offset += lengths[idx];
        ++found_cnt;
    }

    return {found_cnt, found_cnt > 0 ? operation_status_t::ok_k : operation_status_t::not_found_k};
}

operation_result_t ustore_t::bulk_load(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) {
    return batch_upsert(keys, values, sizes);
}

operation_result_t ustore_t::range_select(key_t key, size_t length, values_span_t values) const {
    map_client();

    ustore::status_t status;
    ustore_key_t key_ = key;
    ustore_length_t len = length;
    ustore_length_t* found_counts = nullptr;
    ustore_key_t* found_keys = nullptr;

    // First scan keys
    ustore_scan_t scan {};
    scan.db = client_.db;
    scan.error = status.member_ptr();
    scan.arena = &client_.memory;
    scan.options = options_;
    scan.tasks_count = 1;
    scan.collections = &collection_;
    scan.start_keys = &key_;
    scan.count_limits = &len;
    scan.counts = &found_counts;
    scan.keys = &found_keys;
    ustore_scan(&scan);
    if (!status)
        return {0, operation_status_t::error_k};

    ustore_length_t* offsets = nullptr;
    ustore_length_t* lengths = nullptr;
    ustore_byte_t* values_ = nullptr;

    // Then do batch read
    ustore_read_t read {};
    read.db = client_.db;
    read.error = status.member_ptr();
    read.arena = &client_.memory;
    read.options = ustore_options_t(options_ | ustore_option_dont_discard_memory_k);
    read.tasks_count = *found_counts;
    read.collections = &collection_;
    read.keys = found_keys;
    read.keys_stride = sizeof(ustore_key_t);
    read.offsets = &offsets;
    read.lengths = &lengths;
    read.values = &values_;
    ustore_read(&read);
    if (!status)
        return {0, operation_status_t::error_k};

    size_t offset = 0;
    for (size_t idx = 0; idx < *found_counts; ++idx) {
        if (lengths[idx] == ustore_length_missing_k)
            continue;
        memcpy(values.data() + offset, values_ + offsets[idx], lengths[idx]);
        offset += lengths[idx];
    }

    return {*found_counts, *found_counts > 0 ? operation_status_t::ok_k : operation_status_t::not_found_k};
}

operation_result_t ustore_t::scan(key_t key, size_t length, value_span_t single_value) const {
    map_client();

    ustore::status_t status;
    ustore_key_t key_ = key;
    ustore_length_t len =
        std::min<ustore_length_t>(length, 1'000'000); // Note: Don't scan all at once because the DB might be very big
    ustore_length_t* found_counts = nullptr;
    ustore_key_t* found_keys = nullptr;

    ustore_length_t* offsets = nullptr;
    ustore_length_t* lengths = nullptr;
    ustore_byte_t* values_ = nullptr;

    // Init scan
    ustore_scan_t scan {};
    scan.db = client_.db;
    scan.error = status.member_ptr();
    scan.arena = &client_.memory;
    scan.options = options_;
    scan.tasks_count = 1;
    scan.collections = &collection_;
    scan.start_keys = &key_;
    scan.count_limits = &len;
    scan.counts = &found_counts;
    scan.keys = &found_keys;

    // Init batch read
    ustore_read_t read {};
    read.db = client_.db;
    read.error = status.member_ptr();
    read.arena = &client_.memory;
    read.options = ustore_options_t(options_ | ustore_option_dont_discard_memory_k);
    read.collections = &collection_;
    read.keys_stride = sizeof(ustore_key_t);
    read.offsets = &offsets;
    read.lengths = &lengths;
    read.values = &values_;

    ustore_length_t scanned = 0;
    ustore_length_t remaining_keys_cnt = length;
    while (remaining_keys_cnt) {
        // First scan
        ustore_scan(&scan);
        if (!status)
            return {0, operation_status_t::error_k};

        // Then read
        read.tasks_count = *found_counts;
        read.keys = found_keys;
        ustore_read(&read);
        if (!status)
            return {0, operation_status_t::error_k};

        scanned += *found_counts;
        for (size_t idx = 0; idx < *found_counts; ++idx)
            if (lengths[idx] != ustore_length_missing_k)
                memcpy(single_value.data(), values_ + offsets[idx], lengths[idx]);

        key_ += len;
        remaining_keys_cnt = remaining_keys_cnt - len;
        len = std::min(len, remaining_keys_cnt);
    }

    return {scanned, scanned > 0 ? operation_status_t::ok_k : operation_status_t::not_found_k};
}

std::string ustore_t::info() { return fmt::format("v{}, {}", USTORE_VERSION, USTORE_ENGINE_NAME); }

void ustore_t::flush() {
    map_client();

    // Note: Workaround for flush
    ustore::status_t status;
    ustore_write_t write {};
    write.db = client_.db;
    write.error = status.member_ptr();
    write.arena = &client_.memory;
    write.options = ustore_options_t(options_ | ustore_option_write_flush_k);
    write.tasks_count = 0;
    write.collections = &collection_;
    write.keys = nullptr;
    write.lengths = nullptr;
    write.values = nullptr;
    ustore_write(&write);
}

size_t ustore_t::size_on_disk() const {
    size_t files_size = ucsb::size_on_disk(main_dir_path_);
    for (auto const& db_path : storage_dir_paths_) {
        if (fs::exists(db_path))
            files_size += ucsb::size_on_disk(db_path);
    }
    return files_size;
}

std::unique_ptr<transaction_t> ustore_t::create_transaction() {
    map_client();

    ustore::status_t status;
    ustore_transaction_t transaction {};

    ustore_transaction_init_t txn_init {};
    txn_init.db = client_.db;
    txn_init.error = status.member_ptr();
    txn_init.transaction = &transaction;
    ustore_transaction_init(&txn_init);
    if (status)
        return std::make_unique<ustore_transact_t>(client_.db, transaction);

    return {};
}

} // namespace ucsb::ustore
#pragma once

#include <cassert>
#include <string>
#include <fstream>
#include <streambuf>

#include <ukv/cpp/status.hpp>
#include <ukv/cpp/types.hpp>
#include <ukv/ukv.h>
#include <helpers/config_loader.hpp>

#include "src/core/db.hpp"
#include "src/core/helper.hpp"
#include "src/core/types.hpp"
namespace ucsb::ukv {

namespace fs = ucsb::fs;
using namespace unum::ukv;

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

thread_local arena_t arena(nullptr);

class ukv_t : public ucsb::db_t {
  public:
    inline ukv_t() : db_(nullptr) {}
    ~ukv_t() { free(); }
    void set_config(fs::path const& config_path,
                    fs::path const& main_dir_path,
                    std::vector<fs::path> const& storage_dir_paths,
                    db_hints_t const& hints) override;
    bool open() override;
    bool close() override;

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
    inline value_view_t make_value(std::byte const* ptr, size_t length) {
        return {reinterpret_cast<ukv_bytes_cptr_t>(ptr), static_cast<ukv_length_t>(length)};
    }

    void free();

    fs::path config_path_;
    fs::path main_dir_path_;
    std::vector<fs::path> storage_dir_paths_;
    db_hints_t hints_;

    ukv_database_t db_;
    ukv_collection_t collection_ = ukv_collection_main_k;
    ukv_options_t option = ukv_option_dont_discard_memory_k;
};

void ukv_t::set_config(fs::path const& config_path,
                       fs::path const& main_dir_path,
                       std::vector<fs::path> const& storage_dir_paths,
                       db_hints_t const& hints) {
    config_path_ = config_path;
    main_dir_path_ = main_dir_path;
    storage_dir_paths_ = storage_dir_paths;
    hints_ = hints;
}

bool ukv_t::open() {
    if (db_)
        return true;

    // Read config from file
    std::ifstream stream(config_path_);
    if (!stream)
        return false;
    std::string str_config((std::istreambuf_iterator<char>(stream)), std::istreambuf_iterator<char>());

    // Load and overwrite
    config_t config;
    auto status = config_loader_t::load_from_json_string(str_config, config);
    if (!status)
        return false;
    config.directory = main_dir_path_;
    config.data_directories.clear();
#if defined(UKV_ENGINE_IS_ROCKSDB) || defined(UKV_ENGINE_IS_UDISK)
    for (auto const& dir : storage_dir_paths_) {
        size_t storage_size_on_disk = (hints_.records_count * hints_.value_length) / storage_dir_paths_.size();
        config.data_directories.push_back({dir, storage_size_on_disk});
    }
#endif
    config.engine_config_path = ""; // !!!TODO: Think solution

    // Convert to json string
    status = config_loader_t::save_to_json_string(config, str_config);
    if (!status)
        return false;

    ukv_database_init_t init {};
    init.config = str_config.c_str();
    init.db = &db_;
    init.error = status.member_ptr();
    ukv_database_init(&init);
    if (!status)
        return status;

    arena = arena_t(db_);
    return true;
}

void ukv_t::free() {
    ukv_database_free(db_);
    db_ = nullptr;
}

bool ukv_t::close() {
#if !defined(UKV_ENGINE_IS_UMEM)
    free();
#endif
    return true;
}

operation_result_t ukv_t::upsert(key_t key, value_spanc_t value) {
    status_t status;
    ukv_key_t key_ = key;
    ukv_length_t length = value.size();
    auto value_ = make_value(value.data(), value.size());

    ukv_write_t write {};
    write.db = db_;
    write.error = status.member_ptr();
    write.arena = arena.member_ptr();
    write.options = option;
    write.tasks_count = 1;
    write.collections = &collection_;
    write.keys = &key_;
    write.lengths = reinterpret_cast<ukv_length_t const*>(&length);
    write.values = value_.member_ptr();
    ukv_write(&write);
    return {size_t(status), status ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t ukv_t::update(key_t key, value_spanc_t value) {
    status_t status;
    ukv_key_t key_ = key;
    ukv_byte_t* value_ = nullptr;

    ukv_read_t read {};
    read.db = db_;
    read.error = status.member_ptr();
    read.arena = arena.member_ptr();
    read.options = option;
    read.tasks_count = 1;
    read.collections = &collection_;
    read.keys = &key_;
    read.values = &value_;
    ukv_read(&read);
    if (!status)
        return {0, operation_status_t::not_found_k};

    return upsert(key, value);
}

operation_result_t ukv_t::remove(key_t key) {
    status_t status;
    ukv_key_t key_ = key;

    ukv_write_t write {};
    write.db = db_;
    write.error = status.member_ptr();
    write.arena = arena.member_ptr();
    write.options = option;
    write.tasks_count = 1;
    write.collections = &collection_;
    write.keys = &key_;
    ukv_write(&write);

    return {status ? size_t(1) : 0, status ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t ukv_t::read(key_t key, value_span_t value) const {
    status_t status;
    ukv_key_t key_ = key;
    ukv_byte_t* value_ = nullptr;
    ukv_length_t* lengths = nullptr;

    ukv_read_t read {};
    read.db = db_;
    read.error = status.member_ptr();
    read.arena = arena.member_ptr();
    read.options = option;
    read.tasks_count = 1;
    read.collections = &collection_;
    read.keys = &key_;
    read.lengths = &lengths;
    read.values = &value_;
    ukv_read(&read);
    if (!status)
        return {0, operation_status_t::error_k};

    if (lengths[0] == ukv_length_missing_k)
        return {0, operation_status_t::not_found_k};
    memcpy(value.data(), value_, lengths[0]);
    return {1, operation_status_t::ok_k};
}

operation_result_t ukv_t::batch_upsert(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) {
    status_t status;
    std::vector<ukv_length_t> offsets;
    offsets.reserve(sizes.size() + 1);
    offsets.push_back(0);
    size_t offset = 0;
    for (auto size : sizes) {
        offset += size;
        offsets.push_back(offset);
    }

    auto val = make_value(values.data(), values.size());
    ukv_write_t write {};
    write.db = db_;
    write.error = status.member_ptr();
    write.arena = arena.member_ptr();
    write.options = option;
    write.tasks_count = keys.size();
    write.collections = &collection_;
    write.keys = reinterpret_cast<ukv_key_t const*>(keys.data());
    write.keys_stride = sizeof(ukv_key_t);
    write.offsets = offsets.data();
    write.offsets_stride = sizeof(ukv_length_t);
    write.lengths = reinterpret_cast<ukv_length_t const*>(sizes.data());
    write.lengths_stride = sizeof(ukv_length_t);
    write.values = val.member_ptr();
    ukv_write(&write);

    return {status ? keys.size() : 0, status ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t ukv_t::batch_read(keys_spanc_t keys, values_span_t values) const {
    status_t status;
    ukv_byte_t* values_ = nullptr;
    ukv_length_t* lengths = nullptr;
    ukv_length_t* offsets = nullptr;
    ukv_octet_t* presences = nullptr;
    size_t found_cnt = 0;

    ukv_read_t read {};
    read.db = db_;
    read.error = status.member_ptr();
    read.arena = arena.member_ptr();
    read.options = option;
    read.tasks_count = keys.size();
    read.collections = &collection_;
    read.keys = reinterpret_cast<ukv_key_t const*>(keys.data());
    read.keys_stride = sizeof(ukv_key_t);
    read.presences = &presences;
    read.offsets = &offsets;
    read.lengths = &lengths;
    read.values = &values_;
    ukv_read(&read);
    if (!status)
        return {0, operation_status_t::error_k};

    for (size_t idx = 0; idx < keys.size(); ++idx) {

        if (!presences[idx])
            continue;

        memcpy(values.data() + offsets[idx], values_ + offsets[idx], lengths[idx]);
        ++found_cnt;
    }
    return {found_cnt, operation_status_t::ok_k};
}

operation_result_t ukv_t::bulk_load(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) {
    return batch_upsert(keys, values, sizes);
}

operation_result_t ukv_t::range_select(key_t key, size_t length, values_span_t values) const {
    status_t status;

    ukv_key_t key_ = key;
    ukv_length_t len = length;
    ukv_length_t* found_counts = nullptr;
    ukv_key_t* found_keys = nullptr;

    ukv_scan_t scan {};
    scan.db = db_;
    scan.error = status.member_ptr();
    scan.arena = arena.member_ptr();
    scan.options = option;
    scan.tasks_count = 1;
    scan.collections = &collection_;
    scan.start_keys = &key_;
    scan.count_limits = &len;
    scan.counts = &found_counts;
    scan.keys = &found_keys;
    ukv_scan(&scan);
    if (!status)
        return {0, operation_status_t::error_k};

    ukv_length_t* offsets = nullptr;
    ukv_length_t* lengths = nullptr;
    ukv_byte_t* values_ = nullptr;
    size_t offset = 0;

    ukv_read_t read {};
    read.db = db_;
    read.error = status.member_ptr();
    read.arena = arena.member_ptr();
    read.options = option;
    read.tasks_count = *found_counts;
    read.collections = &collection_;
    read.keys = found_keys;
    read.keys_stride = sizeof(ukv_key_t);
    read.offsets = &offsets;
    read.lengths = &lengths;
    read.values = &values_;
    ukv_read(&read);

    for (size_t idx = 0; idx < *found_counts; ++idx) {
        if (lengths[idx] == ukv_length_missing_k)
            continue;
        memcpy(values.data() + offset, values_ + offsets[idx], lengths[idx]);
        offset += lengths[idx];
    }
    return {*found_counts, operation_status_t::ok_k};
}

operation_result_t ukv_t::scan(key_t key, size_t length, value_span_t single_value) const {
    status_t status;

    ukv_key_t key_ = key;
    ukv_length_t len = length;
    ukv_length_t* found_counts = nullptr;
    ukv_key_t* found_keys = nullptr;

    ukv_scan_t scan {};
    scan.db = db_;
    scan.error = status.member_ptr();
    scan.arena = arena.member_ptr();
    scan.options = option;
    scan.tasks_count = 1;
    scan.collections = &collection_;
    scan.start_keys = &key_;
    scan.count_limits = &len;
    scan.counts = &found_counts;
    scan.keys = &found_keys;
    ukv_scan(&scan);
    if (!status)
        return {0, operation_status_t::error_k};

    ukv_length_t* offsets = nullptr;
    ukv_length_t* lengths = nullptr;
    ukv_byte_t* values_ = nullptr;

    ukv_read_t read {};
    read.db = db_;
    read.error = status.member_ptr();
    read.arena = arena.member_ptr();
    read.options = option;
    read.tasks_count = *found_counts;
    read.collections = &collection_;
    read.keys = found_keys;
    read.keys_stride = sizeof(ukv_key_t);
    read.offsets = &offsets;
    read.lengths = &lengths;
    read.values = &values_;
    ukv_read(&read);

    for (size_t idx = 0; idx < *found_counts; ++idx) {
        if (lengths[idx] == ukv_length_missing_k)
            continue;
        memcpy(single_value.data(), values_ + offsets[idx], lengths[idx]);
    }
    return {*found_counts, operation_status_t::ok_k};
}

void ukv_t::flush() {
    // !!!TODO: Think solution
}

size_t ukv_t::size_on_disk() const {
    size_t files_size = ucsb::size_on_disk(main_dir_path_);
    for (auto const& db_path : storage_dir_paths_) {
        if (fs::exists(db_path))
            files_size += ucsb::size_on_disk(db_path);
    }
    return files_size;
}

std::unique_ptr<transaction_t> ukv_t::create_transaction() { return {}; }

} // namespace ucsb::ukv
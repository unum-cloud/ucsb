#pragma once

#include <iostream>
#include <string>
#include <fmt/format.h>
#include <nlohmann/json.hpp>

#include <leveldb/options.h>
#include <leveldb/write_batch.h>
#include <leveldb/db.h>
#include <leveldb/status.h>
#include <leveldb/cache.h>
#include <leveldb/filter_policy.h>

#include "ucsb/core/types.hpp"
#include "ucsb/core/db.hpp"

namespace google {

namespace fs = ucsb::fs;

using key_t = ucsb::key_t;
using keys_span_t = ucsb::keys_span_t;
using value_span_t = ucsb::value_span_t;
using value_spanc_t = ucsb::value_spanc_t;
using operation_status_t = ucsb::operation_status_t;
using operation_result_t = ucsb::operation_result_t;

struct leveldb_t : public ucsb::db_t {
  public:
    inline leveldb_t() : db_(nullptr) {}
    ~leveldb_t() override = default;

    bool init(fs::path const& config_path, fs::path const& dir_path) override;
    void destroy() override;

    operation_result_t insert(key_t key, value_spanc_t value) override;
    operation_result_t update(key_t key, value_spanc_t value) override;
    operation_result_t remove(key_t key) override;

    operation_result_t read(key_t key, value_span_t value) const override;
    operation_result_t batch_read(keys_span_t keys) const override;

    operation_result_t range_select(key_t key, size_t length, value_span_t single_value) const override;
    operation_result_t scan(value_span_t single_value) const override;

  private:
    struct config_t {
        size_t write_buffer_size = 0;
        size_t max_file_size = 0;
        size_t max_open_files = -1;
        std::string compression;
        size_t cache_size = 0;
        size_t filter_bits = -1;
    };

    bool load_config(fs::path const& config_path, config_t& config);

    leveldb::DB* db_;
};

bool leveldb_t::init(fs::path const& config_path, fs::path const& dir_path) {

    config_t config;
    if (!load_config(config_path, config))
        return false;

    leveldb::Options options;
    options.create_if_missing = true;
    if (config.write_buffer_size > 0)
        options.write_buffer_size = config.write_buffer_size;
    if (config.max_file_size > 0)
        options.max_file_size = config.max_file_size;
    if (config.max_open_files > 0)
        options.max_open_files = config.max_open_files;
    if (config.compression == "snappy")
        options.compression = leveldb::kSnappyCompression;
    else
        options.compression = leveldb::kNoCompression;
    if (config.cache_size > 0)
        options.block_cache = leveldb::NewLRUCache(config.cache_size);
    if (config.filter_bits > 0)
        options.filter_policy = leveldb::NewBloomFilterPolicy(config.filter_bits);

    leveldb::Status status = leveldb::DB::Open(options, dir_path.string(), &db_);
    return status.ok();
}

void leveldb_t::destroy() {
    delete db_;
    db_ = nullptr;
}

operation_result_t leveldb_t::insert(key_t key, value_spanc_t value) {
    std::string data(reinterpret_cast<char const*>(value.data()), value.size());
    leveldb::WriteOptions wopt;
    leveldb::Slice slice {std::to_string(key)};
    leveldb::Status status = db_->Put(wopt, slice, data);
    if (!status.ok())
        return {0, operation_status_t::error_k};
    return {1, operation_status_t::ok_k};
}

operation_result_t leveldb_t::update(key_t key, value_spanc_t value) {

    std::string data;
    leveldb::Slice slice {std::to_string(key)};
    leveldb::Status status = db_->Get(leveldb::ReadOptions(), slice, &data);
    if (status.IsNotFound())
        return {1, operation_status_t::not_found_k};
    else if (!status.ok())
        return {0, operation_status_t::error_k};

    data = std::string(reinterpret_cast<char const*>(value.data()), value.size());
    leveldb::WriteOptions wopt;
    status = db_->Put(wopt, slice, data);
    if (!status.ok())
        return {0, operation_status_t::error_k};
    return {1, operation_status_t::ok_k};
}

operation_result_t leveldb_t::remove(key_t key) {
    leveldb::WriteOptions wopt;
    leveldb::Slice slice {std::to_string(key)};
    leveldb::Status status = db_->Delete(wopt, slice);
    if (!status.ok())
        return {0, operation_status_t::error_k};

    return {1, operation_status_t::ok_k};
}

operation_result_t leveldb_t::read(key_t key, value_span_t value) const {
    std::string data;
    leveldb::Slice slice {std::to_string(key)};
    leveldb::Status status = db_->Get(leveldb::ReadOptions(), slice, &data);
    if (status.IsNotFound())
        return {1, operation_status_t::not_found_k};
    else if (!status.ok())
        return {0, operation_status_t::error_k};

    memcpy(value.data(), data.data(), data.size());
    return {1, operation_status_t::ok_k};
}

operation_result_t leveldb_t::batch_read(keys_span_t keys) const {

    // Note: imitation of batch read!
    for (auto const& key : keys) {
        std::string data;
        leveldb::Slice slice {std::to_string(key)};
        leveldb::Status status = db_->Get(leveldb::ReadOptions(), slice, &data);
    }
    return {keys.size(), operation_status_t::ok_k};
}

operation_result_t leveldb_t::range_select(key_t key, size_t length, value_span_t single_value) const {

    leveldb::Iterator* db_iter = db_->NewIterator(leveldb::ReadOptions());
    leveldb::Slice slice {std::to_string(key)};
    db_iter->Seek(slice);
    size_t selected_records_count = 0;
    for (int i = 0; db_iter->Valid() && i < length; i++) {
        std::string data = db_iter->value().ToString();
        memcpy(single_value.data(), data.data(), data.size());
        db_iter->Next();
        ++selected_records_count;
    }
    delete db_iter;
    return {selected_records_count, operation_status_t::ok_k};
}

operation_result_t leveldb_t::scan(value_span_t single_value) const {

    size_t scaned_records_count = 0;
    leveldb::Iterator* db_iter = db_->NewIterator(leveldb::ReadOptions());
    db_iter->SeekToFirst();
    while (db_iter->Valid()) {
        std::string data = db_iter->value().ToString();
        memcpy(single_value.data(), data.data(), data.size());
        db_iter->Next();
        ++scaned_records_count;
    }
    delete db_iter;
    return {scaned_records_count, operation_status_t::ok_k};
}

bool leveldb_t::load_config(fs::path const& config_path, config_t& config) {
    if (!fs::exists(config_path.c_str()))
        return false;

    std::ifstream i_config(config_path);
    nlohmann::json j_config;
    i_config >> j_config;

    config.write_buffer_size = j_config.value("write_buffer_size", 67108864);
    config.max_file_size = j_config.value("max_file_size", 67108864);
    config.max_open_files = j_config.value("max_open_files", 1000);
    config.compression = j_config.value("compression", "none");
    config.cache_size = j_config.value("cache_size", 134217728);
    config.filter_bits = j_config.value("filter_bits", 10);

    return true;
}

} // namespace google
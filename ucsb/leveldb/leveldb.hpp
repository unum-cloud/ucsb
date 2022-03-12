#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <fmt/format.h>
#include <nlohmann/json.hpp>

#include <leveldb/options.h>
#include <leveldb/write_batch.h>
#include <leveldb/db.h>
#include <leveldb/status.h>
#include <leveldb/cache.h>
#include <leveldb/filter_policy.h>
#include <leveldb/comparator.h>

#include "ucsb/core/types.hpp"
#include "ucsb/core/db.hpp"
#include "ucsb/core/helper.hpp"

namespace google {

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

/**
 * @brief LevelDB wrapper for the UCSB benchmark.
 * It's the precursor of RocksDB by Facebook.
 * https://github.com/google/leveldb
 */
struct leveldb_t : public ucsb::db_t {
  public:
    inline leveldb_t() : db_(nullptr) {}
    inline ~leveldb_t() { close(); }

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

    operation_result_t bulk_insert(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) override;

    operation_result_t range_select(key_t key, size_t length, values_span_t values) const override;
    operation_result_t scan(key_t key, size_t length, value_span_t single_value) const override;

    void flush() override;
    size_t size_on_disk() const override;

    std::unique_ptr<transaction_t> create_transaction() override;

  private:
    struct config_t {
        size_t write_buffer_size = 0;
        size_t max_file_size = 0;
        size_t max_open_files = -1;
        std::string compression;
        size_t cache_size = 0;
        size_t filter_bits = -1;
    };

    inline bool load_config(config_t& config);

    struct key_comparator_t final /*: public leveldb::Comparator*/ {
        int Compare(leveldb::Slice const& left, leveldb::Slice const& right) const /*override*/ {
            assert(left.size() == sizeof(key_t));
            assert(right.size() == sizeof(key_t));

            key_t left_key = *reinterpret_cast<key_t const*>(left.data());
            key_t right_key = *reinterpret_cast<key_t const*>(right.data());
            return left_key < right_key ? -1 : left_key > right_key;
        }
        const char* Name() const { return "KeyComparator"; }
        void FindShortestSeparator(std::string*, const leveldb::Slice&) const {}
        void FindShortSuccessor(std::string*) const {}
    };

    fs::path config_path_;
    fs::path dir_path_;

    leveldb::Options options_;
    std::unique_ptr<leveldb::DB> db_;
    key_comparator_t key_cmp_;
};

void leveldb_t::set_config(fs::path const& config_path, fs::path const& dir_path) {
    config_path_ = config_path;
    dir_path_ = dir_path;
}

bool leveldb_t::open() {
    if (db_)
        return true;

    config_t config;
    if (!load_config(config))
        return false;

    options_ = leveldb::Options();
    options_.create_if_missing = true;
    // options.comparator = &key_cmp_;
    if (config.write_buffer_size > 0)
        options_.write_buffer_size = config.write_buffer_size;
    if (config.max_file_size > 0)
        options_.max_file_size = config.max_file_size;
    if (config.max_open_files > 0)
        options_.max_open_files = config.max_open_files;
    if (config.compression == "snappy")
        options_.compression = leveldb::kSnappyCompression;
    else
        options_.compression = leveldb::kNoCompression;
    if (config.cache_size > 0)
        options_.block_cache = leveldb::NewLRUCache(config.cache_size);
    if (config.filter_bits > 0)
        options_.filter_policy = leveldb::NewBloomFilterPolicy(config.filter_bits);

    leveldb::DB* db_raw = nullptr;
    leveldb::Status status = leveldb::DB::Open(options_, dir_path_.string(), &db_raw);
    db_.reset(db_raw);

    return status.ok();
}

bool leveldb_t::close() {
    db_.reset(nullptr);
    return true;
}

void leveldb_t::destroy() {
    bool ok = close();
    assert(ok);
    leveldb::DestroyDB(dir_path_.string(), options_);
}

operation_result_t leveldb_t::insert(key_t key, value_spanc_t value) {
    leveldb::Slice slice {reinterpret_cast<char const*>(&key), sizeof(key_t)};
    leveldb::Slice data_slice {reinterpret_cast<char const*>(value.data()), value.size()};
    leveldb::WriteOptions wopt;
    leveldb::Status status = db_->Put(wopt, slice, data_slice);
    return {1, status.ok() ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t leveldb_t::update(key_t key, value_spanc_t value) {

    std::string data;
    leveldb::Slice slice {reinterpret_cast<char const*>(&key), sizeof(key_t)};
    leveldb::Status status = db_->Get(leveldb::ReadOptions(), slice, &data);
    if (status.IsNotFound())
        return {1, operation_status_t::not_found_k};
    else if (!status.ok())
        return {1, operation_status_t::error_k};

    leveldb::Slice data_slice {reinterpret_cast<char const*>(value.data()), value.size()};
    leveldb::WriteOptions wopt;
    status = db_->Put(wopt, slice, data_slice);
    return {1, status.ok() ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t leveldb_t::remove(key_t key) {
    leveldb::WriteOptions wopt;
    leveldb::Slice slice {reinterpret_cast<char const*>(&key), sizeof(key_t)};
    leveldb::Status status = db_->Delete(wopt, slice);
    return {1, status.ok() ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t leveldb_t::read(key_t key, value_span_t value) const {
    std::string data;
    leveldb::Slice slice {reinterpret_cast<char const*>(&key), sizeof(key_t)};
    leveldb::Status status = db_->Get(leveldb::ReadOptions(), slice, &data);
    if (status.IsNotFound())
        return {1, operation_status_t::not_found_k};
    else if (!status.ok())
        return {0, operation_status_t::error_k};

    memcpy(value.data(), data.data(), data.size());
    return {1, operation_status_t::ok_k};
}

operation_result_t leveldb_t::batch_insert(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) {
    leveldb::WriteBatch batch;
    for (size_t idx = 0; idx < keys.size(); ++idx) {
        auto key = keys[idx];
        auto value = values[idx];

        leveldb::Slice key_slice {reinterpret_cast<char const*>(&key), sizeof(key_t)};
        leveldb::Slice value_slice {reinterpret_cast<char const*>(&value), sizes[idx]};
        batch.Put(key_slice, value_slice);
    }

    leveldb::Status status = db_->Write(leveldb::WriteOptions(), &batch);
    return {keys.size(), status.ok() ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t leveldb_t::batch_read(keys_spanc_t keys, values_span_t values) const {

    // Note: imitation of batch read!
    size_t offset = 0;
    size_t found_cnt = 0;
    for (auto key : keys) {
        std::string data;
        leveldb::Slice slice {reinterpret_cast<char const*>(&key), sizeof(key_t)};
        leveldb::Status status = db_->Get(leveldb::ReadOptions(), slice, &data);
        if (status.ok()) {
            memcpy(values.data() + offset, data.data(), data.size());
            offset += data.size();
            ++found_cnt;
        }
    }
    return {found_cnt, operation_status_t::ok_k};
}

operation_result_t leveldb_t::bulk_insert(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) {
    // Currently this DB doesn't have bulk insert so instead we do batch insert
    return batch_insert(keys, values, sizes);
}

operation_result_t leveldb_t::range_select(key_t key, size_t length, values_span_t values) const {

    leveldb::Iterator* db_iter = db_->NewIterator(leveldb::ReadOptions());
    leveldb::Slice slice {reinterpret_cast<char const*>(&key), sizeof(key_t)};
    db_iter->Seek(slice);
    size_t offset = 0;
    size_t selected_records_count = 0;
    for (size_t i = 0; db_iter->Valid() && i < length; i++) {
        std::string data = db_iter->value().ToString();
        memcpy(values.data() + offset, data.data(), data.size());
        offset += data.size();
        db_iter->Next();
        ++selected_records_count;
    }
    delete db_iter;
    return {selected_records_count, operation_status_t::ok_k};
}

operation_result_t leveldb_t::scan(key_t key, size_t length, value_span_t single_value) const {

    leveldb::Iterator* db_iter = db_->NewIterator(leveldb::ReadOptions());
    leveldb::Slice slice {reinterpret_cast<char const*>(&key), sizeof(key_t)};
    db_iter->Seek(slice);
    size_t scanned_records_count = 0;
    for (size_t i = 0; db_iter->Valid() && i < length; i++) {
        std::string data = db_iter->value().ToString();
        memcpy(single_value.data(), data.data(), data.size());
        db_iter->Next();
        ++scanned_records_count;
    }
    delete db_iter;
    return {scanned_records_count, operation_status_t::ok_k};
}

void leveldb_t::flush() {
    // Nothing to do
}

size_t leveldb_t::size_on_disk() const {
    return ucsb::size_on_disk(dir_path_);
}

std::unique_ptr<transaction_t> leveldb_t::create_transaction() {
    return {};
}

bool leveldb_t::load_config(config_t& config) {
    if (!fs::exists(config_path_))
        return false;

    std::ifstream i_config(config_path_);
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
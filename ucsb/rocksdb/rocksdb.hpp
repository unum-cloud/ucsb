#pragma once

#include <iostream>
#include <cstring>
#include <string>
#include <vector>
#include <fmt/format.h>

#include <rocksdb/status.h>
#include <rocksdb/cache.h>
#include <rocksdb/write_batch.h>
#include <rocksdb/utilities/options_util.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <leveldb/comparator.h>

#include "ucsb/core/types.hpp"
#include "ucsb/core/db.hpp"

namespace facebook {

namespace fs = ucsb::fs;

using key_t = ucsb::key_t;
using keys_span_t = ucsb::keys_span_t;
using value_span_t = ucsb::value_span_t;
using value_spanc_t = ucsb::value_spanc_t;
using operation_status_t = ucsb::operation_status_t;
using operation_result_t = ucsb::operation_result_t;

struct key_comparator_t : public rocksdb::Comparator {
    int Compare(rocksdb::Slice const& left, rocksdb::Slice const& right) const override {
        assert(left.size() == sizeof(key_t));
        assert(right.size() == sizeof(key_t));

        key_t left_key = *reinterpret_cast<key_t const*>(left.data());
        key_t right_key = *reinterpret_cast<key_t const*>(right.data());
        return left_key < right_key ? -1 : left_key > right_key;
    }
    const char* Name() const { return "KeyComparator"; }
    void FindShortestSeparator(std::string*, const rocksdb::Slice&) const {}
    void FindShortSuccessor(std::string*) const {}
};

struct rocksdb_t : public ucsb::db_t {
  public:
    inline rocksdb_t() : db_(nullptr) {}
    ~rocksdb_t() { close(); }

    void set_config(fs::path const& config_path, fs::path const& dir_path) override;
    bool open() override;
    bool close() override;
    void destroy() override;

    operation_result_t insert(key_t key, value_spanc_t value) override;
    operation_result_t update(key_t key, value_spanc_t value) override;
    operation_result_t remove(key_t key) override;

    operation_result_t read(key_t key, value_span_t value) const override;
    operation_result_t batch_read(keys_span_t keys) const override;

    operation_result_t range_select(key_t key, size_t length, value_span_t single_value) const override;
    operation_result_t scan(value_span_t single_value) const override;

  private:
    fs::path config_path_;
    fs::path dir_path_;

    rocksdb::DB* db_;
    key_comparator_t key_cmp;
};

void rocksdb_t::set_config(fs::path const& config_path, fs::path const& dir_path) {
    config_path_ = config_path;
    dir_path_ = dir_path;
}

bool rocksdb_t::open() {
    if (db_)
        return true;

    rocksdb::Options options;
    std::vector<rocksdb::ColumnFamilyDescriptor> cf_descs;
    std::vector<rocksdb::ColumnFamilyHandle*> cf_handles;
    rocksdb::Status status =
        rocksdb::LoadOptionsFromFile(config_path_.string(), rocksdb::Env::Default(), &options, &cf_descs);
    if (!status.ok())
        return false;

    options.comparator = &key_cmp;
    if (cf_descs.empty())
        status = rocksdb::DB::Open(options, dir_path_.string(), &db_);
    else
        status = rocksdb::DB::Open(options, dir_path_.string(), cf_descs, &cf_handles, &db_);

    return status.ok();
}

bool rocksdb_t::close() {
    delete db_;
    db_ = nullptr;
    return true;
}

void rocksdb_t::destroy() {
    close();
}

operation_result_t rocksdb_t::insert(key_t key, value_spanc_t value) {
    std::string data(reinterpret_cast<char const*>(value.data()), value.size());
    rocksdb::WriteOptions wopt;
    rocksdb::Slice slice {reinterpret_cast<char const*>(&key), sizeof(key)};
    rocksdb::Status status = db_->Put(wopt, slice, data);
    if (!status.ok())
        return {0, operation_status_t::error_k};
    return {1, operation_status_t::ok_k};
}

operation_result_t rocksdb_t::update(key_t key, value_spanc_t value) {

    std::string data;
    rocksdb::Slice slice {reinterpret_cast<char const*>(&key), sizeof(key)};
    rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), slice, &data);
    if (status.IsNotFound())
        return {1, operation_status_t::not_found_k};
    else if (!status.ok())
        return {0, operation_status_t::error_k};

    data = std::string(reinterpret_cast<char const*>(value.data()), value.size());
    rocksdb::WriteOptions wopt;
    status = db_->Put(wopt, slice, data);
    if (!status.ok())
        return {0, operation_status_t::error_k};
    return {1, operation_status_t::ok_k};
}

operation_result_t rocksdb_t::remove(key_t key) {
    rocksdb::WriteOptions wopt;
    rocksdb::Slice slice {reinterpret_cast<char const*>(&key), sizeof(key)};
    rocksdb::Status status = db_->Delete(wopt, slice);
    if (!status.ok())
        return {0, operation_status_t::error_k};

    return {1, operation_status_t::ok_k};
}

operation_result_t rocksdb_t::read(key_t key, value_span_t value) const {
    std::string data;
    rocksdb::Slice slice {reinterpret_cast<char const*>(&key), sizeof(key)};
    rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), slice, &data);
    if (status.IsNotFound())
        return {1, operation_status_t::not_found_k};
    else if (!status.ok())
        return {0, operation_status_t::error_k};

    memcpy(value.data(), data.data(), data.size());
    return {1, operation_status_t::ok_k};
}

operation_result_t rocksdb_t::batch_read(keys_span_t keys) const {

    std::vector<rocksdb::Slice> slices;
    slices.reserve(keys.size());
    for (const auto& key : keys) {
        rocksdb::Slice slice {reinterpret_cast<char const*>(&key), sizeof(key)};
        slices.push_back(slice);
    }

    std::vector<std::string> data;
    data.reserve(keys.size());
    std::vector<rocksdb::Status> status = db_->MultiGet(rocksdb::ReadOptions(), slices, &data);

    return {keys.size(), operation_status_t::ok_k};
}

operation_result_t rocksdb_t::range_select(key_t key, size_t length, value_span_t single_value) const {

    rocksdb::Iterator* db_iter = db_->NewIterator(rocksdb::ReadOptions());
    rocksdb::Slice slice {reinterpret_cast<char const*>(&key), sizeof(key)};
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

operation_result_t rocksdb_t::scan(value_span_t single_value) const {

    size_t scanned_records_count = 0;
    rocksdb::Iterator* db_iter = db_->NewIterator(rocksdb::ReadOptions());
    db_iter->SeekToFirst();
    while (db_iter->Valid()) {
        std::string data = db_iter->value().ToString();
        memcpy(single_value.data(), data.data(), data.size());
        db_iter->Next();
        ++scanned_records_count;
    }
    delete db_iter;
    return {scanned_records_count, operation_status_t::ok_k};
}

} // namespace facebook
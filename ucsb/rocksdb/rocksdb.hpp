#pragma once

#include <atomic>
#include <iostream>
#include <cstring>
#include <memory>
#include <string>
#include <vector>
#include <fmt/format.h>

#include <rocksdb/status.h>
#include <rocksdb/cache.h>
#include <rocksdb/write_batch.h>
#include <rocksdb/utilities/options_util.h>
#include <rocksdb/utilities/transaction_db.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/comparator.h>
#include <rocksdb/filter_policy.h>

#include "ucsb/core/types.hpp"
#include "ucsb/core/db.hpp"
#include "ucsb/core/helper.hpp"

#include "rocksdb_transaction.hpp"

namespace facebook {

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

enum class db_mode_t {
    regular_k,
    transactional_k,
};

/*
 * @brief Preallocated buffers used for batch operations.
 * Globals and especially `thread_local`s are a bad practice.
 */
thread_local std::vector<key_t> batch_keys;
thread_local std::vector<rocksdb::Slice> key_slices;
thread_local std::vector<rocksdb::PinnableSlice> value_slices;
thread_local std::vector<rocksdb::Status> statuses;

/**
 * @brief RocksDB wrapper for the UCSB benchmark.
 * https://github.com/facebook/rocksdb
 *
 * Warning: Use custom keys comparator or swap byte from
 * little-endian to big-endian via `__builtin_bswap64`.
 */
struct rocksdb_t : public ucsb::db_t {
  public:
    inline rocksdb_t(db_mode_t mode = db_mode_t::regular_k) : db_(nullptr), transaction_db_(nullptr), mode_(mode) {}
    inline ~rocksdb_t() { close(); }

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
    fs::path config_path_;
    fs::path dir_path_;

    bool load_aditional_options();

    struct key_comparator_t final : public rocksdb::Comparator {
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

    rocksdb::Options options_;
    rocksdb::TransactionDBOptions transaction_options_;
    rocksdb::ReadOptions read_options_;
    rocksdb::WriteOptions write_options_;

    std::vector<rocksdb::ColumnFamilyDescriptor> cf_descs_;
    std::vector<rocksdb::ColumnFamilyHandle*> cf_handles_;

    std::unique_ptr<rocksdb::DB> db_;
    rocksdb::TransactionDB* transaction_db_;
    key_comparator_t key_cmp_;
    db_mode_t mode_;
};

void rocksdb_t::set_config(fs::path const& config_path, fs::path const& dir_path) {
    config_path_ = config_path;
    dir_path_ = dir_path;
}

bool rocksdb_t::open() {
    if (db_)
        return true;

    cf_descs_.clear();
    cf_handles_.clear();

    options_ = rocksdb::Options();
    rocksdb::Status status =
        rocksdb::LoadOptionsFromFile(config_path_.string(), rocksdb::Env::Default(), &options_, &cf_descs_);
    if (!status.ok() || cf_descs_.empty())
        return false;
    if (!load_aditional_options())
        return false;

    rocksdb::BlockBasedTableOptions table_options;
    table_options.block_cache = rocksdb::NewLRUCache(options_.target_file_size_base * 10);
    table_options.cache_index_and_filter_blocks = true;
    table_options.cache_index_and_filter_blocks_with_high_priority = true;
    table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10));
    options_.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
    // options_.comparator = &key_cmp_;

    // Overwrite latency-affecting settings, that aren't externally configurable.
    read_options_.verify_checksums = false;
    read_options_.background_purge_on_iterator_cleanup = true;
    write_options_.disableWAL = true;

    rocksdb::DB* db_raw = nullptr;
    if (mode_ == db_mode_t::regular_k)
        status = rocksdb::DB::Open(options_, dir_path_.string(), cf_descs_, &cf_handles_, &db_raw);
    else {
        status = rocksdb::TransactionDB::Open(options_,
                                              transaction_options_,
                                              dir_path_.string(),
                                              cf_descs_,
                                              &cf_handles_,
                                              &transaction_db_);
        db_raw = transaction_db_;
    }
    db_.reset(db_raw);

    return status.ok();
}

bool rocksdb_t::close() {
    batch_keys.clear();
    key_slices.clear();
    value_slices.clear();
    statuses.clear();

    db_.reset(nullptr);
    cf_handles_.clear();
    transaction_db_ = nullptr;
    return true;
}

void rocksdb_t::destroy() {
    bool ok = close();
    assert(ok);
    rocksdb::DestroyDB(dir_path_.string(), options_, cf_descs_);
}

operation_result_t rocksdb_t::insert(key_t key, value_spanc_t value) {
    rocksdb::Status status = db_->Put(write_options_, to_slice(key), to_slice(value));
    return {1, status.ok() ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t rocksdb_t::update(key_t key, value_spanc_t value) {

    key_t key_to_read = key;
    key_t key_to_write = key;

    rocksdb::PinnableSlice data;
    rocksdb::Status status = db_->Get(read_options_, cf_handles_[0], to_slice(key_to_read), &data);
    if (status.IsNotFound())
        return {1, operation_status_t::not_found_k};
    else if (!status.ok())
        return {0, operation_status_t::error_k};

    status = db_->Put(write_options_, to_slice(key_to_write), to_slice(value));
    return {1, status.ok() ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t rocksdb_t::remove(key_t key) {
    rocksdb::Status status = db_->Delete(write_options_, to_slice(key));
    return {1, status.ok() ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t rocksdb_t::read(key_t key, value_span_t value) const {
    rocksdb::PinnableSlice data;
    rocksdb::Status status = db_->Get(read_options_, cf_handles_[0], to_slice(key), &data);
    if (status.IsNotFound())
        return {1, operation_status_t::not_found_k};
    else if (!status.ok())
        return {0, operation_status_t::error_k};

    memcpy(value.data(), data.data(), data.size());
    return {1, operation_status_t::ok_k};
}

operation_result_t rocksdb_t::batch_insert(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) {

    size_t offset = 0;
    rocksdb::WriteBatch batch;
    for (size_t idx = 0; idx != keys.size(); ++idx) {
        key_t key = keys[idx];
        batch.Put(to_slice(key), to_slice(values.subspan(offset, sizes[idx])));
        offset += sizes[idx];
    }
    rocksdb::Status status = db_->Write(write_options_, &batch);
    return {keys.size(), status.ok() ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t rocksdb_t::batch_read(keys_spanc_t keys, values_span_t values) const {

    if (keys.size() > batch_keys.size()) {
        batch_keys.resize(keys.size());
        key_slices.resize(keys.size());
        value_slices.resize(keys.size());
        statuses.resize(keys.size());
    }

    for (size_t idx = 0; idx != keys.size(); ++idx)
        key_slices[idx] = to_slice(batch_keys[idx] = keys[idx]);

    db_->MultiGet(read_options_,
                  cf_handles_[0],
                  key_slices.size(),
                  key_slices.data(),
                  value_slices.data(),
                  statuses.data());

    size_t offset = 0;
    size_t found_cnt = 0;
    for (size_t i = 0; i != statuses.size(); ++i) {
        if (!statuses[i].ok())
            continue;
        memcpy(values.data() + offset, value_slices[i].data(), value_slices[i].size());
        offset += value_slices[i].size();
        ++found_cnt;
    }

    return {found_cnt, operation_status_t::ok_k};
}

operation_result_t rocksdb_t::bulk_load(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) {
    size_t idx = 0;
    size_t data_offset = 0;
    std::vector<std::string> files;

    while (true) {
        std::string sst_file_path = fmt::format("/tmp/rocksdb_tmp_{}.sst", files.size());
        files.push_back(sst_file_path);

        rocksdb::SstFileWriter sst_file_writer(rocksdb::EnvOptions(), options_, options_.comparator);
        rocksdb::Status status = sst_file_writer.Open(sst_file_path);
        if (!status.ok())
            break;

        for (; idx != keys.size(); ++idx) {
            auto key = keys[idx];
            status = sst_file_writer.Add(to_slice(key), to_slice(values.subspan(data_offset, sizes[idx])));
            if (!status.ok())
                break;
            data_offset += sizes[idx];
        }
        if (!status.ok())
            break;
        status = sst_file_writer.Finish();
        if (!status.ok() || idx == keys.size())
            break;
    }

    if (idx != keys.size()) {
        for (auto const& file_path : files)
            fs::remove(file_path);
        return {keys.size(), operation_status_t::error_k};
    }

    rocksdb::IngestExternalFileOptions ingest_options;
    ingest_options.move_files = true;
    rocksdb::Status status = db_->IngestExternalFile(files, ingest_options);
    for (auto const& file_path : files)
        fs::remove(file_path);
    if (!status.ok())
        return {0, operation_status_t::error_k};

    return {keys.size(), operation_status_t::ok_k};
}

operation_result_t rocksdb_t::range_select(key_t key, size_t length, values_span_t values) const {

    size_t i = 0;
    size_t exported_bytes = 0;
    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(read_options_));
    it->Seek(to_slice(key));
    for (; it->Valid() && i != length; i++, it->Next()) {
        memcpy(values.data() + exported_bytes, it->value().data(), it->value().size());
        exported_bytes += it->value().size();
    }
    return {i, operation_status_t::ok_k};
}

operation_result_t rocksdb_t::scan(key_t key, size_t length, value_span_t single_value) const {

    size_t i = 0;
    rocksdb::ReadOptions scan_options = read_options_;
    // It's recommended to disable caching on long scans.
    // https://github.com/facebook/rocksdb/blob/49a10feb21dc5c766bb272406136667e1d8a969e/include/rocksdb/options.h#L1462
    scan_options.fill_cache = false;
    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(read_options_));
    it->Seek(to_slice(key));
    for (; it->Valid() && i != length; i++, it->Next())
        memcpy(single_value.data(), it->value().data(), it->value().size());
    return {i, operation_status_t::ok_k};
}

void rocksdb_t::flush() {
    db_->Flush(rocksdb::FlushOptions());
}

size_t rocksdb_t::size_on_disk() const {
    size_t files_size = 0;
    for (auto const& db_path : options_.db_paths) {
        if (!db_path.path.empty() && fs::exists(db_path.path))
            files_size += ucsb::size_on_disk(db_path.path);
    }
    return files_size + ucsb::size_on_disk(dir_path_);
}

std::unique_ptr<transaction_t> rocksdb_t::create_transaction() {

    std::unique_ptr<rocksdb::Transaction> raw(transaction_db_->BeginTransaction(write_options_));
    auto id = size_t(raw.get());
    raw->SetName(std::to_string(id));
    return std::make_unique<rocksdb_transaction_t>(std::move(raw), cf_handles_);
}

bool rocksdb_t::load_aditional_options() {
    if (!fs::exists(config_path_))
        return false;

    fs::path transaction_config_path = config_path_.parent_path();
    transaction_config_path += "/additional.cfg";
    std::ifstream i_config(transaction_config_path);
    nlohmann::json j_config;
    i_config >> j_config;

    std::vector<std::string> db_paths = j_config["db_paths"].get<std::vector<std::string>>();
    for (auto const& db_path : db_paths) {
        if (!db_path.empty()) {
            size_t files_size = 0;
            if (fs::exists(db_path))
                files_size = ucsb::size_on_disk(db_path);
            options_.db_paths.push_back({db_path, files_size});
        }
    }

    transaction_options_.default_write_batch_flush_threshold =
        j_config["default_write_batch_flush_threshold"].get<int64_t>();
    if (transaction_options_.default_write_batch_flush_threshold > 0)
        transaction_options_.write_policy = rocksdb::TxnDBWritePolicy::WRITE_UNPREPARED;

    return true;
}

} // namespace facebook
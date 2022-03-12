#pragma once

/**
 * @brief Warning: We added this macro to have two different builds: regular and transactional
 * Bacause RocksDB has had a linker error
 */
// #define build_transaction_m

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
#ifdef build_transaction_m
#include <rocksdb/utilities/transaction_db.h>
#endif
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

/**
 * @brief Prealocated buffers for batch operations.
 * These variables defined as global not as function local,
 * because at the end of the benchmark (see close())
 * we need to clear them before RocksDB statis objects are destructed
 * https://github.com/facebook/rocksdb/issues/649
 */
thread_local std::vector<rocksdb::Slice> key_slices;
thread_local std::vector<rocksdb::PinnableSlice> value_slices;
thread_local std::vector<rocksdb::Status> statuses;

/**
 * @brief RocksDB wrapper for the UCSB benchmark.
 * https://github.com/facebook/rocksdb
 */
template <db_mode_t mode_ak>
struct rocksdb_gt : public ucsb::db_t {
  public:
    inline rocksdb_gt()
        : db_(nullptr)
#ifdef build_transaction_m
          ,
          transaction_db_(nullptr)
#endif
          ,
          do_compaction_on_flush(false) {
    }
    inline ~rocksdb_gt() { close(); }

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
    fs::path config_path_;
    fs::path dir_path_;

    bool load_aditional_options();
    void do_compaction();

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
#ifdef build_transaction_m
    rocksdb::TransactionDBOptions transaction_options_;
#endif
    rocksdb::ReadOptions read_options_;
    rocksdb::WriteOptions write_options_;

    std::vector<rocksdb::ColumnFamilyDescriptor> cf_descs_;
    std::vector<rocksdb::ColumnFamilyHandle*> cf_handles_;

    std::unique_ptr<rocksdb::DB> db_;
#ifdef build_transaction_m
    rocksdb::TransactionDB* transaction_db_;
#endif
    key_comparator_t key_cmp_;
    std::atomic_bool do_compaction_on_flush;
};

template <db_mode_t mode_ak>
void rocksdb_gt<mode_ak>::set_config(fs::path const& config_path, fs::path const& dir_path) {
    config_path_ = config_path;
    dir_path_ = dir_path;
}

template <db_mode_t mode_ak>
bool rocksdb_gt<mode_ak>::open() {
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

    read_options_.verify_checksums = false;
    write_options_.disableWAL = true;

    rocksdb::DB* db_raw = nullptr;
    if constexpr (mode_ak == db_mode_t::regular_k)
        status = rocksdb::DB::Open(options_, dir_path_.string(), cf_descs_, &cf_handles_, &db_raw);
    else {
#ifdef build_transaction_m
        status = rocksdb::TransactionDB::Open(options_,
                                              transaction_options_,
                                              dir_path_.string(),
                                              cf_descs_,
                                              &cf_handles_,
                                              &transaction_db_);
        db_raw = transaction_db_;
#else
        return false;
#endif
    }
    db_.reset(db_raw);

    return status.ok();
}

template <db_mode_t mode_ak>
bool rocksdb_gt<mode_ak>::close() {
    key_slices.clear();
    value_slices.clear();
    statuses.clear();
    do_compaction_on_flush.store(false);

    db_.reset(nullptr);
    cf_handles_.clear();
#ifdef build_transaction_m
    transaction_db_ = nullptr;
#endif
    return true;
}

template <db_mode_t mode_ak>
void rocksdb_gt<mode_ak>::destroy() {
    bool ok = close();
    assert(ok);
    rocksdb::DestroyDB(dir_path_.string(), options_, cf_descs_);
}

template <db_mode_t mode_ak>
operation_result_t rocksdb_gt<mode_ak>::insert(key_t key, value_spanc_t value) {
    rocksdb::Slice key_slice {reinterpret_cast<char const*>(&key), sizeof(key_t)};
    rocksdb::Slice value_slice {reinterpret_cast<char const*>(value.data()), value.size()};
    rocksdb::Status status = db_->Put(write_options_, slice, value_slice);
    return {1, status.ok() ? operation_status_t::ok_k : operation_status_t::error_k};
}

template <db_mode_t mode_ak>
operation_result_t rocksdb_gt<mode_ak>::update(key_t key, value_spanc_t value) {

    std::string data;
    rocksdb::Slice key_slice {reinterpret_cast<char const*>(&key), sizeof(key_t)};
    rocksdb::Status status = db_->Get(read_options_, slice, &data);
    if (status.IsNotFound())
        return {1, operation_status_t::not_found_k};
    else if (!status.ok())
        return {0, operation_status_t::error_k};

    rocksdb::Slice value_slice {reinterpret_cast<char const*>(value.data()), value.size()};
    status = db_->Put(write_options_, slice, value_slice);
    return {1, status.ok() ? operation_status_t::ok_k : operation_status_t::error_k};
}

template <db_mode_t mode_ak>
operation_result_t rocksdb_gt<mode_ak>::remove(key_t key) {
    rocksdb::Slice key_slice {reinterpret_cast<char const*>(&key), sizeof(key_t)};
    rocksdb::Status status = db_->Delete(write_options_, slice);
    return {1, status.ok() ? operation_status_t::ok_k : operation_status_t::error_k};
}

template <db_mode_t mode_ak>
operation_result_t rocksdb_gt<mode_ak>::read(key_t key, value_span_t value) const {
    std::string data;
    rocksdb::Slice key_slice {reinterpret_cast<char const*>(&key), sizeof(key_t)};
    rocksdb::Status status = db_->Get(read_options_, slice, &data);
    if (status.IsNotFound())
        return {1, operation_status_t::not_found_k};
    else if (!status.ok())
        return {0, operation_status_t::error_k};

    memcpy(value.data(), data.data(), data.size());
    return {1, operation_status_t::ok_k};
}

template <db_mode_t mode_ak>
operation_result_t rocksdb_gt<mode_ak>::batch_insert(keys_spanc_t keys,
                                                     values_spanc_t values,
                                                     value_lengths_spanc_t sizes) {

    size_t offset = 0;
    for (size_t idx = 0; idx < keys.size(); ++idx) {
        rocksdb::Slice key_slice {reinterpret_cast<char const*>(&keys[idx]), sizeof(key_t)};
        rocksdb::Slice value_slice {reinterpret_cast<char const*>(values.data() + offset), sizes[idx]};
        rocksdb::Status status = db_->Put(write_options_, slice, value_slice);
        if (!status.ok())
            return {0, operation_status_t::error_k};
        offset += sizes[idx];
    }
    return {keys.size(), operation_status_t::ok_k};
}

template <db_mode_t mode_ak>
operation_result_t rocksdb_gt<mode_ak>::batch_read(keys_spanc_t keys, values_span_t values) const {

    if (keys.size() > key_slices.size()) {
        key_slices = std::vector<rocksdb::Slice>(keys.size());
        value_slices = std::vector<rocksdb::PinnableSlice>(keys.size());
        statuses = std::vector<rocksdb::Status>(keys.size());
    }

    for (size_t idx = 0; idx < keys.size(); ++idx) {
        rocksdb::Slice key_slice {reinterpret_cast<char const*>(&keys[idx]), sizeof(keys[idx])};
        key_slices[idx] = slice;
    }

    db_->MultiGet(read_options_,
                  cf_handles_[0],
                  key_slices.size(),
                  key_slices.data(),
                  value_slices.data(),
                  statuses.data());

    size_t offset = 0;
    size_t found_cnt = 0;
    for (size_t i = 0; i < statuses.size(); ++i) {
        if (statuses[i].ok()) {
            memcpy(values.data() + offset, value_slices[i].data(), value_slices[i].size());
            offset += value_slices[i].size();
            ++found_cnt;
        }
    }

    return {found_cnt, operation_status_t::ok_k};
}

template <db_mode_t mode_ak>
operation_result_t rocksdb_gt<mode_ak>::bulk_insert(keys_spanc_t keys,
                                                    values_spanc_t values,
                                                    value_lengths_spanc_t sizes) {

    std::string sst_file_path("/tmp/rocksdb_tmp.sst");
    rocksdb::SstFileWriter sst_file_writer(rocksdb::EnvOptions(), options_, options_.comparator);
    rocksdb::Status status = sst_file_writer.Open(sst_file_path);
    if (!status.ok()) {
        fs::remove(sst_file_path);
        return {0, operation_status_t::error_k};
    }

    size_t idx = 0;
    size_t offset = 0;
    for (; idx < keys.size(); ++idx) {
        auto key = keys[idx];

        // Warning: if not using custom comparator need to swap little endian to big endian
        if (options_.comparator != &key_cmp_)
            key = __builtin_bswap64(key);

        rocksdb::Slice key_slice {reinterpret_cast<char const*>(&key), sizeof(key)};
        rocksdb::Slice value_slice {reinterpret_cast<char const*>(values.data() + offset), sizes[idx]};
        status = sst_file_writer.Add(slice, value_slice);
        if (!status.ok())
            break;
        offset += sizes[idx];
    }
    status = sst_file_writer.Finish();
    if (!status.ok()) {
        fs::remove(sst_file_path);
        return {0, operation_status_t::error_k};
    }

    rocksdb::IngestExternalFileOptions ingest_options;
    ingest_options.move_files = true;
    status = db_->IngestExternalFile({sst_file_path}, ingest_options);
    fs::remove(sst_file_path);
    if (!status.ok())
        return {0, operation_status_t::error_k};
    do_compaction_on_flush.store(true);

    return {idx, operation_status_t::ok_k};
}

template <db_mode_t mode_ak>
operation_result_t rocksdb_gt<mode_ak>::range_select(key_t key, size_t length, values_span_t values) const {

    rocksdb::Iterator* db_iter = db_->NewIterator(read_options_);
    rocksdb::Slice key_slice {reinterpret_cast<char const*>(&key), sizeof(key_t)};
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

template <db_mode_t mode_ak>
operation_result_t rocksdb_gt<mode_ak>::scan(key_t key, size_t length, value_span_t single_value) const {

    rocksdb::Iterator* db_iter = db_->NewIterator(read_options_);
    rocksdb::Slice key_slice {reinterpret_cast<char const*>(&key), sizeof(key_t)};
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

template <db_mode_t mode_ak>
void rocksdb_gt<mode_ak>::flush() {
    db_->Flush(rocksdb::FlushOptions());
    if (do_compaction_on_flush.load(std::memory_order_relaxed)) {
        do_compaction();
        do_compaction_on_flush.store(false);
    }
}

template <db_mode_t mode_ak>
size_t rocksdb_gt<mode_ak>::size_on_disk() const {
    size_t files_size = 0;
    for (auto const& db_path : options_.db_paths) {
        if (!db_path.path.empty() && fs::exists(db_path.path))
            files_size += ucsb::size_on_disk(db_path.path);
    }
    return files_size + ucsb::size_on_disk(dir_path_);
}

template <db_mode_t mode_ak>
std::unique_ptr<transaction_t> rocksdb_gt<mode_ak>::create_transaction() {

#ifdef build_transaction_m
    std::unique_ptr<rocksdb::Transaction> raw_transaction;
    raw_transaction.reset(transaction_db_->BeginTransaction(write_options_));
    auto id = size_t(raw_transaction.get());
    raw_transaction->SetName(std::to_string(id));
    return std::make_unique<rocksdb_transaction_t>(std::move(raw_transaction), cf_handles_);
#else
    return {};
#endif
}

template <db_mode_t mode_ak>
bool rocksdb_gt<mode_ak>::load_aditional_options() {
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

#ifdef build_transaction_m
    transaction_options_.default_write_batch_flush_threshold =
        j_config["default_write_batch_flush_threshold"].get<int64_t>();
    if (transaction_options_.default_write_batch_flush_threshold > 0)
        transaction_options_.write_policy = rocksdb::TxnDBWritePolicy::WRITE_UNPREPARED;
#endif

    return true;
}

template <db_mode_t mode_ak>
void rocksdb_gt<mode_ak>::do_compaction() {
    // https://www.facebook.com/groups/rocksdb.dev/posts/912061515559030/
    rocksdb::CompactionOptions compaction_options;
    rocksdb::ColumnFamilyMetaData meta;
    db_->GetColumnFamilyMetaData(&meta);
    for (auto const& level : meta.levels) {
        std::vector<std::string> file_names;
        file_names.reserve(level.files.size());
        for (auto const& file : level.files) {
            std::string full_name = file.db_path + "/" + file.name;
            file_names.push_back(full_name);
        }
        db_->CompactFiles(compaction_options, file_names, level.level);
    }
}

} // namespace facebook
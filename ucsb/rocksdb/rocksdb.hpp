#pragma once

/**
 * @brief Warning: We added this macro to have two different builds: regular and transactional
 * Bacause RocksDB has had a linker error
 */
// #define build_transaction_m

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
using bulk_metadata_t = ucsb::bulk_metadata_t;
using operation_status_t = ucsb::operation_status_t;
using operation_result_t = ucsb::operation_result_t;
using transaction_t = ucsb::transaction_t;

enum class db_mode_t {
    regular_k,
    transactional_k,
};

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
    {
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

    bulk_metadata_t prepare_bulk_import_data(keys_spanc_t keys,
                                             values_spanc_t values,
                                             value_lengths_spanc_t sizes) const override;
    operation_result_t bulk_import(bulk_metadata_t const& metadata) override;

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
#ifdef build_transaction_m
    rocksdb::TransactionDBOptions transaction_options_;
#endif
    std::vector<rocksdb::ColumnFamilyDescriptor> cf_descs_;

    std::unique_ptr<rocksdb::DB> db_;
#ifdef build_transaction_m
    rocksdb::TransactionDB* transaction_db_;
#endif
    key_comparator_t key_cmp_;
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

    options_ = rocksdb::Options();
    cf_descs_.clear();
    rocksdb::Status status =
        rocksdb::LoadOptionsFromFile(config_path_.string(), rocksdb::Env::Default(), &options_, &cf_descs_);
    if (!status.ok())
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

    rocksdb::DB* db_raw = nullptr;
    if constexpr (mode_ak == db_mode_t::regular_k) {
        if (cf_descs_.empty())
            status = rocksdb::DB::Open(options_, dir_path_.string(), &db_raw);
        else {
            std::vector<rocksdb::ColumnFamilyHandle*> cf_handles;
            status = rocksdb::DB::Open(options_, dir_path_.string(), cf_descs_, &cf_handles, &db_raw);
        }
    }
    else {
#ifdef build_transaction_m
        if (cf_descs_.empty())
            status = rocksdb::TransactionDB::Open(options_, transaction_options_, dir_path_.string(), &transaction_db_);
        else {
            std::vector<rocksdb::ColumnFamilyHandle*> cf_handles;
            status = rocksdb::TransactionDB::Open(options_,
                                                  transaction_options_,
                                                  dir_path_.string(),
                                                  cf_descs_,
                                                  &cf_handles,
                                                  &transaction_db_);
        }
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
    db_.reset(nullptr);
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
    rocksdb::Slice slice {reinterpret_cast<char const*>(&key), sizeof(key)};
    rocksdb::Slice data_slice {reinterpret_cast<char const*>(value.data()), value.size()};
    rocksdb::WriteOptions wopt;
    rocksdb::Status status = db_->Put(wopt, slice, data_slice);
    if (!status.ok())
        return {0, operation_status_t::error_k};
    return {1, operation_status_t::ok_k};
}

template <db_mode_t mode_ak>
operation_result_t rocksdb_gt<mode_ak>::update(key_t key, value_spanc_t value) {

    std::string data;
    rocksdb::Slice slice {reinterpret_cast<char const*>(&key), sizeof(key)};
    rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), slice, &data);
    if (status.IsNotFound())
        return {1, operation_status_t::not_found_k};
    else if (!status.ok())
        return {0, operation_status_t::error_k};

    rocksdb::Slice data_slice {reinterpret_cast<char const*>(value.data()), value.size()};
    rocksdb::WriteOptions wopt;
    status = db_->Put(wopt, slice, data_slice);
    if (!status.ok())
        return {0, operation_status_t::error_k};
    return {1, operation_status_t::ok_k};
}

template <db_mode_t mode_ak>
operation_result_t rocksdb_gt<mode_ak>::remove(key_t key) {
    rocksdb::WriteOptions wopt;
    rocksdb::Slice slice {reinterpret_cast<char const*>(&key), sizeof(key)};
    rocksdb::Status status = db_->Delete(wopt, slice);
    if (!status.ok())
        return {0, operation_status_t::error_k};

    return {1, operation_status_t::ok_k};
}

template <db_mode_t mode_ak>
operation_result_t rocksdb_gt<mode_ak>::read(key_t key, value_span_t value) const {
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

template <db_mode_t mode_ak>
operation_result_t rocksdb_gt<mode_ak>::batch_insert(keys_spanc_t keys,
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

        rocksdb::Slice slice {reinterpret_cast<char const*>(&key), sizeof(key)};
        rocksdb::Slice data_slice {reinterpret_cast<char const*>(values.data() + offset), sizes[idx]};
        status = sst_file_writer.Add(slice, data_slice);
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
    if (!status.ok()) {
        fs::remove(sst_file_path);
        return {0, operation_status_t::error_k};
    }
    fs::remove(sst_file_path);

    return {idx, operation_status_t::ok_k};
}

template <db_mode_t mode_ak>
operation_result_t rocksdb_gt<mode_ak>::batch_read(keys_spanc_t keys, values_span_t values) const {

    std::vector<rocksdb::Slice> slices;
    slices.reserve(keys.size());
    for (const auto& key : keys) {
        rocksdb::Slice slice {reinterpret_cast<char const*>(&key), sizeof(key)};
        slices.push_back(slice);
    }

    std::vector<std::string> data;
    data.reserve(keys.size());
    std::vector<rocksdb::Status> status = db_->MultiGet(rocksdb::ReadOptions(), slices, &data);

    size_t offset = 0;
    size_t found_cnt = 0;
    for (size_t i = 0; i < status.size(); ++i) {
        if (status[i].ok()) {
            memcpy(values.data() + offset, data[i].data(), data[i].size());
            offset += data[i].size();
            ++found_cnt;
        }
    }

    return {found_cnt, operation_status_t::ok_k};
}

template <db_mode_t mode_ak>
bulk_metadata_t rocksdb_gt<mode_ak>::prepare_bulk_import_data(keys_spanc_t keys,
                                                              values_spanc_t values,
                                                              value_lengths_spanc_t sizes) const {
    size_t data_idx = 0;
    size_t data_offset = 0;
    bulk_metadata_t metadata;
    for (size_t i = 0; true; ++i) {
        std::string sst_file_path = fmt::format("/tmp/rocksdb_tmp_{}.sst", i);
        metadata.files.insert(sst_file_path);

        rocksdb::SstFileWriter sst_file_writer(rocksdb::EnvOptions(), options_, options_.comparator);
        rocksdb::Status status = sst_file_writer.Open(sst_file_path);
        if (!status.ok())
            break;

        for (; data_idx < keys.size(); ++data_idx) {
            auto key = keys[data_idx];

            // Warning: if not using custom comparator need to swap little endian to big endian
            if (options_.comparator != &key_cmp_)
                key = __builtin_bswap64(key);

            rocksdb::Slice slice {reinterpret_cast<char const*>(&key), sizeof(key)};
            rocksdb::Slice data_slice {reinterpret_cast<char const*>(values.data() + data_offset), sizes[data_idx]};
            status = sst_file_writer.Add(slice, data_slice);
            if (status.ok())
                data_offset += sizes[data_idx];
            else
                break;
        }
        status = sst_file_writer.Finish();
        if (!status.ok() || data_idx == keys.size())
            break;
    }

    if (data_idx != keys.size()) {
        for (auto const& file_path : metadata.files)
            fs::remove(file_path);
        return bulk_metadata_t();
    }
    metadata.records_count = keys.size();

    return metadata;
}

template <db_mode_t mode_ak>
operation_result_t rocksdb_gt<mode_ak>::bulk_import(bulk_metadata_t const& metadata) {

    rocksdb::IngestExternalFileOptions ingest_options;
    ingest_options.move_files = true;
    for (auto file_it = metadata.files.cbegin(); file_it != metadata.files.cend(); ++file_it) {
        rocksdb::Status status = db_->IngestExternalFile({*file_it}, ingest_options);
        if (!status.ok()) {
            for (; file_it != metadata.files.cend(); ++file_it)
                fs::remove(*file_it);
            return {0, operation_status_t::error_k};
        }
    }

    return {metadata.records_count, operation_status_t::ok_k};
}

template <db_mode_t mode_ak>
operation_result_t rocksdb_gt<mode_ak>::range_select(key_t key, size_t length, values_span_t values) const {

    rocksdb::Iterator* db_iter = db_->NewIterator(rocksdb::ReadOptions());
    rocksdb::Slice slice {reinterpret_cast<char const*>(&key), sizeof(key)};
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

    rocksdb::Iterator* db_iter = db_->NewIterator(rocksdb::ReadOptions());
    rocksdb::Slice slice {reinterpret_cast<char const*>(&key), sizeof(key)};
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
    rocksdb::WriteOptions write_options;
    std::unique_ptr<rocksdb::Transaction> raw_transaction;
    raw_transaction.reset(transaction_db_->BeginTransaction(write_options));
    auto id = size_t(raw_transaction.get());
    raw_transaction->SetName(std::to_string(id));
    return std::make_unique<rocksdb_transaction_t>(std::move(raw_transaction));
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

} // namespace facebook
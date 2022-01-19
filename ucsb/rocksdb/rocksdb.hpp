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
#include <rocksdb/comparator.h>
#include <rocksdb/filter_policy.h>

#include "ucsb/core/types.hpp"
#include "ucsb/core/db.hpp"
#include "ucsb/core/helper.hpp"

namespace facebook {

namespace fs = ucsb::fs;

using key_t = ucsb::key_t;
using keys_spanc_t = ucsb::keys_spanc_t;
using value_span_t = ucsb::value_span_t;
using value_spanc_t = ucsb::value_spanc_t;
using values_spanc_t = ucsb::values_spanc_t;
using value_lengths_spanc_t = ucsb::value_lengths_spanc_t;
using bulk_metadata_t = ucsb::bulk_metadata_t;
using operation_status_t = ucsb::operation_status_t;
using operation_result_t = ucsb::operation_result_t;

/**
 * @brief RocksDB wrapper for the UCSB benchmark.
 * https://github.com/facebook/rocksdb
 */
struct rocksdb_t : public ucsb::db_t {
  public:
    inline rocksdb_t() : db_(nullptr) {}
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
    operation_result_t batch_read(keys_spanc_t keys) const override;

    bulk_metadata_t prepare_bulk_import_data(keys_spanc_t keys,
                                             values_spanc_t values,
                                             value_lengths_spanc_t sizes) const override;
    operation_result_t bulk_import(bulk_metadata_t const& metadata) override;

    operation_result_t range_select(key_t key, size_t length, value_span_t single_value) const override;
    operation_result_t scan(value_span_t single_value) const override;

    size_t size_on_disk() const override;

  private:
    fs::path config_path_;
    fs::path dir_path_;

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
    std::vector<rocksdb::ColumnFamilyDescriptor> cf_descs_;
    rocksdb::DB* db_;
    key_comparator_t key_cmp_;
};

void rocksdb_t::set_config(fs::path const& config_path, fs::path const& dir_path) {
    config_path_ = config_path;
    dir_path_ = dir_path;
}

bool rocksdb_t::open() {
    if (db_)
        return true;

    options_ = rocksdb::Options();
    cf_descs_.clear();
    rocksdb::Status status =
        rocksdb::LoadOptionsFromFile(config_path_.string(), rocksdb::Env::Default(), &options_, &cf_descs_);
    if (!status.ok())
        return false;

    rocksdb::BlockBasedTableOptions table_options;
    table_options.block_cache = rocksdb::NewLRUCache(options_.target_file_size_base * 10);
    table_options.cache_index_and_filter_blocks = true;
    table_options.cache_index_and_filter_blocks_with_high_priority = true;
    table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10));
    options_.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));

    // options_.comparator = &key_cmp_;
    if (cf_descs_.empty())
        status = rocksdb::DB::Open(options_, dir_path_.string(), &db_);
    else {
        std::vector<rocksdb::ColumnFamilyHandle*> cf_handles;
        status = rocksdb::DB::Open(options_, dir_path_.string(), cf_descs_, &cf_handles, &db_);
    }

    return status.ok();
}

bool rocksdb_t::close() {
    delete db_;
    db_ = nullptr;
    return true;
}

void rocksdb_t::destroy() {
    bool ok = close();
    assert(ok);
    rocksdb::DestroyDB(dir_path_.string(), options_, cf_descs_);
}

operation_result_t rocksdb_t::insert(key_t key, value_spanc_t value) {
    rocksdb::Slice slice {reinterpret_cast<char const*>(&key), sizeof(key)};
    std::string data(reinterpret_cast<char const*>(value.data()), value.size());
    rocksdb::WriteOptions wopt;
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

operation_result_t rocksdb_t::batch_insert(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) {

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
        std::string data(reinterpret_cast<char const*>(values.data() + offset), sizes[idx]);
        status = sst_file_writer.Add(slice, data);
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

operation_result_t rocksdb_t::batch_read(keys_spanc_t keys) const {

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

bulk_metadata_t rocksdb_t::prepare_bulk_import_data(keys_spanc_t keys,
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
            std::string data(reinterpret_cast<char const*>(values.data() + data_offset), sizes[data_idx]);
            status = sst_file_writer.Add(slice, data);
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

operation_result_t rocksdb_t::bulk_import(bulk_metadata_t const& metadata) {

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

operation_result_t rocksdb_t::range_select(key_t key, size_t length, value_span_t single_value) const {

    rocksdb::Iterator* db_iter = db_->NewIterator(rocksdb::ReadOptions());
    rocksdb::Slice slice {reinterpret_cast<char const*>(&key), sizeof(key)};
    db_iter->Seek(slice);
    size_t selected_records_count = 0;
    for (size_t i = 0; db_iter->Valid() && i < length; i++) {
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

size_t rocksdb_t::size_on_disk() const {
    return ucsb::size_on_disk(dir_path_);
}

} // namespace facebook
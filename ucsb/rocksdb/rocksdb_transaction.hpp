#pragma once

#ifdef build_transaction_m

#include <memory>
#include <vector>
#include <fmt/format.h>

#include <rocksdb/utilities/transaction_db.h>

#include "ucsb/core/types.hpp"
#include "ucsb/core/data_accessor.hpp"
#include "ucsb/core/helper.hpp"

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

/**
 * @brief Prealocated buffers for batch operations.
 * These variables defined as global not as function local,
 * because at the end of the benchmark (see ~rocksdb_transaction_t())
 * we need to clear them before RocksDB statis objects are destructed
 * https://github.com/facebook/rocksdb/issues/649
 */
thread_local std::vector<rocksdb::Slice> transaction_key_slices;
thread_local std::vector<rocksdb::PinnableSlice> transaction_value_slices;
thread_local std::vector<rocksdb::Status> transaction_statuses;

/**
 * @brief RocksDB transactional wrapper for the UCSB benchmark.
 */
struct rocksdb_transaction_t : public ucsb::transaction_t {
  public:
    inline rocksdb_transaction_t(std::unique_ptr<rocksdb::Transaction>&& transaction,
                                 std::vector<rocksdb::ColumnFamilyHandle*> const& cf_handles)
        : transaction_(std::forward<std::unique_ptr<rocksdb::Transaction>&&>(transaction)), cf_handles_(cf_handles) {
        read_options_.verify_checksums = false;
    }
    inline ~rocksdb_transaction_t();

    operation_result_t insert(key_t key, value_spanc_t value) override;
    operation_result_t update(key_t key, value_spanc_t value) override;
    operation_result_t remove(key_t key) override;

    operation_result_t read(key_t key, value_span_t value) const override;
    operation_result_t batch_insert(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) override;
    operation_result_t batch_read(keys_spanc_t keys, values_span_t values) const override;

    operation_result_t bulk_insert(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) override;

    operation_result_t range_select(key_t key, size_t length, values_span_t values) const override;
    operation_result_t scan(key_t key, size_t length, value_span_t single_value) const override;

  private:
    std::unique_ptr<rocksdb::Transaction> transaction_;
    std::vector<rocksdb::ColumnFamilyHandle*> cf_handles_;

    rocksdb::ReadOptions read_options_;
};

inline rocksdb_transaction_t::~rocksdb_transaction_t() {
    transaction_key_slices.clear();
    transaction_value_slices.clear();
    transaction_statuses.clear();

    auto status = transaction_->Commit();
    assert(status.ok());
}

operation_result_t rocksdb_transaction_t::insert(key_t key, value_spanc_t value) {
    rocksdb::Slice key_slice {reinterpret_cast<char const*>(&key), sizeof(key)};
    rocksdb::Slice value_slice {reinterpret_cast<char const*>(value.data()), value.size()};
    rocksdb::Status status = transaction_->Put(key_slice, value_slice);
    if (!status.ok()) {
        assert(status.IsTryAgain());
        status = transaction_->Commit();
        assert(status.ok());
        status = transaction_->Put(key_slice, value_slice);
        assert(status.ok());
    }
    return {1, operation_status_t::ok_k};
}

operation_result_t rocksdb_transaction_t::update(key_t key, value_spanc_t value) {

    std::string data;
    rocksdb::Slice key_slice {reinterpret_cast<char const*>(&key), sizeof(key)};
    rocksdb::Status status = transaction_->Get(read_options_, key_slice, &data);
    if (status.IsNotFound())
        return {1, operation_status_t::not_found_k};
    else if (!status.ok())
        return {0, operation_status_t::error_k};

    rocksdb::Slice value_slice {reinterpret_cast<char const*>(value.data()), value.size()};
    status = transaction_->Put(key_slice, value_slice);
    if (!status.ok()) {
        assert(status.IsTryAgain());
        status = transaction_->Commit();
        assert(status.ok());
        status = transaction_->Put(key_slice, value_slice);
        assert(status.ok());
    }
    return {1, operation_status_t::ok_k};
}

operation_result_t rocksdb_transaction_t::remove(key_t key) {
    rocksdb::Slice key_slice {reinterpret_cast<char const*>(&key), sizeof(key)};
    rocksdb::Status status = transaction_->Delete(key_slice);
    if (!status.ok()) {
        assert(status.IsTryAgain());
        status = transaction_->Commit();
        assert(status.ok());
        status = transaction_->Delete(key_slice);
        assert(status.ok());
    }

    return {1, operation_status_t::ok_k};
}

operation_result_t rocksdb_transaction_t::read(key_t key, value_span_t value) const {
    std::string data;
    rocksdb::Slice key_slice {reinterpret_cast<char const*>(&key), sizeof(key)};
    rocksdb::Status status = transaction_->Get(read_options_, key_slice, &data);
    if (status.IsNotFound())
        return {1, operation_status_t::not_found_k};
    else if (!status.ok())
        return {0, operation_status_t::error_k};

    memcpy(value.data(), data.data(), data.size());
    return {1, operation_status_t::ok_k};
}

operation_result_t rocksdb_transaction_t::batch_insert(keys_spanc_t keys,
                                                       values_spanc_t values,
                                                       value_lengths_spanc_t sizes) {
    return {0, operation_status_t::not_implemented_k};
}

operation_result_t rocksdb_transaction_t::batch_read(keys_spanc_t keys, values_span_t values) const {

    if (keys.size() > transaction_key_slices.size()) {
        transaction_key_slices = std::vector<rocksdb::Slice>(keys.size());
        transaction_value_slices = std::vector<rocksdb::PinnableSlice>(keys.size());
        transaction_statuses = std::vector<rocksdb::Status>(keys.size());
    }

    for (size_t idx = 0; idx < keys.size(); ++idx) {
        rocksdb::Slice key_slice {reinterpret_cast<char const*>(&keys[idx]), sizeof(keys[idx])};
        transaction_key_slices[idx] = key_slice;
    }

    transaction_->MultiGet(read_options_,
                           cf_handles_[0],
                           transaction_key_slices.size(),
                           transaction_key_slices.data(),
                           transaction_value_slices.data(),
                           transaction_statuses.data());

    size_t offset = 0;
    size_t found_cnt = 0;
    for (size_t i = 0; i < transaction_statuses.size(); ++i) {
        if (transaction_statuses[i].ok()) {
            memcpy(values.data() + offset, transaction_value_slices[i].data(), transaction_value_slices[i].size());
            offset += transaction_value_slices[i].size();
            ++found_cnt;
        }
    }

    return {found_cnt, operation_status_t::ok_k};
}

operation_result_t rocksdb_transaction_t::bulk_insert(keys_spanc_t keys,
                                                      values_spanc_t values,
                                                      value_lengths_spanc_t sizes) {
    return {0, operation_status_t::not_implemented_k};
}

operation_result_t rocksdb_transaction_t::range_select(key_t key, size_t length, values_span_t values) const {

    rocksdb::Iterator* db_iter = transaction_->GetIterator(read_options_);
    rocksdb::Slice key_slice {reinterpret_cast<char const*>(&key), sizeof(key)};
    db_iter->Seek(key_slice);
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

operation_result_t rocksdb_transaction_t::scan(key_t key, size_t length, value_span_t single_value) const {

    rocksdb::Iterator* db_iter = transaction_->GetIterator(read_options_);
    rocksdb::Slice key_slice {reinterpret_cast<char const*>(&key), sizeof(key)};
    db_iter->Seek(key_slice);
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

} // namespace facebook

#endif
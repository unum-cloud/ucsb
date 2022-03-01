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
using bulk_metadata_t = ucsb::bulk_metadata_t;
using operation_status_t = ucsb::operation_status_t;
using operation_result_t = ucsb::operation_result_t;

/**
 * @brief RocksDB transactional wrapper for the UCSB benchmark.
 */
struct rocksdb_transaction_t : public ucsb::transaction_t {
  public:
    inline rocksdb_transaction_t(std::unique_ptr<rocksdb::Transaction>&& transaction,
                                 std::vector<rocksdb::ColumnFamilyHandle*> const& cf_handles)
        : transaction_(std::forward<std::unique_ptr<rocksdb::Transaction>&&>(transaction)), cf_handles_(cf_handles) {}
    inline ~rocksdb_transaction_t();

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

  private:
    std::unique_ptr<rocksdb::Transaction> transaction_;
    std::vector<rocksdb::ColumnFamilyHandle*> cf_handles_;

    std::vector<rocksdb::Slice> key_slices_;
    mutable std::vector<rocksdb::PinnableSlice> values_;
    mutable std::vector<rocksdb::Status> statuses_;
};

inline rocksdb_transaction_t::~rocksdb_transaction_t() {
    auto status = transaction_->Commit();
    assert(status.ok());
}

operation_result_t rocksdb_transaction_t::insert(key_t key, value_spanc_t value) {
    rocksdb::Slice slice {reinterpret_cast<char const*>(&key), sizeof(key)};
    rocksdb::Slice data_slice {reinterpret_cast<char const*>(value.data()), value.size()};
    rocksdb::Status status = transaction_->Put(slice, data_slice);
    if (!status.ok()) {
        assert(status.IsTryAgain());
        status = transaction_->Commit();
        assert(status.ok());
        status = transaction_->Put(slice, data_slice);
        assert(status.ok());
    }
    return {1, operation_status_t::ok_k};
}

operation_result_t rocksdb_transaction_t::update(key_t key, value_spanc_t value) {

    std::string data;
    rocksdb::Slice slice {reinterpret_cast<char const*>(&key), sizeof(key)};
    rocksdb::Status status = transaction_->Get(rocksdb::ReadOptions(), slice, &data);
    if (status.IsNotFound())
        return {1, operation_status_t::not_found_k};
    else if (!status.ok())
        return {0, operation_status_t::error_k};

    rocksdb::Slice data_slice {reinterpret_cast<char const*>(value.data()), value.size()};
    status = transaction_->Put(slice, data_slice);
    if (!status.ok()) {
        assert(status.IsTryAgain());
        status = transaction_->Commit();
        assert(status.ok());
        status = transaction_->Put(slice, data_slice);
        assert(status.ok());
    }
    return {1, operation_status_t::ok_k};
}

operation_result_t rocksdb_transaction_t::remove(key_t key) {
    rocksdb::Slice slice {reinterpret_cast<char const*>(&key), sizeof(key)};
    rocksdb::Status status = transaction_->Delete(slice);
    if (!status.ok()) {
        assert(status.IsTryAgain());
        status = transaction_->Commit();
        assert(status.ok());
        status = transaction_->Delete(slice);
        assert(status.ok());
    }

    return {1, operation_status_t::ok_k};
}

operation_result_t rocksdb_transaction_t::read(key_t key, value_span_t value) const {
    std::string data;
    rocksdb::Slice slice {reinterpret_cast<char const*>(&key), sizeof(key)};
    rocksdb::Status status = transaction_->Get(rocksdb::ReadOptions(), slice, &data);
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

    if (keys.size() > key_slices_.size()) {
        key_slices_ = std::vector<rocksdb::Slice>(keys.size());
        values_ = std::vector<rocksdb::PinnableSlice>(keys.size());
        statuses_ = std::vector<rocksdb::Status>(keys.size());
    }

    for (size_t idx = 0; idx < keys.size(); ++idx) {
        rocksdb::Slice slice {reinterpret_cast<char const*>(&keys[idx]), sizeof(keys[idx])};
        key_slices_[idx] = slice;
    }

    transaction_->MultiGet(rocksdb::ReadOptions(),
                           cf_handles_[0],
                           key_slices_.size(),
                           key_slices_.data(),
                           values_.data(),
                           statuses_.data());

    size_t offset = 0;
    size_t found_cnt = 0;
    for (size_t i = 0; i < statuses_.size(); ++i) {
        if (statuses_[i].ok()) {
            memcpy(values.data() + offset, values_[i].data(), values_[i].size());
            offset += values_[i].size();
            ++found_cnt;
        }
    }

    return {found_cnt, operation_status_t::ok_k};
}

bulk_metadata_t rocksdb_transaction_t::prepare_bulk_import_data(keys_spanc_t keys,
                                                                values_spanc_t values,
                                                                value_lengths_spanc_t sizes) const {
    // RocksDB doesn't support bulk import by transaction
    (void)keys;
    (void)values;
    (void)sizes;
    return {};
}

operation_result_t rocksdb_transaction_t::bulk_import(bulk_metadata_t const& metadata) {
    return {0, operation_status_t::not_implemented_k};
}

operation_result_t rocksdb_transaction_t::range_select(key_t key, size_t length, values_span_t values) const {

    rocksdb::Iterator* db_iter = transaction_->GetIterator(rocksdb::ReadOptions());
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

operation_result_t rocksdb_transaction_t::scan(key_t key, size_t length, value_span_t single_value) const {

    rocksdb::Iterator* db_iter = transaction_->GetIterator(rocksdb::ReadOptions());
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

} // namespace facebook

#endif
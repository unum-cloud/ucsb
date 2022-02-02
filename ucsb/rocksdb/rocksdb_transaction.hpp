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
    inline rocksdb_transaction_t(std::unique_ptr<rocksdb::Transaction>&& transaction)
        : transaction_(std::forward<std::unique_ptr<rocksdb::Transaction>&&>(transaction)) {}
    inline ~rocksdb_transaction_t();

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

  private:
    std::unique_ptr<rocksdb::Transaction> transaction_;
    mutable dbuffer_t batch_buffer_;
};

inline rocksdb_transaction_t::~rocksdb_transaction_t() {
    auto status = transaction_->Commit();
    assert(status.ok());
}

operation_result_t rocksdb_transaction_t::insert(key_t key, value_spanc_t value) {
    rocksdb::Slice slice {reinterpret_cast<char const*>(&key), sizeof(key)};
    std::string data(reinterpret_cast<char const*>(value.data()), value.size());
    rocksdb::Status status = transaction_->Put(slice, data);
    if (!status.ok()) {
        assert(status.IsTryAgain());
        status = transaction_->Commit();
        assert(status.ok());
        status = transaction_->Put(slice, data);
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

    data = std::string(reinterpret_cast<char const*>(value.data()), value.size());
    status = transaction_->Put(slice, data);
    if (!status.ok()) {
        assert(status.IsTryAgain());
        status = transaction_->Commit();
        assert(status.ok());
        status = transaction_->Put(slice, data);
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

operation_result_t rocksdb_transaction_t::batch_read(keys_spanc_t keys) const {
    std::vector<rocksdb::Slice> slices;
    slices.reserve(keys.size());
    for (const auto& key : keys) {
        rocksdb::Slice slice {reinterpret_cast<char const*>(&key), sizeof(key)};
        slices.push_back(slice);
    }

    std::vector<std::string> data;
    data.reserve(keys.size());
    std::vector<rocksdb::Status> status = transaction_->MultiGet(rocksdb::ReadOptions(), slices, &data);

    return {keys.size(), operation_status_t::ok_k};
}

bulk_metadata_t rocksdb_transaction_t::prepare_bulk_import_data(keys_spanc_t keys,
                                                                values_spanc_t values,
                                                                value_lengths_spanc_t sizes) const {
    // UnumDB doesn't support bulk import by transaction
    (void)keys;
    (void)values;
    (void)sizes;
    return {};
}

operation_result_t rocksdb_transaction_t::bulk_import(bulk_metadata_t const& metadata) {
    return {0, operation_status_t::not_implemented_k};
}

operation_result_t rocksdb_transaction_t::range_select(key_t key, size_t length, value_span_t single_value) const {
    rocksdb::Iterator* db_iter = transaction_->GetIterator(rocksdb::ReadOptions());
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

operation_result_t rocksdb_transaction_t::scan(value_span_t single_value) const {
    size_t scanned_records_count = 0;
    rocksdb::Iterator* db_iter = transaction_->GetIterator(rocksdb::ReadOptions());
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

#endif
#pragma once

#include <memory>
#include <vector>

#include <fmt/format.h>
#include <rocksdb/utilities/transaction_db.h>

#include "src/core/types.hpp"
#include "src/core/data_accessor.hpp"
#include "src/core/helper.hpp"

namespace ucsb::facebook {

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

inline rocksdb::Slice to_slice(key_t& key) {
    key = __builtin_bswap64(key);
    static_assert(sizeof(key_t) == sizeof(uint64_t), "Check `__builtin_bswap64`");
    return {reinterpret_cast<char const*>(&key), sizeof(key_t)};
}

inline rocksdb::Slice to_slice(value_spanc_t value) {
    return {reinterpret_cast<char const*>(value.data()), value.size()};
}

/*
 * @brief Preallocated buffers used for batch operations.
 * Globals and especially `thread_local`s are a bad practice.
 */
thread_local std::vector<key_t> transaction_batch_keys;
thread_local std::vector<rocksdb::Slice> transaction_key_slices;
thread_local std::vector<rocksdb::PinnableSlice> transaction_value_slices;
thread_local std::vector<rocksdb::Status> transaction_statuses;

/**
 * @brief RocksDB transactional wrapper for the UCSB benchmark.
 * Wraps all of our operations into transactions or just
 * snapshots if read-only workloads run.
 */
class rocksdb_transaction_t : public ucsb::transaction_t {
  public:
    inline rocksdb_transaction_t(std::unique_ptr<rocksdb::Transaction> transaction,
                                 std::vector<rocksdb::ColumnFamilyHandle*> const& cf_handles)
        : transaction_(std::move(transaction)), cf_handles_(cf_handles) {
        read_options_.verify_checksums = false;
    }
    ~rocksdb_transaction_t();

    operation_result_t upsert(key_t key, value_spanc_t value) override;
    operation_result_t update(key_t key, value_spanc_t value) override;
    operation_result_t remove(key_t key) override;

    operation_result_t read(key_t key, value_span_t value) const override;
    operation_result_t batch_upsert(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) override;
    operation_result_t batch_read(keys_spanc_t keys, values_span_t values) const override;

    operation_result_t bulk_load(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) override;

    operation_result_t range_select(key_t key, size_t length, values_span_t values) const override;
    operation_result_t scan(key_t key, size_t length, value_span_t single_value) const override;

  private:
    std::unique_ptr<rocksdb::Transaction> transaction_;
    std::vector<rocksdb::ColumnFamilyHandle*> cf_handles_;

    rocksdb::ReadOptions read_options_;
};

rocksdb_transaction_t::~rocksdb_transaction_t() {
    transaction_batch_keys.clear();
    transaction_key_slices.clear();
    transaction_value_slices.clear();
    transaction_statuses.clear();

    auto status = transaction_->Commit();
    assert(status.ok());
}

operation_result_t rocksdb_transaction_t::upsert(key_t key, value_spanc_t value) {
    rocksdb::Status status = transaction_->Put(to_slice(key), to_slice(value));
    if (!status.ok()) {
        assert(status.IsTryAgain());
        status = transaction_->Commit();
        assert(status.ok());
        status = transaction_->Put(to_slice(key), to_slice(value));
        assert(status.ok());
    }
    return {1, operation_status_t::ok_k};
}

operation_result_t rocksdb_transaction_t::update(key_t key, value_spanc_t value) {

    key_t key_to_read = key, key_to_write = key;
    rocksdb::PinnableSlice data;
    rocksdb::Status status = transaction_->Get(read_options_, to_slice(key_to_read), &data);
    if (status.IsNotFound())
        return {0, operation_status_t::not_found_k};
    else if (!status.ok())
        return {0, operation_status_t::error_k};

    status = transaction_->Put(to_slice(key_to_write), to_slice(value));
    if (!status.ok()) {
        assert(status.IsTryAgain());
        status = transaction_->Commit();
        assert(status.ok());
        status = transaction_->Put(to_slice(key_to_write), to_slice(value));
        assert(status.ok());
    }
    return {1, operation_status_t::ok_k};
}

operation_result_t rocksdb_transaction_t::remove(key_t key) {
    rocksdb::Status status = transaction_->Delete(to_slice(key));
    if (!status.ok()) {
        assert(status.IsTryAgain());
        status = transaction_->Commit();
        assert(status.ok());
        status = transaction_->Delete(to_slice(key));
        assert(status.ok());
    }

    return {1, operation_status_t::ok_k};
}

operation_result_t rocksdb_transaction_t::read(key_t key, value_span_t value) const {
    rocksdb::PinnableSlice data;
    rocksdb::Status status = transaction_->Get(read_options_, to_slice(key), &data);
    if (status.IsNotFound())
        return {0, operation_status_t::not_found_k};
    else if (!status.ok())
        return {0, operation_status_t::error_k};

    memcpy(value.data(), data.data(), data.size());
    return {1, operation_status_t::ok_k};
}

operation_result_t rocksdb_transaction_t::batch_upsert(keys_spanc_t keys,
                                                       values_spanc_t values,
                                                       value_lengths_spanc_t sizes) {

    size_t offset = 0;
    for (size_t idx = 0; idx < keys.size(); ++idx) {
        auto key = keys[idx];
        rocksdb::Status status = transaction_->Put(to_slice(key), to_slice(values.subspan(offset, sizes[idx])));
        if (!status.ok()) {
            assert(status.IsTryAgain());
            status = transaction_->Commit();
            assert(status.ok());
            status = transaction_->Put(to_slice(key), to_slice(values.subspan(offset, sizes[idx])));
            assert(status.ok());
        }
        offset += sizes[idx];
    }
    return {keys.size(), operation_status_t::ok_k};
}

operation_result_t rocksdb_transaction_t::batch_read(keys_spanc_t keys, values_span_t values) const {

    if (keys.size() > transaction_batch_keys.size()) {
        transaction_batch_keys.resize(keys.size());
        transaction_key_slices.resize(keys.size());
        transaction_value_slices.resize(keys.size());
        transaction_statuses.resize(keys.size());
    }

    for (size_t idx = 0; idx < keys.size(); ++idx)
        transaction_key_slices[idx] = to_slice(transaction_batch_keys[idx] = keys[idx]);

    transaction_->MultiGet(read_options_,
                           cf_handles_.front(),
                           transaction_key_slices.size(),
                           transaction_key_slices.data(),
                           transaction_value_slices.data(),
                           transaction_statuses.data());

    size_t offset = 0;
    size_t found_cnt = 0;
    for (size_t i = 0; i < transaction_statuses.size(); ++i) {
        if (!transaction_statuses[i].ok())
            continue;

        memcpy(values.data() + offset, transaction_value_slices[i].data(), transaction_value_slices[i].size());
        offset += transaction_value_slices[i].size();
        ++found_cnt;
    }

    return {found_cnt, operation_status_t::ok_k};
}

operation_result_t rocksdb_transaction_t::bulk_load(keys_spanc_t keys,
                                                    values_spanc_t values,
                                                    value_lengths_spanc_t sizes) {
    return batch_upsert(keys, values, sizes);
}

operation_result_t rocksdb_transaction_t::range_select(key_t key, size_t length, values_span_t values) const {

    size_t i = 0;
    size_t exported_bytes = 0;
    std::unique_ptr<rocksdb::Iterator> it(transaction_->GetIterator(read_options_));
    it->Seek(to_slice(key));
    for (; it->Valid() && i != length; i++, it->Next()) {
        memcpy(values.data() + exported_bytes, it->value().data(), it->value().size());
        exported_bytes += it->value().size();
    }
    return {i, operation_status_t::ok_k};
}

operation_result_t rocksdb_transaction_t::scan(key_t key, size_t length, value_span_t single_value) const {

    size_t i = 0;
    rocksdb::ReadOptions scan_options = read_options_;
    // It's recommended to disable caching on long scans.
    // https://github.com/facebook/rocksdb/blob/49a10feb21dc5c766bb272406136667e1d8a969e/include/rocksdb/options.h#L1462
    scan_options.fill_cache = false;
    std::unique_ptr<rocksdb::Iterator> it(transaction_->GetIterator(scan_options));
    it->Seek(to_slice(key));
    for (; it->Valid() && i != length; i++, it->Next())
        memcpy(single_value.data(), it->value().data(), it->value().size());
    return {i, operation_status_t::ok_k};
}

} // namespace ucsb::facebook

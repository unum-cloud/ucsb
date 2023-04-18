#pragma once

#include <ustore/db.h>
#include <ustore/cpp/status.hpp>

#include "src/core/types.hpp"
#include "src/core/data_accessor.hpp"

namespace ucsb::ustore {

namespace fs = ucsb::fs;
namespace ustore = unum::ustore;

using key_t = ucsb::key_t;
using keys_spanc_t = ucsb::keys_spanc_t;
using value_span_t = ucsb::value_span_t;
using value_spanc_t = ucsb::value_spanc_t;
using values_span_t = ucsb::values_span_t;
using values_spanc_t = ucsb::values_spanc_t;
using value_lengths_spanc_t = ucsb::value_lengths_spanc_t;
using operation_status_t = ucsb::operation_status_t;
using operation_result_t = ucsb::operation_result_t;

thread_local ustore::arena_t arena_(nullptr);

inline ustore::value_view_t make_value(std::byte const* ptr, size_t length) {
    return {reinterpret_cast<ustore_bytes_cptr_t>(ptr), static_cast<ustore_length_t>(length)};
}

class ustore_transact_t : public ucsb::transaction_t {
  public:
    inline ustore_transact_t(ustore_database_t db, ustore_transaction_t transaction)
        : db_(db), transaction_(transaction), arena_(db_) {}
    ~ustore_transact_t();

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
    inline ustore::status_t commit() {
        ustore::status_t status;
        ustore_transaction_commit_t txn_commit {};
        txn_commit.db = db_;
        txn_commit.transaction = transaction_;
        txn_commit.error = status.member_ptr();
        txn_commit.options = options_;
        ustore_transaction_commit(&txn_commit);
        return status;
    }

    ustore_database_t db_;
    ustore_transaction_t transaction_;
    ustore_collection_t collection_ = ustore_collection_main_k;
    ustore_options_t options_ = ustore_options_default_k;
    ustore::arena_t mutable arena_;
};

ustore_transact_t::~ustore_transact_t() {
    [[maybe_unused]] auto status = commit();
    assert(status);
    ustore_transaction_free(transaction_);
}

operation_result_t ustore_transact_t::upsert(key_t key, value_spanc_t value) {
    ustore::status_t status;
    ustore_key_t key_ = key;
    ustore_length_t length = value.size();
    auto value_ = make_value(value.data(), value.size());

    ustore_write_t write {};
    write.db = db_;
    write.transaction = transaction_;
    write.error = status.member_ptr();
    write.arena = arena_.member_ptr();
    write.options = options_;
    write.tasks_count = 1;
    write.collections = &collection_;
    write.keys = &key_;
    write.lengths = reinterpret_cast<ustore_length_t const*>(&length);
    write.values = value_.member_ptr();
    ustore_write(&write);
    if (!status && commit()) {
        status.release_exception();
        ustore_write(&write);
    }

    return {size_t(status), status ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t ustore_transact_t::update(key_t key, value_spanc_t value) {
    ustore::status_t status;
    ustore_key_t key_ = key;
    ustore_byte_t* value_ = nullptr;

    ustore_read_t read {};
    read.db = db_;
    read.transaction = transaction_;
    read.error = status.member_ptr();
    read.arena = arena_.member_ptr();
    read.options = options_;
    read.tasks_count = 1;
    read.collections = &collection_;
    read.keys = &key_;
    read.values = &value_;
    ustore_read(&read);
    if (!status)
        return {0, operation_status_t::not_found_k};

    return upsert(key, value);
}

operation_result_t ustore_transact_t::remove(key_t key) {
    ustore::status_t status;
    ustore_key_t key_ = key;

    ustore_write_t write {};
    write.db = db_;
    write.transaction = transaction_;
    write.error = status.member_ptr();
    write.arena = arena_.member_ptr();
    write.options = options_;
    write.tasks_count = 1;
    write.collections = &collection_;
    write.keys = &key_;
    ustore_write(&write);
    if (!status && commit()) {
        status.release_exception();
        ustore_write(&write);
    }

    return {status ? size_t(1) : 0, status ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t ustore_transact_t::read(key_t key, value_span_t value) const {
    ustore::status_t status;
    ustore_key_t key_ = key;
    ustore_byte_t* value_ = nullptr;
    ustore_length_t* lengths = nullptr;

    ustore_read_t read {};
    read.db = db_;
    read.transaction = transaction_;
    read.error = status.member_ptr();
    read.arena = arena_.member_ptr();
    read.options = options_;
    read.tasks_count = 1;
    read.collections = &collection_;
    read.keys = &key_;
    read.lengths = &lengths;
    read.values = &value_;
    ustore_read(&read);
    if (!status)
        return {0, operation_status_t::error_k};
    if (lengths[0] == ustore_length_missing_k)
        return {0, operation_status_t::not_found_k};

    memcpy(value.data(), value_, lengths[0]);
    return {1, operation_status_t::ok_k};
}

operation_result_t ustore_transact_t::batch_upsert(keys_spanc_t keys,
                                                   values_spanc_t values,
                                                   value_lengths_spanc_t sizes) {
    ustore::status_t status;
    std::vector<ustore_length_t> offsets;
    offsets.reserve(sizes.size() + 1);
    offsets.push_back(0);
    for (auto size : sizes)
        offsets.push_back(offsets.back() + size);

    auto values_ = make_value(values.data(), values.size());
    ustore_write_t write {};
    write.db = db_;
    write.transaction = transaction_;
    write.error = status.member_ptr();
    write.arena = arena_.member_ptr();
    write.options = options_;
    write.tasks_count = keys.size();
    write.collections = &collection_;
    write.keys = reinterpret_cast<ustore_key_t const*>(keys.data());
    write.keys_stride = sizeof(ustore_key_t);
    write.offsets = offsets.data();
    write.offsets_stride = sizeof(ustore_length_t);
    write.lengths = reinterpret_cast<ustore_length_t const*>(sizes.data());
    write.lengths_stride = sizeof(ustore_length_t);
    write.values = values_.member_ptr();
    ustore_write(&write);
    if (!status && commit()) {
        status.release_exception();
        ustore_write(&write);
    }

    return {status ? keys.size() : 0, status ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t ustore_transact_t::batch_read(keys_spanc_t keys, values_span_t values) const {
    ustore::status_t status;
    ustore_octet_t* presences = nullptr;
    ustore_length_t* offsets = nullptr;
    ustore_length_t* lengths = nullptr;
    ustore_byte_t* values_ = nullptr;

    ustore_read_t read {};
    read.db = db_;
    read.transaction = transaction_;
    read.error = status.member_ptr();
    read.arena = arena_.member_ptr();
    read.options = options_;
    read.tasks_count = keys.size();
    read.collections = &collection_;
    read.keys = reinterpret_cast<ustore_key_t const*>(keys.data());
    read.keys_stride = sizeof(ustore_key_t);
    read.presences = &presences;
    read.offsets = &offsets;
    read.lengths = &lengths;
    read.values = &values_;
    ustore_read(&read);
    if (!status)
        return {0, operation_status_t::error_k};

    size_t offset = 0;
    size_t found_cnt = 0;
    for (size_t idx = 0; idx < keys.size(); ++idx) {
        if (lengths[idx] == ustore_length_missing_k)
            continue;
        memcpy(values.data() + offset, values_ + offsets[idx], lengths[idx]);
        offset += lengths[idx];
        ++found_cnt;
    }

    return {found_cnt, found_cnt > 0 ? operation_status_t::ok_k : operation_status_t::not_found_k};
}

operation_result_t ustore_transact_t::bulk_load(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) {
    return batch_upsert(keys, values, sizes);
}

operation_result_t ustore_transact_t::range_select(key_t key, size_t length, values_span_t values) const {
    ustore::status_t status;
    ustore_key_t key_ = key;
    ustore_length_t len = length;
    ustore_length_t* found_counts = nullptr;
    ustore_key_t* found_keys = nullptr;

    // First scan keys
    ustore_scan_t scan {};
    scan.db = db_;
    scan.transaction = transaction_;
    scan.error = status.member_ptr();
    scan.arena = arena_.member_ptr();
    scan.options = options_;
    scan.tasks_count = 1;
    scan.collections = &collection_;
    scan.start_keys = &key_;
    scan.count_limits = &len;
    scan.counts = &found_counts;
    scan.keys = &found_keys;
    ustore_scan(&scan);
    if (!status)
        return {0, operation_status_t::error_k};

    ustore_length_t* offsets = nullptr;
    ustore_length_t* lengths = nullptr;
    ustore_byte_t* values_ = nullptr;

    // Then do batch read
    ustore_read_t read {};
    read.db = db_;
    read.transaction = transaction_;
    read.error = status.member_ptr();
    read.arena = arena_.member_ptr();
    read.options = ustore_options_t(options_ | ustore_option_dont_discard_memory_k);
    read.tasks_count = *found_counts;
    read.collections = &collection_;
    read.keys = found_keys;
    read.keys_stride = sizeof(ustore_key_t);
    read.offsets = &offsets;
    read.lengths = &lengths;
    read.values = &values_;
    ustore_read(&read);
    if (!status)
        return {0, operation_status_t::error_k};

    size_t offset = 0;
    for (size_t idx = 0; idx < *found_counts; ++idx) {
        if (lengths[idx] == ustore_length_missing_k)
            continue;
        memcpy(values.data() + offset, values_ + offsets[idx], lengths[idx]);
        offset += lengths[idx];
    }

    return {*found_counts, *found_counts > 0 ? operation_status_t::ok_k : operation_status_t::not_found_k};
}

operation_result_t ustore_transact_t::scan(key_t key, size_t length, value_span_t single_value) const {
    ustore::status_t status;
    ustore_key_t key_ = key;
    ustore_length_t len =
        std::min<ustore_length_t>(length, 1'000'000); // Note: Don't scan all at once because the DB might be very big
    ustore_length_t* found_counts = nullptr;
    ustore_key_t* found_keys = nullptr;

    ustore_length_t* offsets = nullptr;
    ustore_length_t* lengths = nullptr;
    ustore_byte_t* values_ = nullptr;

    // Init scan
    ustore_scan_t scan {};
    scan.db = db_;
    scan.transaction = transaction_;
    scan.error = status.member_ptr();
    scan.arena = arena_.member_ptr();
    scan.options = options_;
    scan.tasks_count = 1;
    scan.collections = &collection_;
    scan.start_keys = &key_;
    scan.count_limits = &len;
    scan.counts = &found_counts;
    scan.keys = &found_keys;

    // Init batch read
    ustore_read_t read {};
    read.db = db_;
    read.transaction = transaction_;
    read.error = status.member_ptr();
    read.arena = arena_.member_ptr();
    read.options = ustore_options_t(options_ | ustore_option_dont_discard_memory_k);
    read.collections = &collection_;
    read.keys_stride = sizeof(ustore_key_t);
    read.offsets = &offsets;
    read.lengths = &lengths;
    read.values = &values_;

    ustore_length_t scanned = 0;
    ustore_length_t remaining_keys_cnt = length;
    while (remaining_keys_cnt) {
        // First scan
        ustore_scan(&scan);
        if (!status)
            return {0, operation_status_t::error_k};

        // Then read
        read.tasks_count = *found_counts;
        read.keys = found_keys;
        ustore_read(&read);
        if (!status)
            return {0, operation_status_t::error_k};

        scanned += *found_counts;
        for (size_t idx = 0; idx < *found_counts; ++idx)
            if (lengths[idx] != ustore_length_missing_k)
                memcpy(single_value.data(), values_ + offsets[idx], lengths[idx]);

        key_ += len;
        remaining_keys_cnt = remaining_keys_cnt - len;
        len = std::min(len, remaining_keys_cnt);
    }

    return {scanned, scanned > 0 ? operation_status_t::ok_k : operation_status_t::not_found_k};
}

} // namespace ucsb::ustore
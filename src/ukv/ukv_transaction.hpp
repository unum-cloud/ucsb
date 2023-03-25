#pragma once

#include <ukv/db.h>
#include <ukv/cpp/status.hpp>

#include "src/core/types.hpp"
#include "src/core/data_accessor.hpp"

namespace ucsb::ukv {

namespace fs = ucsb::fs;
namespace ukv = unum::ukv;

using key_t = ucsb::key_t;
using keys_spanc_t = ucsb::keys_spanc_t;
using value_span_t = ucsb::value_span_t;
using value_spanc_t = ucsb::value_spanc_t;
using values_span_t = ucsb::values_span_t;
using values_spanc_t = ucsb::values_spanc_t;
using value_lengths_spanc_t = ucsb::value_lengths_spanc_t;
using operation_status_t = ucsb::operation_status_t;
using operation_result_t = ucsb::operation_result_t;

thread_local ukv::arena_t arena_(nullptr);

inline ukv::value_view_t make_value(std::byte const* ptr, size_t length) {
    return {reinterpret_cast<ukv_bytes_cptr_t>(ptr), static_cast<ukv_length_t>(length)};
}

class ukv_transact_t : public ucsb::transaction_t {
  public:
    inline ukv_transact_t(ukv_database_t db, ukv_transaction_t transaction)
        : db_(db), transaction_(transaction), arena_(db_) {}
    ~ukv_transact_t();

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
    inline ukv::status_t commit() {
        ukv::status_t status;
        ukv_transaction_commit_t txn_commit {};
        txn_commit.db = db_;
        txn_commit.transaction = transaction_;
        txn_commit.error = status.member_ptr();
        txn_commit.options = options_;
        ukv_transaction_commit(&txn_commit);
        return status;
    }

    ukv_database_t db_;
    ukv_transaction_t transaction_;
    ukv_collection_t collection_ = ukv_collection_main_k;
    ukv_options_t options_ = ukv_options_default_k;
    ukv::arena_t mutable arena_;
};

ukv_transact_t::~ukv_transact_t() {
    [[maybe_unused]] auto status = commit();
    assert(status);
    ukv_transaction_free(transaction_);
}

operation_result_t ukv_transact_t::upsert(key_t key, value_spanc_t value) {
    ukv::status_t status;
    ukv_key_t key_ = key;
    ukv_length_t length = value.size();
    auto value_ = make_value(value.data(), value.size());

    ukv_write_t write {};
    write.db = db_;
    write.transaction = transaction_;
    write.error = status.member_ptr();
    write.arena = arena_.member_ptr();
    write.options = options_;
    write.tasks_count = 1;
    write.collections = &collection_;
    write.keys = &key_;
    write.lengths = reinterpret_cast<ukv_length_t const*>(&length);
    write.values = value_.member_ptr();
    ukv_write(&write);
    if (!status && commit()) {
        status.release_exception();
        ukv_write(&write);
    }

    return {size_t(status), status ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t ukv_transact_t::update(key_t key, value_spanc_t value) {
    ukv::status_t status;
    ukv_key_t key_ = key;
    ukv_byte_t* value_ = nullptr;

    ukv_read_t read {};
    read.db = db_;
    read.transaction = transaction_;
    read.error = status.member_ptr();
    read.arena = arena_.member_ptr();
    read.options = options_;
    read.tasks_count = 1;
    read.collections = &collection_;
    read.keys = &key_;
    read.values = &value_;
    ukv_read(&read);
    if (!status)
        return {0, operation_status_t::not_found_k};

    return upsert(key, value);
}

operation_result_t ukv_transact_t::remove(key_t key) {
    ukv::status_t status;
    ukv_key_t key_ = key;

    ukv_write_t write {};
    write.db = db_;
    write.transaction = transaction_;
    write.error = status.member_ptr();
    write.arena = arena_.member_ptr();
    write.options = options_;
    write.tasks_count = 1;
    write.collections = &collection_;
    write.keys = &key_;
    ukv_write(&write);
    if (!status && commit()) {
        status.release_exception();
        ukv_write(&write);
    }

    return {status ? size_t(1) : 0, status ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t ukv_transact_t::read(key_t key, value_span_t value) const {
    ukv::status_t status;
    ukv_key_t key_ = key;
    ukv_byte_t* value_ = nullptr;
    ukv_length_t* lengths = nullptr;

    ukv_read_t read {};
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
    ukv_read(&read);
    if (!status)
        return {0, operation_status_t::error_k};
    if (lengths[0] == ukv_length_missing_k)
        return {0, operation_status_t::not_found_k};

    memcpy(value.data(), value_, lengths[0]);
    return {1, operation_status_t::ok_k};
}

operation_result_t ukv_transact_t::batch_upsert(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) {
    ukv::status_t status;
    std::vector<ukv_length_t> offsets;
    offsets.reserve(sizes.size() + 1);
    offsets.push_back(0);
    for (auto size : sizes)
        offsets.push_back(offsets.back() + size);

    auto values_ = make_value(values.data(), values.size());
    ukv_write_t write {};
    write.db = db_;
    write.transaction = transaction_;
    write.error = status.member_ptr();
    write.arena = arena_.member_ptr();
    write.options = options_;
    write.tasks_count = keys.size();
    write.collections = &collection_;
    write.keys = reinterpret_cast<ukv_key_t const*>(keys.data());
    write.keys_stride = sizeof(ukv_key_t);
    write.offsets = offsets.data();
    write.offsets_stride = sizeof(ukv_length_t);
    write.lengths = reinterpret_cast<ukv_length_t const*>(sizes.data());
    write.lengths_stride = sizeof(ukv_length_t);
    write.values = values_.member_ptr();
    ukv_write(&write);
    if (!status && commit()) {
        status.release_exception();
        ukv_write(&write);
    }

    return {status ? keys.size() : 0, status ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t ukv_transact_t::batch_read(keys_spanc_t keys, values_span_t values) const {
    ukv::status_t status;
    ukv_octet_t* presences = nullptr;
    ukv_length_t* offsets = nullptr;
    ukv_length_t* lengths = nullptr;
    ukv_byte_t* values_ = nullptr;

    ukv_read_t read {};
    read.db = db_;
    read.transaction = transaction_;
    read.error = status.member_ptr();
    read.arena = arena_.member_ptr();
    read.options = options_;
    read.tasks_count = keys.size();
    read.collections = &collection_;
    read.keys = reinterpret_cast<ukv_key_t const*>(keys.data());
    read.keys_stride = sizeof(ukv_key_t);
    read.presences = &presences;
    read.offsets = &offsets;
    read.lengths = &lengths;
    read.values = &values_;
    ukv_read(&read);
    if (!status)
        return {0, operation_status_t::error_k};

    size_t offset = 0;
    size_t found_cnt = 0;
    for (size_t idx = 0; idx < keys.size(); ++idx) {
        if (lengths[idx] == ukv_length_missing_k)
            continue;
        memcpy(values.data() + offset, values_ + offsets[idx], lengths[idx]);
        offset += lengths[idx];
        ++found_cnt;
    }

    return {found_cnt, found_cnt > 0 ? operation_status_t::ok_k : operation_status_t::not_found_k};
}

operation_result_t ukv_transact_t::bulk_load(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) {
    return batch_upsert(keys, values, sizes);
}

operation_result_t ukv_transact_t::range_select(key_t key, size_t length, values_span_t values) const {
    ukv::status_t status;
    ukv_key_t key_ = key;
    ukv_length_t len = length;
    ukv_length_t* found_counts = nullptr;
    ukv_key_t* found_keys = nullptr;

    // First scan keys
    ukv_scan_t scan {};
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
    ukv_scan(&scan);
    if (!status)
        return {0, operation_status_t::error_k};

    ukv_length_t* offsets = nullptr;
    ukv_length_t* lengths = nullptr;
    ukv_byte_t* values_ = nullptr;

    // Then do batch read
    ukv_read_t read {};
    read.db = db_;
    read.transaction = transaction_;
    read.error = status.member_ptr();
    read.arena = arena_.member_ptr();
    read.options = ukv_options_t(options_ | ukv_option_dont_discard_memory_k);
    read.tasks_count = *found_counts;
    read.collections = &collection_;
    read.keys = found_keys;
    read.keys_stride = sizeof(ukv_key_t);
    read.offsets = &offsets;
    read.lengths = &lengths;
    read.values = &values_;
    ukv_read(&read);
    if (!status)
        return {0, operation_status_t::error_k};

    size_t offset = 0;
    for (size_t idx = 0; idx < *found_counts; ++idx) {
        if (lengths[idx] == ukv_length_missing_k)
            continue;
        memcpy(values.data() + offset, values_ + offsets[idx], lengths[idx]);
        offset += lengths[idx];
    }

    return {*found_counts, *found_counts > 0 ? operation_status_t::ok_k : operation_status_t::not_found_k};
}

operation_result_t ukv_transact_t::scan(key_t key, size_t length, value_span_t single_value) const {
    ukv::status_t status;
    ukv_key_t key_ = key;
    ukv_length_t len =
        std::min<ukv_length_t>(length, 1'000'000); // Note: Don't scan all at once because the DB might be very big
    ukv_length_t* found_counts = nullptr;
    ukv_key_t* found_keys = nullptr;

    ukv_length_t* offsets = nullptr;
    ukv_length_t* lengths = nullptr;
    ukv_byte_t* values_ = nullptr;

    // Init scan
    ukv_scan_t scan {};
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
    ukv_read_t read {};
    read.db = db_;
    read.transaction = transaction_;
    read.error = status.member_ptr();
    read.arena = arena_.member_ptr();
    read.options = ukv_options_t(options_ | ukv_option_dont_discard_memory_k);
    read.collections = &collection_;
    read.keys_stride = sizeof(ukv_key_t);
    read.offsets = &offsets;
    read.lengths = &lengths;
    read.values = &values_;

    ukv_length_t scanned = 0;
    ukv_length_t remaining_keys_cnt = length;
    while (remaining_keys_cnt) {
        // First scan
        ukv_scan(&scan);
        if (!status)
            return {0, operation_status_t::error_k};

        // Then read
        read.tasks_count = *found_counts;
        read.keys = found_keys;
        ukv_read(&read);
        if (!status)
            return {0, operation_status_t::error_k};

        scanned += *found_counts;
        for (size_t idx = 0; idx < *found_counts; ++idx)
            if (lengths[idx] != ukv_length_missing_k)
                memcpy(single_value.data(), values_ + offsets[idx], lengths[idx]);

        key_ += len;
        remaining_keys_cnt = remaining_keys_cnt - len;
        len = std::min(len, remaining_keys_cnt);
    }

    return {scanned, scanned > 0 ? operation_status_t::ok_k : operation_status_t::not_found_k};
}

} // namespace ucsb::ukv
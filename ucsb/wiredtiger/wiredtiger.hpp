#pragma once

#include <string>
#include <vector>
#include <fmt/format.h>

#include <wiredtiger.h>

#include "ucsb/core/types.hpp"
#include "ucsb/core/db.hpp"
#include "ucsb/core/helper.hpp"

namespace mongodb {

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

/**
 * @brief WiredTiger wrapper for the UCSB benchmark.
 * WiredTiger is the core key-value engine of MongoDB.
 * https://github.com/wiredtiger/wiredtiger
 */
struct wiredtiger_t : public ucsb::db_t {

    inline wiredtiger_t()
        : conn_(nullptr), session_(nullptr), cursor_(nullptr), batch_insert_cursor_(nullptr),
          table_name_("table:access") {}
    inline ~wiredtiger_t() override = default;

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

    WT_CONNECTION* conn_;
    WT_SESSION* session_;
    mutable WT_CURSOR* cursor_;
    WT_CURSOR* batch_insert_cursor_;
    std::string table_name_;
};

inline int compare_keys(
    WT_COLLATOR* collator, WT_SESSION* session, WT_ITEM const* left, WT_ITEM const* right, int* res) noexcept {
    (void)collator;
    (void)session;

    key_t left_key = *reinterpret_cast<key_t const*>(left->data);
    key_t right_key = *reinterpret_cast<key_t const*>(right->data);
    *res = left_key < right_key ? -1 : left_key > right_key;
    return 0;
}

WT_COLLATOR key_comparator = {compare_keys, nullptr, nullptr};

void wiredtiger_t::set_config(fs::path const& config_path, fs::path const& dir_path) {
    config_path_ = config_path;
    dir_path_ = dir_path;
}

bool wiredtiger_t::open() {

    int res = wiredtiger_open(dir_path_.c_str(), NULL, "create", &conn_);
    if (res)
        return false;

    res = conn_->open_session(conn_, NULL, NULL, &session_);
    if (res) {
        close();
        return false;
    }

    // res = conn_->add_collator(conn_, "key_comparator", &key_comparator, NULL);
    // if (res) {
    //     close();
    //     return false;
    // }

    static_assert(sizeof(key_t) == sizeof(uint64_t), "Need to change `key_format` below");
    res = session_->create(session_, table_name_.c_str(), "key_format=Q,value_format=u");
    if (res) {
        close();
        return false;
    }

    res = session_->open_cursor(session_, table_name_.c_str(), NULL, NULL, &cursor_);
    if (res) {
        close();
        return false;
    }

    return true;
}

bool wiredtiger_t::close() {
    if (!conn_)
        return true;

    int res = conn_->close(conn_, NULL);
    if (res)
        return false;

    cursor_ = nullptr;
    batch_insert_cursor_ = nullptr;
    session_ = nullptr;
    conn_ = nullptr;
    return true;
}

void wiredtiger_t::destroy() {
    if (session_)
        session_->drop(session_, table_name_.c_str(), "key_format=Q,value_format=u");

    bool ok = close();
    assert(ok);

    ucsb::clear_directory(dir_path_);
}

operation_result_t wiredtiger_t::insert(key_t key, value_spanc_t value) {

    cursor_->set_key(cursor_, key);
    WT_ITEM db_value;
    db_value.data = value.data();
    db_value.size = value.size();
    cursor_->set_value(cursor_, &db_value);
    int res = cursor_->insert(cursor_);
    cursor_->reset(cursor_);
    return {1, res == 0 ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t wiredtiger_t::update(key_t key, value_spanc_t value) {

    cursor_->set_key(cursor_, key);
    WT_ITEM db_value;
    db_value.data = value.data();
    db_value.size = value.size();
    cursor_->set_value(cursor_, &db_value);
    int res = cursor_->update(cursor_);
    cursor_->reset(cursor_);
    return {1, res == 0 ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t wiredtiger_t::remove(key_t key) {

    cursor_->set_key(cursor_, key);
    int res = cursor_->remove(cursor_);
    cursor_->reset(cursor_);
    return {1, res == 0 ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t wiredtiger_t::read(key_t key, value_span_t value) const {

    cursor_->set_key(cursor_, key);
    int res = cursor_->search(cursor_);
    if (res)
        return {0, operation_status_t::error_k};
    WT_ITEM db_value;
    res = cursor_->get_value(cursor_, &db_value);
    cursor_->reset(cursor_);
    if (res)
        return {1, operation_status_t::not_found_k};

    memcpy(value.data(), db_value.data, db_value.size);
    return {1, operation_status_t::ok_k};
}

operation_result_t wiredtiger_t::batch_insert(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) {

    size_t offset = 0;
    for (size_t idx = 0; idx < keys.size(); ++idx) {
        cursor_->set_key(cursor_, &keys[idx]);
        WT_ITEM db_value;
        db_value.data = values.data() + offset;
        db_value.size = sizes[idx];
        cursor_->set_value(cursor_, &db_value);
        int res = cursor_->insert(cursor_);
        cursor_->reset(cursor_);
        if (res)
            return {0, operation_status_t::error_k};
        offset += sizes[idx];
    }
    return {keys.size(), operation_status_t::ok_k};
}

operation_result_t wiredtiger_t::batch_read(keys_spanc_t keys, values_span_t values) const {

    // Note: imitation of batch read!
    size_t offset = 0;
    size_t found_cnt = 0;
    for (auto key : keys) {
        WT_ITEM db_value;
        cursor_->set_key(cursor_, key);
        int res = cursor_->search(cursor_);
        if (res == 0) {
            res = cursor_->get_value(cursor_, &db_value);
            if (res == 0) {
                memcpy(values.data() + offset, db_value.data, db_value.size);
                offset += db_value.size;
                ++found_cnt;
            }
        }
        cursor_->reset(cursor_);
    }
    return {found_cnt, operation_status_t::ok_k};
}

operation_result_t wiredtiger_t::bulk_insert(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) {
    // Warnings:
    //   DB must be empty
    //   No other cursors while doing batch insert

    if (cursor_) {
        cursor_->close(cursor_);
        cursor_ = nullptr;
    }

    if (batch_insert_cursor_ == nullptr) {
        // Batch cursor will be closed in flush()
        auto res = session_->open_cursor(session_, table_name_.c_str(), NULL, "bulk", &batch_insert_cursor_);
        if (res)
            return {0, operation_status_t::error_k};
    }

    size_t offset = 0;
    for (size_t idx = 0; idx < keys.size(); ++idx) {
        batch_insert_cursor_->set_key(batch_insert_cursor_, keys[idx]);
        WT_ITEM db_value;
        db_value.data = &values[offset];
        db_value.size = sizes[idx];
        batch_insert_cursor_->set_value(batch_insert_cursor_, &db_value);
        batch_insert_cursor_->insert(batch_insert_cursor_);
        offset += sizes[idx];
    }

    return {keys.size(), operation_status_t::ok_k};
}

operation_result_t wiredtiger_t::range_select(key_t key, size_t length, values_span_t values) const {

    cursor_->set_key(cursor_, key);
    int res = cursor_->search(cursor_);
    if (res)
        return {0, operation_status_t::error_k};

    size_t i = 0;
    WT_ITEM db_value;
    const char* db_key = nullptr;
    size_t offset = 0;
    size_t selected_records_count = 0;
    while ((res = cursor_->next(cursor_)) == 0 && i++ < length) {
        res = cursor_->get_key(cursor_, &db_key);
        res |= cursor_->get_value(cursor_, &db_value);
        if (res == 0) {
            memcpy(values.data() + offset, db_value.data, db_value.size);
            offset += db_value.size;
            ++selected_records_count;
        }
    }
    return {selected_records_count, operation_status_t::ok_k};
}

operation_result_t wiredtiger_t::scan(key_t key, size_t length, value_span_t single_value) const {

    cursor_->set_key(cursor_, key);
    int res = cursor_->search(cursor_);
    if (res)
        return {0, operation_status_t::error_k};

    size_t i = 0;
    WT_ITEM db_value;
    const char* db_key = nullptr;
    size_t scanned_records_count = 0;
    while ((res = cursor_->next(cursor_)) == 0 && i++ < length) {
        res = cursor_->get_key(cursor_, &db_key);
        res |= cursor_->get_value(cursor_, &db_value);
        if (res == 0) {
            memcpy(single_value.data(), db_value.data, db_value.size);
            ++scanned_records_count;
        }
    }
    return {scanned_records_count, operation_status_t::ok_k};
}

void wiredtiger_t::flush() {
    if (batch_insert_cursor_) {
        batch_insert_cursor_->close(batch_insert_cursor_);
        batch_insert_cursor_ = nullptr;
    }
}

size_t wiredtiger_t::size_on_disk() const {
    return ucsb::size_on_disk(dir_path_);
}

std::unique_ptr<transaction_t> wiredtiger_t::create_transaction() {
    return {};
}

} // namespace mongodb
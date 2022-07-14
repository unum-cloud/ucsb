#pragma once

#include <string>
#include <vector>
#include <fmt/format.h>

#include <wiredtiger.h>

#include "ucsb/core/types.hpp"
#include "ucsb/core/db.hpp"
#include "ucsb/core/helper.hpp"
#include "ucsb/core/printable.hpp"

namespace mongodb {

namespace fs = ucsb::fs;

struct session_deleter_t {
    void operator()(WT_SESSION* session) { session->close(session, NULL); }
};

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
using session_uptr_t = std::unique_ptr<WT_SESSION, session_deleter_t>;

/**
 * @brief WiredTiger wrapper for the UCSB benchmark.
 * WiredTiger is the core key-value engine of MongoDB.
 * https://github.com/wiredtiger/wiredtiger
 */

struct wiredtiger_t : public ucsb::db_t {

    inline wiredtiger_t()
        : conn_(nullptr), bulk_load_session_(nullptr), bulk_load_cursor_(nullptr), table_name_("table:access") {}
    inline ~wiredtiger_t() override = default;

    void set_config(fs::path const& config_path, fs::path const& dir_path) override;
    bool open() override;
    bool close() override;
    void destroy() override;

    operation_result_t upsert(key_t key, value_spanc_t value) override;
    operation_result_t update(key_t key, value_spanc_t value) override;
    operation_result_t remove(key_t key) override;

    operation_result_t read(key_t key, value_span_t value) const override;
    operation_result_t batch_upsert(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) override;
    operation_result_t batch_read(keys_spanc_t keys, values_span_t values) const override;

    operation_result_t bulk_load(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) override;

    operation_result_t range_select(key_t key, size_t length, values_span_t values) const override;
    operation_result_t scan(key_t key, size_t length, value_span_t single_value) const override;

    void flush() override;
    size_t size_on_disk() const override;

    std::unique_ptr<transaction_t> create_transaction() override;

    session_uptr_t start_session() const;
    WT_CURSOR* get_cursor(WT_SESSION* session, const char* config) const;

  private:
    struct config_t {
        size_t cache_size = 0;
    };

    inline bool load_config(config_t& config);
    inline std::string create_str_config(config_t const& config) const;

    fs::path config_path_;
    fs::path dir_path_;

    WT_CONNECTION* conn_;
    session_uptr_t bulk_load_session_;
    WT_CURSOR* bulk_load_cursor_;
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

session_uptr_t wiredtiger_t::start_session() const {

    WT_SESSION* session = nullptr;
    auto res = conn_->open_session(conn_, NULL, NULL, &session);
    if (res)
        return nullptr;

    res = session->create(session, table_name_.c_str(), "key_format=Q,value_format=u");
    if (res)
        return nullptr;

    return session_uptr_t(session, session_deleter_t {});
}

WT_CURSOR* wiredtiger_t::get_cursor(WT_SESSION* session, const char* config) const {
    if (!session)
        return nullptr;

    WT_CURSOR* cursor = nullptr;
    auto res = session->open_cursor(session, table_name_.c_str(), NULL, config, &cursor);
    if (res)
        return nullptr;

    return cursor;
}

bool wiredtiger_t::open() {

    if (conn_)
        return true;

    config_t config;
    if (!load_config(config))
        return false;

    std::string str_config = create_str_config(config);
    int res = wiredtiger_open(dir_path_.c_str(), NULL, str_config.c_str(), &conn_);
    if (res)
        return false;

    return true;
}

bool wiredtiger_t::close() {
    if (!conn_)
        return true;

    int res = conn_->close(conn_, NULL);
    if (res)
        return false;

    conn_ = nullptr;
    return true;
}

void wiredtiger_t::destroy() {
    [[maybe_unused]] bool ok = close();
    assert(ok);

    ucsb::clear_directory(dir_path_);
}

operation_result_t wiredtiger_t::upsert(key_t key, value_spanc_t value) {

    auto session = start_session();
    auto cursor = get_cursor(session.get(), NULL);
    if (!cursor)
        return {0, operation_status_t::error_k};

    cursor->set_key(cursor, key);
    WT_ITEM db_value;
    db_value.data = value.data();
    db_value.size = value.size();
    cursor->set_value(cursor, &db_value);
    auto res = cursor->insert(cursor);
    cursor->reset(cursor);

    return {1, res == 0 ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t wiredtiger_t::update(key_t key, value_spanc_t value) {

    auto session = start_session();
    auto cursor = get_cursor(session.get(), NULL);
    if (!cursor)
        return {1, operation_status_t::error_k};

    cursor->set_key(cursor, key);
    WT_ITEM db_value;
    db_value.data = value.data();
    db_value.size = value.size();
    cursor->set_value(cursor, &db_value);
    auto res = cursor->update(cursor);
    cursor->reset(cursor);

    return {1, res == 0 ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t wiredtiger_t::remove(key_t key) {

    auto session = start_session();
    auto cursor = get_cursor(session.get(), NULL);
    if (!cursor)
        return {1, operation_status_t::error_k};

    cursor->set_key(cursor, key);
    auto res = cursor->remove(cursor);
    cursor->reset(cursor);

    return {1, res == 0 ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t wiredtiger_t::read(key_t key, value_span_t value) const {

    auto session = start_session();
    auto cursor = get_cursor(session.get(), NULL);
    if (!cursor)
        return {1, operation_status_t::error_k};

    cursor->set_key(cursor, key);
    auto res = cursor->search(cursor);
    if (res)
        return {1, operation_status_t::not_found_k};

    WT_ITEM db_value;
    res = cursor->get_value(cursor, &db_value);
    cursor->reset(cursor);
    if (res)
        return {1, operation_status_t::not_found_k};

    memcpy(value.data(), db_value.data, db_value.size);

    return {1, operation_status_t::ok_k};
}

operation_result_t wiredtiger_t::batch_upsert(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) {

    auto session = start_session();
    auto cursor = get_cursor(session.get(), NULL);
    if (!cursor)
        return {1, operation_status_t::error_k};

    size_t offset = 0;
    size_t upserted = 0;
    for (size_t idx = 0; idx < keys.size(); ++idx) {
        cursor->set_key(cursor, keys[idx]);
        WT_ITEM db_value;
        db_value.data = values.data() + offset;
        db_value.size = sizes[idx];
        cursor->set_value(cursor, &db_value);
        int res = cursor->insert(cursor);
        cursor->reset(cursor);
        if (!res)
            upserted++;
        offset += sizes[idx];
    }

    return {upserted, operation_status_t::ok_k};
}

operation_result_t wiredtiger_t::batch_read(keys_spanc_t keys, values_span_t values) const {

    auto session = start_session();
    auto cursor = get_cursor(session.get(), NULL);
    if (!cursor)
        return {keys.size(), operation_status_t::error_k};

    // Note: imitation of batch read!
    size_t offset = 0;
    size_t found_cnt = 0;
    for (auto key : keys) {
        WT_ITEM db_value;
        cursor->set_key(cursor, key);
        int res = cursor->search(cursor);
        if (res == 0) {
            res = cursor->get_value(cursor, &db_value);
            if (res == 0) {
                memcpy(values.data() + offset, db_value.data, db_value.size);
                offset += db_value.size;
                ++found_cnt;
            }
        }
        cursor->reset(cursor);
    }

    return {found_cnt, operation_status_t::ok_k};
}

operation_result_t wiredtiger_t::bulk_load(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) {
    // Warnings:
    //   DB must be empty
    //   No other cursors while doing bulk load

    if (bulk_load_cursor_ == nullptr) {
        bulk_load_session_ = start_session();
        bulk_load_cursor_ = get_cursor(bulk_load_session_.get(), "bulk");
        if (!bulk_load_cursor_)
            return {keys.size(), operation_status_t::error_k};
    }

    size_t offset = 0;
    for (size_t idx = 0; idx < keys.size(); ++idx) {
        bulk_load_cursor_->set_key(bulk_load_cursor_, keys[idx]);
        WT_ITEM db_value;
        db_value.data = &values[offset];
        db_value.size = sizes[idx];
        bulk_load_cursor_->set_value(bulk_load_cursor_, &db_value);
        bulk_load_cursor_->insert(bulk_load_cursor_);
        offset += sizes[idx];
    }

    return {keys.size(), operation_status_t::ok_k};
}

operation_result_t wiredtiger_t::range_select(key_t key, size_t length, values_span_t values) const {

    auto session = start_session();
    auto cursor = get_cursor(session.get(), NULL);
    if (!cursor)
        return {length, operation_status_t::error_k};

    cursor->set_key(cursor, key);
    auto res = cursor->search(cursor);
    if (res)
        return {length, operation_status_t::error_k};

    size_t i = 0;
    WT_ITEM db_value;
    const char* db_key = nullptr;
    size_t offset = 0;
    size_t selected_records_count = 0;
    while ((res = cursor->next(cursor)) == 0 && i++ < length) {
        res = cursor->get_key(cursor, &db_key);
        res |= cursor->get_value(cursor, &db_value);
        if (res == 0) {
            memcpy(values.data() + offset, db_value.data, db_value.size);
            offset += db_value.size;
            ++selected_records_count;
        }
    }

    return {selected_records_count, operation_status_t::ok_k};
}

operation_result_t wiredtiger_t::scan(key_t key, size_t length, value_span_t single_value) const {

    auto session = start_session();
    auto cursor = get_cursor(session.get(), NULL);
    if (!cursor)
        return {length, operation_status_t::error_k};

    cursor->set_key(cursor, key);
    auto res = cursor->search(cursor);
    if (res)
        return {length, operation_status_t::error_k};

    size_t i = 0;
    WT_ITEM db_value;
    const char* db_key = nullptr;
    size_t scanned_records_count = 0;
    while ((res = cursor->next(cursor)) == 0 && i++ < length) {
        res = cursor->get_key(cursor, &db_key);
        res |= cursor->get_value(cursor, &db_value);
        if (res == 0) {
            memcpy(single_value.data(), db_value.data, db_value.size);
            ++scanned_records_count;
        }
    }

    return {scanned_records_count, operation_status_t::ok_k};
}

void wiredtiger_t::flush() {
    if (bulk_load_cursor_) {
        bulk_load_cursor_->close(bulk_load_cursor_);
        bulk_load_cursor_ = nullptr;
    }
    if (bulk_load_session_) {
        bulk_load_session_.reset();
        bulk_load_session_ = nullptr;
    }
}

size_t wiredtiger_t::size_on_disk() const {
    return ucsb::size_on_disk(dir_path_);
}

std::unique_ptr<transaction_t> wiredtiger_t::create_transaction() {
    return {};
}

bool wiredtiger_t::load_config(config_t& config) {
    if (!fs::exists(config_path_))
        return false;

    std::ifstream i_config(config_path_);
    nlohmann::json j_config;
    i_config >> j_config;

    config.cache_size = j_config.value<size_t>("cache_size", 100'000'000);

    return true;
}

inline std::string wiredtiger_t::create_str_config(config_t const& config) const {

    std::string str_config = "create";
    std::string str_cache_size = fmt::format("cache_size={:.0M}", ucsb::printable_bytes_t {config.cache_size});
    return fmt::format("{},{}", str_config, str_cache_size);
}

} // namespace mongodb
#pragma once

#include <string>
#include <vector>
#include <fmt/format.h>

#include <wiredtiger.h>

#include "ucsb/core/types.hpp"
#include "ucsb/core/db.hpp"

namespace mongodb {

void testutil_die(int e, const char* fmt, ...) {
    va_list ap;

    /* Flush output to be sure it doesn't mix with fatal errors. */
    (void)fflush(stdout);
    (void)fflush(stderr);

    if (fmt != NULL) {
        fprintf(stderr, ": ");
        va_start(ap, fmt);
        vfprintf(stderr, fmt, ap);
        va_end(ap);
    }
    if (e != 0)
        fprintf(stderr, ": %s", wiredtiger_strerror(e));

    throw std::runtime_error("WIREDTIGER unwired");
}

#define error_check(call)                                                         \
    do {                                                                          \
        int __r;                                                                  \
        if ((__r = (call)) != 0 && __r != ENOTSUP)                                \
            testutil_die(__r, "%s/%d: %s", __PRETTY_FUNCTION__, __LINE__, #call); \
    } while (0)

namespace fs = ucsb::fs;

using key_t = ucsb::key_t;
using keys_span_t = ucsb::keys_span_t;
using value_span_t = ucsb::value_span_t;
using value_spanc_t = ucsb::value_spanc_t;
using operation_status_t = ucsb::operation_status_t;
using operation_result_t = ucsb::operation_result_t;

struct wiredtiger_t : public ucsb::db_t {
  public:
    inline wiredtiger_t()
        : conn_(nullptr), session_(nullptr), cursor_(nullptr), table_name_("table:access"), key_buffer_(100) {}
    ~wiredtiger_t() override = default;

    bool init(fs::path const& config_path, fs::path const& dir_path) override;
    void destroy() override;

    operation_result_t insert(key_t key, value_spanc_t value) override;
    operation_result_t update(key_t key, value_spanc_t value) override;
    operation_result_t remove(key_t key) override;

    operation_result_t read(key_t key, value_span_t value) const override;
    operation_result_t batch_read(keys_span_t keys) const override;

    operation_result_t range_select(key_t key, size_t length, value_span_t single_value) const override;
    operation_result_t scan(value_span_t single_value) const override;

  private:
    char const* build_key(key_t key);

    WT_CONNECTION* conn_;
    WT_SESSION* session_;
    WT_CURSOR* cursor_;
    std::string table_name_;
    std::vector<char> key_buffer_;
    std::vector<char> value_buffer_;
};

bool wiredtiger_t::init(fs::path const& config_path, fs::path const& dir_path) {

    error_check(wiredtiger_open(dir_path.c_str(), NULL, "create", &conn_));
    error_check(conn_->open_session(conn_, NULL, NULL, &session_));
    error_check(session_->create(session_, table_name_.c_str(), "key_format=S,value_format=u"));
    error_check(session_->open_cursor(session_, table_name_.c_str(), NULL, NULL, &cursor_));

    return true;
}

void wiredtiger_t::destroy() {
    error_check(conn_->close(conn_, NULL));
    cursor_ = nullptr;
    session_ = nullptr;
    conn_ = nullptr;
}

operation_result_t wiredtiger_t::insert(key_t key, value_spanc_t value) {

    auto db_key = build_key(key);
    cursor_->set_key(cursor_, db_key);
    WT_ITEM db_value;
    db_value.data = value.data();
    db_value.size = value.size();
    cursor_->set_value(cursor_, &db_value);
    int res = cursor_->insert(cursor_);
    cursor_->reset(cursor_);
    if (res)
        return {0, operation_status_t::error_k};
    return {1, operation_status_t::ok_k};
}

operation_result_t wiredtiger_t::update(key_t key, value_spanc_t value) {

    auto db_key = build_key(key);
    cursor_->set_key(cursor_, db_key);
    WT_ITEM db_value;
    db_value.data = value.data();
    db_value.size = value.size();
    cursor_->set_value(cursor_, &db_value);
    int res = cursor_->update(cursor_);
    cursor_->reset(cursor_);
    if (res)
        return {0, operation_status_t::error_k};
    return {1, operation_status_t::ok_k};
}

operation_result_t wiredtiger_t::remove(key_t key) {

    auto db_key = build_key(key);
    cursor_->set_key(cursor_, db_key);
    int res = cursor_->remove(cursor_);
    cursor_->reset(cursor_);
    if (res)
        return {0, operation_status_t::error_k};
    return {1, operation_status_t::ok_k};
}

operation_result_t wiredtiger_t::read(key_t key, value_span_t value) const {

    auto db_key = build_key(key);
    cursor_->set_key(cursor_, db_key);
    error_check(cursor_->search(cursor_));
    WT_ITEM db_value;
    int res = cursor_->get_value(cursor_, &db_value);
    cursor_->reset(cursor_);
    if (res)
        return {0, operation_status_t::not_found_k};

    memcpy(value.data(), db_value.data, db_value.size);
    return {1, operation_status_t::ok_k};
}

operation_result_t wiredtiger_t::batch_read(keys_span_t keys) const {

    // Note: imitation of batch read!
    for (auto const& key : keys) {
        WT_ITEM db_value;
        auto db_key = build_key(key);
        cursor_->set_key(cursor_, db_key);
        error_check(cursor_->search(cursor_));
        int res = cursor_->get_value(cursor_, &db_value);
        if (res == 0) {
            if (db_value.size > value_buffer_.size())
                value_buffer_ = std::vector<char>(db_value.size);
            memcpy(value_buffer_.data(), db_value.data, db_value.size);
        }
        cursor_->reset(cursor_);
    }
    return {keys.size(), operation_status_t::ok_k};
}

operation_result_t wiredtiger_t::range_select(key_t key, size_t length, value_span_t single_value) const {

    auto db_key = build_key(key);
    WT_ITEM db_value;
    int res = 0;
    int i = 0;
    cursor_->set_key(cursor_, db_key);
    error_check(cursor_->search(cursor_));

    size_t selected_records_count = 0;
    while ((res = cursor_->next(cursor_)) == 0 && i++ < length) {
        error_check(cursor_->get_key(cursor_, &db_key));
        error_check(cursor_->get_value(cursor_, &db_value));
        memcpy(single_value.data(), db_value.data, db_value.size);
        ++selected_records_count;
    }
    return {selected_records_count, operation_status_t::ok_k};
}

operation_result_t wiredtiger_t::scan(value_span_t single_value) const {

    const char* db_key = nullptr;
    WT_ITEM db_value;
    int res = 0;
    error_check(session_->open_cursor(session_, table_name_.c_str(), NULL, NULL, &cursor_));

    size_t scanned_records_count = 0;
    while ((res = cursor_->next(cursor_)) == 0) {
        error_check(cursor_->get_key(cursor_, &db_key));
        error_check(cursor_->get_value(cursor_, &db_value));
        memcpy(single_value.data(), db_value.data, db_value.size);
        ++scanned_records_count;
    }
    return {scanned_records_count, operation_status_t::ok_k};
}

char const* wiredtiger_t::build_key(key_t key) {
    char str_format[] = "%d";
    int len = std::snprintf(key_buffer_.data(), key_buffer_.size(), str_format, key);
    return key_buffer_.data();
}

} // namespace mongodb
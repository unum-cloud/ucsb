#pragma once

#include <string>
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
using operation_status_t = ucsb::operation_status_t;
using operation_result_t = ucsb::operation_result_t;

struct wiredtiger_t : public ucsb::db_t {
  public:
    inline wiredtiger_t() : conn_(nullptr), session_(nullptr), cursor_(nullptr), table_name_("table:access") {}
    ~wiredtiger_t() override = default;

    bool init(fs::path const& config_path, fs::path const& dir_path) override;
    void destroy() override;

    operation_result_t insert(key_t key, value_span_t value) override;
    operation_result_t update(key_t key, value_span_t value) override;
    operation_result_t read(key_t key, value_span_t value) const override;
    operation_result_t remove(key_t key) override;
    operation_result_t batch_read(keys_span_t keys) const override;
    operation_result_t range_select(key_t key, size_t length, value_span_t single_value) const override;
    operation_result_t scan(value_span_t single_value) const override;

  private:
    WT_CONNECTION* conn_;
    WT_SESSION* session_;
    WT_CURSOR* cursor_;
    std::string table_name_;
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

operation_result_t wiredtiger_t::insert(key_t key, value_span_t value) {

    std::string str_key = std::to_string(key);
    std::string data(reinterpret_cast<char*>(value.data()), value.size());

    cursor_->set_key(cursor_, str_key.c_str());
    WT_ITEM db_value;
    db_value.data = data.c_str();
    db_value.size = data.size();
    cursor_->set_value(cursor_, &db_value);
    int res = cursor_->insert(cursor_);
    cursor_->reset(cursor_);
    if (res)
        return {0, operation_status_t::error_k};
    return {1, operation_status_t::ok_k};
}

operation_result_t wiredtiger_t::update(key_t key, value_span_t value) {

    std::string str_key = std::to_string(key);
    std::string data(reinterpret_cast<char*>(value.data()), value.size());

    cursor_->set_key(cursor_, str_key.c_str());
    WT_ITEM db_value;
    db_value.data = data.c_str();
    db_value.size = data.size();
    cursor_->set_value(cursor_, &db_value);
    int res = cursor_->update(cursor_);
    cursor_->reset(cursor_);
    if (res)
        return {0, operation_status_t::error_k};
    return {1, operation_status_t::ok_k};
}

operation_result_t wiredtiger_t::read(key_t key, value_span_t value) const {

    std::string str_key = std::to_string(key);
    WT_ITEM db_value;
    cursor_->set_key(cursor_, str_key.c_str());
    error_check(cursor_->search(cursor_));
    int res = cursor_->get_value(cursor_, &db_value);
    cursor_->reset(cursor_);
    if (res)
        return {0, operation_status_t::not_found_k};

    memcpy(value.data(), db_value.data, db_value.size);
    return {1, operation_status_t::ok_k};
}

operation_result_t wiredtiger_t::remove(key_t key) {

    std::string str_key = std::to_string(key);
    cursor_->set_key(cursor_, str_key.c_str());
    int res = cursor_->remove(cursor_);
    cursor_->reset(cursor_);
    if (res)
        return {0, operation_status_t::error_k};
    return {1, operation_status_t::ok_k};
}

operation_result_t wiredtiger_t::batch_read(keys_span_t keys) const {

    // Note: imitation of batch read!
    for (auto const& key : keys) {
        WT_ITEM db_value;
        std::string str_key = std::to_string(key);
        cursor_->set_key(cursor_, str_key.c_str());
        error_check(cursor_->search(cursor_));
        int res = cursor_->get_value(cursor_, &db_value);
        cursor_->reset(cursor_);
    }
    return {keys.size(), operation_status_t::ok_k};
}

operation_result_t wiredtiger_t::range_select(key_t key, size_t length, value_span_t single_value) const {

    std::string str_key = std::to_string(key);
    const char* db_key = nullptr;
    WT_ITEM db_value;
    int res = 0;
    int i = 0;
    cursor_->set_key(cursor_, str_key.c_str());
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

    size_t scaned_records_count = 0;
    while ((res = cursor_->next(cursor_)) == 0) {
        error_check(cursor_->get_key(cursor_, &db_key));
        error_check(cursor_->get_value(cursor_, &db_value));
        memcpy(single_value.data(), db_value.data, db_value.size);
        ++scaned_records_count;
    }
    return {scaned_records_count, operation_status_t::ok_k};
}

} // namespace mongodb
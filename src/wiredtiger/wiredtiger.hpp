#pragma once

#include <string>
#include <vector>

#include <fmt/format.h>
#include <wiredtiger.h>

#include "src/core/types.hpp"
#include "src/core/db.hpp"
#include "src/core/helper.hpp"
#include "src/core/printable.hpp"

namespace ucsb::mongo {

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
using db_hints_t = ucsb::db_hints_t;
using transaction_t = ucsb::transaction_t;

/**
 * @brief WiredTiger wrapper for the UCSB benchmark.
 * WiredTiger is the core key-value engine of MongoDB.
 * https://github.com/wiredtiger/wiredtiger
 */
class wiredtiger_t : public ucsb::db_t {
  public:
    inline wiredtiger_t() : conn_(nullptr), table_name_("table:access"), state_(0), bulk_load_cursor_(nullptr) {}
    ~wiredtiger_t() override = default;

    void set_config(fs::path const& config_path,
                    fs::path const& main_dir_path,
                    std::vector<fs::path> const& storage_dir_paths,
                    db_hints_t const& hints) override;
    bool open(std::string& error) override;
    void close() override;

    std::string info() override;

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

  private:
    struct config_t {
        size_t cache_size = 0;
    };

    inline bool load_config(config_t& config);
    inline std::string create_str_config(config_t const& config) const;

    inline WT_CURSOR* open_cursor(char const* config = nullptr) const;
    inline void close_cursor(WT_CURSOR* cursor) const;

  private:
    fs::path config_path_;
    fs::path main_dir_path_;
    std::vector<fs::path> storage_dir_paths_;
    db_hints_t hints_;

    WT_CONNECTION* conn_;
    std::string table_name_;

    size_t state_;
    std::vector<WT_SESSION*> sessions_;
    mutable std::atomic_size_t free_sessions_count_;

    // Special cursor for bulk load
    WT_CURSOR* bulk_load_cursor_;
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

void wiredtiger_t::set_config(fs::path const& config_path,
                              fs::path const& main_dir_path,
                              std::vector<fs::path> const& storage_dir_paths,
                              db_hints_t const& hints) {
    config_path_ = config_path;
    main_dir_path_ = main_dir_path;
    storage_dir_paths_ = storage_dir_paths;
    hints_ = hints;
}

bool wiredtiger_t::open(std::string& error) {

    if (conn_)
        return true;

    if (!storage_dir_paths_.empty()) {
        error = "Doesn't support multiple disks";
        return false;
    }

    config_t config;
    if (!load_config(config)) {
        error = "Failed to load config";
        return false;
    }

    std::string str_config = create_str_config(config);
    int res = wiredtiger_open(main_dir_path_.c_str(), NULL, str_config.c_str(), &conn_);
    if (res) {
        error = "Failed to open DB";
        return false;
    }

    // Create sessions for threads
    sessions_.reserve(hints_.threads_count);
    for (size_t i = 0; i < hints_.threads_count; ++i) {
        WT_SESSION* session = nullptr;
        auto res = conn_->open_session(conn_, NULL, NULL, &session);
        if (res)
            break;
        sessions_.push_back(session);
        res = session->create(session, table_name_.c_str(), "key_format=Q,value_format=u");
        if (res)
            break;
    }
    if (sessions_.size() != hints_.threads_count) {
        close();
        return false;
    }
    
    ++state_;
    free_sessions_count_.store(sessions_.size());

    return true;
}

void wiredtiger_t::close() {
    if (!conn_)
        return;

    free_sessions_count_.store(0);
    for (auto session : sessions_)
        session->close(session, NULL);
    sessions_.clear();
    conn_->close(conn_, NULL);
    conn_ = nullptr;
}

inline WT_CURSOR* wiredtiger_t::open_cursor(char const* config) const {
    // Resolve session for the caller thread
    thread_local size_t session_idx = 0;
    thread_local size_t session_state = 0;
    if (session_state != state_) [[unlikely]] {
        auto request_idx = --free_sessions_count_;
        if (request_idx >= sessions_.size()) {
            // Revert
            ++free_sessions_count_;
            return nullptr;
        }
        session_idx = request_idx;
        session_state = state_;
    }

    // Note: Cursor is cached by default, so it doesn't open new cursor for the same session
    WT_CURSOR* cursor = nullptr;
    WT_SESSION* session = sessions_[session_idx];
    auto res = session->open_cursor(session, table_name_.c_str(), NULL, config, &cursor);
    if (res)
        return nullptr;
    return cursor;
}

inline void wiredtiger_t::close_cursor(WT_CURSOR* cursor) const { cursor->close(cursor); }

operation_result_t wiredtiger_t::upsert(key_t key, value_spanc_t value) {

    WT_CURSOR* cursor = open_cursor();
    if (!cursor)
        return {0, operation_status_t::error_k};

    cursor->set_key(cursor, key);
    WT_ITEM db_value;
    db_value.data = value.data();
    db_value.size = value.size();
    cursor->set_value(cursor, &db_value);
    auto res = cursor->insert(cursor);
    close_cursor(cursor);

    return {size_t(res == 0), res == 0 ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t wiredtiger_t::update(key_t key, value_spanc_t value) {

    WT_CURSOR* cursor = open_cursor();
    if (!cursor)
        return {0, operation_status_t::error_k};

    cursor->set_key(cursor, key);
    WT_ITEM db_value;
    db_value.data = value.data();
    db_value.size = value.size();
    cursor->set_value(cursor, &db_value);
    auto res = cursor->update(cursor);
    close_cursor(cursor);

    return {size_t(res == 0), res == 0 ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t wiredtiger_t::remove(key_t key) {

    WT_CURSOR* cursor = open_cursor();
    if (!cursor)
        return {0, operation_status_t::error_k};

    cursor->set_key(cursor, key);
    auto res = cursor->remove(cursor);
    close_cursor(cursor);

    bool ok = res == 0 || res == WT_NOTFOUND;
    return {size_t(ok), ok ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t wiredtiger_t::read(key_t key, value_span_t value) const {

    WT_CURSOR* cursor = open_cursor();
    if (!cursor)
        return {0, operation_status_t::error_k};

    cursor->set_key(cursor, key);
    auto res = cursor->search(cursor);
    if (res)
        return {0, operation_status_t::not_found_k};

    WT_ITEM db_value;
    res = cursor->get_value(cursor, &db_value);
    close_cursor(cursor);
    if (res)
        return {0, operation_status_t::not_found_k};

    memcpy(value.data(), db_value.data, db_value.size);

    return {1, operation_status_t::ok_k};
}

operation_result_t wiredtiger_t::batch_upsert(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) {

   WT_CURSOR* cursor = open_cursor();
    if (!cursor)
        return {0, operation_status_t::error_k};

    size_t offset = 0;
    size_t upserted = 0;
    for (size_t idx = 0; idx < keys.size(); ++idx) {
        cursor->set_key(cursor, keys[idx]);
        WT_ITEM db_value;
        db_value.data = values.data() + offset;
        db_value.size = sizes[idx];
        cursor->set_value(cursor, &db_value);
        int res = cursor->insert(cursor);
        if (!res)
            upserted++;
        offset += sizes[idx];
    }
    close_cursor(cursor);

    return {upserted, operation_status_t::ok_k};
}

operation_result_t wiredtiger_t::batch_read(keys_spanc_t keys, values_span_t values) const {

   WT_CURSOR* cursor = open_cursor();
    if (!cursor)
        return {0, operation_status_t::error_k};

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
    }
    close_cursor(cursor);


    return {found_cnt, operation_status_t::ok_k};
}

operation_result_t wiredtiger_t::bulk_load(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) {
    // Warnings:
    //   DB must be empty
    //   This is single thread interface

    if (!bulk_load_cursor_) {
        bulk_load_cursor_ = open_cursor("bulk");
        if (!bulk_load_cursor_)
            return {0, operation_status_t::error_k};
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

    WT_CURSOR* cursor = open_cursor();
    if (!cursor)
        return {0, operation_status_t::error_k};

    cursor->set_key(cursor, key);
    auto res = cursor->search(cursor);
    if (res)
        return {0, operation_status_t::error_k};

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
    close_cursor(cursor);

    return {selected_records_count, operation_status_t::ok_k};
}

operation_result_t wiredtiger_t::scan(key_t key, size_t length, value_span_t single_value) const {

    WT_CURSOR* cursor = open_cursor();
    if (!cursor)
        return {0, operation_status_t::error_k};

    cursor->set_key(cursor, key);
    auto res = cursor->search(cursor);
    if (res)
        return {0, operation_status_t::error_k};

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
    close_cursor(cursor);

    return {scanned_records_count, operation_status_t::ok_k};
}

std::string wiredtiger_t::info() {
    return fmt::format("v{}.{}.{}", WIREDTIGER_VERSION_MAJOR, WIREDTIGER_VERSION_MINOR, WIREDTIGER_VERSION_PATCH);
}

void wiredtiger_t::flush() {
    if (bulk_load_cursor_) {
        close_cursor(bulk_load_cursor_);
        bulk_load_cursor_ = nullptr;
    }
}

size_t wiredtiger_t::size_on_disk() const { return ucsb::size_on_disk(main_dir_path_); }

std::unique_ptr<transaction_t> wiredtiger_t::create_transaction() { return {}; }

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

} // namespace ucsb::mongo
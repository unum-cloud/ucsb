#pragma once

#include <string>
#include <vector>
#include <sys/stat.h>
#include <fmt/format.h>
#include <nlohmann/json.hpp>

#include <lmdb.h>

#include "ucsb/core/types.hpp"
#include "ucsb/core/db.hpp"
#include "ucsb/core/helper.hpp"

namespace symas {

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
using transaction_t = ucsb::transaction_t;

/**
 * @brief LMDB wrapper for the UCSB benchmark.
 * https://github.com/LMDB/lmdb
 */
struct lmdb_t : public ucsb::db_t {
  public:
    inline lmdb_t() : env_(nullptr), dbi_(0) {}
    inline ~lmdb_t() { close(); }

    void set_config(fs::path const& config_path, fs::path const& dir_path) override;
    bool open() override;
    bool close() override;
    void destroy() override;

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

    void flush() override;
    size_t size_on_disk() const override;

    std::unique_ptr<transaction_t> create_transaction() override;

  private:
    struct config_t {
        size_t map_size = 0;
        bool no_sync = false;
        bool no_meta_sync = false;
        bool no_read_a_head = false;
        bool write_map = false;
    };

    bool load_config(config_t& config);

    fs::path config_path_;
    fs::path dir_path_;

    MDB_env* env_;
    MDB_dbi dbi_;
    mutable std::vector<char> value_buffer_;
};

inline static int compare_keys(MDB_val const* left, MDB_val const* right) noexcept {
    key_t left_key = *reinterpret_cast<key_t const*>(left->mv_data);
    key_t right_key = *reinterpret_cast<key_t const*>(right->mv_data);
    return left_key < right_key ? -1 : left_key > right_key;
}

void lmdb_t::set_config(fs::path const& config_path, fs::path const& dir_path) {
    config_path_ = config_path;
    dir_path_ = dir_path;
}

bool lmdb_t::open() {
    if (env_)
        return true;

    config_t config;
    if (!load_config(config))
        return false;

    int env_opt = 0;
    if (config.no_sync)
        env_opt |= MDB_NOSYNC;
    if (config.no_meta_sync)
        env_opt |= MDB_NOMETASYNC;
    if (config.no_read_a_head)
        env_opt |= MDB_NORDAHEAD;
    if (config.write_map)
        env_opt |= MDB_WRITEMAP;

    int res = mdb_env_create(&env_);
    if (res)
        return false;
    if (config.map_size > 0) {
        res = mdb_env_set_mapsize(env_, config.map_size);
        if (res) {
            close();
            return false;
        }
    }

    res = mdb_env_open(env_, dir_path_.c_str(), env_opt, 0664);
    if (res) {
        close();
        return false;
    }

    MDB_txn* txn;
    res = mdb_txn_begin(env_, nullptr, 0, &txn);
    if (res) {
        close();
        return false;
    }
    res = mdb_open(txn, nullptr, 0, &dbi_);
    if (res) {
        close();
        return false;
    }
    res = mdb_txn_commit(txn);
    if (res) {
        close();
        return false;
    }

    return true;
}

bool lmdb_t::close() {
    if (!env_)
        return true;

    if (dbi_)
        mdb_close(env_, dbi_);
    mdb_env_close(env_);
    dbi_ = 0;
    env_ = nullptr;
    return true;
}

void lmdb_t::destroy() {
    if (!env_ || !dbi_) {
        ucsb::remove_dir_contents(dir_path_);
        return;
    }

    MDB_txn* txn = nullptr;
    int res = mdb_txn_begin(env_, nullptr, 0, &txn);
    if (res)
        return;
    mdb_drop(txn, dbi_, 1);
    res = mdb_txn_commit(txn);
    assert(res == 0);

    bool ok = close();
    assert(ok);

    ucsb::remove_dir_contents(dir_path_);
}

operation_result_t lmdb_t::insert(key_t key, value_spanc_t value) {

    MDB_txn* txn = nullptr;
    MDB_val key_slice, val_slice;

    key_slice.mv_data = &key;
    key_slice.mv_size = sizeof(key);

    val_slice.mv_data = const_cast<void*>(reinterpret_cast<void const*>(value.data()));
    val_slice.mv_size = value.size();

    int res = mdb_txn_begin(env_, nullptr, 0, &txn);
    if (res)
        return {0, operation_status_t::error_k};
    // mdb_set_compare(txn, &dbi_, compare_keys);
    res = mdb_put(txn, dbi_, &key_slice, &val_slice, 0);
    if (res) {
        mdb_txn_abort(txn);
        return {0, operation_status_t::error_k};
    }
    res = mdb_txn_commit(txn);
    if (res)
        return {0, operation_status_t::error_k};

    return {1, operation_status_t::ok_k};
}

operation_result_t lmdb_t::update(key_t key, value_spanc_t value) {

    MDB_txn* txn = nullptr;
    MDB_val key_slice, val_slice;

    key_slice.mv_data = &key;
    key_slice.mv_size = sizeof(key);

    int res = mdb_txn_begin(env_, nullptr, MDB_RDONLY, &txn);
    if (res)
        return {0, operation_status_t::error_k};
    // mdb_set_compare(txn, &dbi_, compare_keys);
    res = mdb_get(txn, dbi_, &key_slice, &val_slice);
    if (res) {
        mdb_txn_abort(txn);
        return {1, operation_status_t::not_found_k};
    }

    val_slice.mv_data = const_cast<void*>(reinterpret_cast<void const*>(value.data()));
    val_slice.mv_size = value.size();

    res = mdb_put(txn, dbi_, &key_slice, &val_slice, 0);
    if (res) {
        mdb_txn_abort(txn);
        return {0, operation_status_t::error_k};
    }

    res = mdb_txn_commit(txn);
    if (res)
        return {0, operation_status_t::error_k};

    return {1, operation_status_t::ok_k};
}

operation_result_t lmdb_t::remove(key_t key) {

    MDB_txn* txn = nullptr;
    MDB_val key_slice;

    key_slice.mv_data = &key;
    key_slice.mv_size = sizeof(key);

    int res = mdb_txn_begin(env_, nullptr, 0, &txn);
    if (res)
        return {0, operation_status_t::error_k};
    // mdb_set_compare(txn, &dbi_, compare_keys);
    res = mdb_del(txn, dbi_, &key_slice, nullptr);
    if (res) {
        mdb_txn_abort(txn);
        return {1, operation_status_t::not_found_k};
    }
    res = mdb_txn_commit(txn);
    if (res)
        return {0, operation_status_t::error_k};

    return {1, operation_status_t::ok_k};
}

operation_result_t lmdb_t::read(key_t key, value_span_t value) const {

    MDB_txn* txn = nullptr;
    MDB_val key_slice, val_slice;

    key_slice.mv_data = &key;
    key_slice.mv_size = sizeof(key);

    int res = mdb_txn_begin(env_, nullptr, MDB_RDONLY, &txn);
    if (res)
        return {0, operation_status_t::error_k};
    // mdb_set_compare(txn, &dbi_, compare_keys);
    res = mdb_get(txn, dbi_, &key_slice, &val_slice);
    if (res) {
        mdb_txn_abort(txn);
        return {1, operation_status_t::not_found_k};
    }
    memcpy(value.data(), val_slice.mv_data, val_slice.mv_size);
    mdb_txn_abort(txn);

    return {1, operation_status_t::ok_k};
}

operation_result_t lmdb_t::batch_insert(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) {
    return {0, operation_status_t::not_implemented_k};
}

operation_result_t lmdb_t::batch_read(keys_spanc_t keys) const {

    MDB_txn* txn = nullptr;
    MDB_val key_slice, val_slice;

    int res = mdb_txn_begin(env_, nullptr, MDB_RDONLY, &txn);
    if (res)
        return {0, operation_status_t::error_k};
    // mdb_set_compare(txn, &dbi_, compare_keys);

    // Note: imitation of batch read!
    for (auto key : keys) {
        key_slice.mv_data = &key;
        key_slice.mv_size = sizeof(key);
        res = mdb_get(txn, dbi_, &key_slice, &val_slice);
        if (res == 0) {
            if (val_slice.mv_size > value_buffer_.size())
                value_buffer_ = std::vector<char>(val_slice.mv_size);
            memcpy(value_buffer_.data(), val_slice.mv_data, val_slice.mv_size);
        }
    }

    mdb_txn_abort(txn);
    return {keys.size(), operation_status_t::ok_k};
}

bulk_metadata_t lmdb_t::prepare_bulk_import_data(keys_spanc_t keys,
                                                 values_spanc_t values,
                                                 value_lengths_spanc_t sizes) const {
    // This DB doesn't support bulk import
    (void)keys;
    (void)values;
    (void)sizes;
    return {};
}

operation_result_t lmdb_t::bulk_import(bulk_metadata_t const& metadata) {
    return {0, operation_status_t::not_implemented_k};
}

operation_result_t lmdb_t::range_select(key_t key, size_t length, value_span_t single_value) const {

    MDB_txn* txn = nullptr;
    MDB_cursor* cursor = nullptr;
    MDB_val key_slice, val_slice;

    key_slice.mv_data = &key;
    key_slice.mv_size = sizeof(key);

    int res = mdb_txn_begin(env_, nullptr, 0, &txn);
    if (res)
        return {0, operation_status_t::error_k};
    // mdb_set_compare(txn, &dbi_, compare_keys);
    res = mdb_cursor_open(txn, dbi_, &cursor);
    if (res) {
        mdb_txn_abort(txn);
        return {0, operation_status_t::error_k};
    }
    res = mdb_cursor_get(cursor, &key_slice, &val_slice, MDB_SET);
    if (res) {
        mdb_txn_abort(txn);
        return {1, operation_status_t::not_found_k};
    }

    size_t selected_records_count = 0;
    for (size_t i = 0; res == 0 && i < length; i++) {
        memcpy(single_value.data(), val_slice.mv_data, val_slice.mv_size);
        res = mdb_cursor_get(cursor, &key_slice, &val_slice, MDB_NEXT);
        ++selected_records_count;
    }

    mdb_cursor_close(cursor);
    mdb_txn_abort(txn);
    return {selected_records_count, operation_status_t::ok_k};
}

operation_result_t lmdb_t::scan(value_span_t single_value) const {

    MDB_txn* txn = nullptr;
    MDB_cursor* cursor = nullptr;
    MDB_val key_slice, val_slice;

    int res = mdb_txn_begin(env_, nullptr, 0, &txn);
    if (res)
        return {0, operation_status_t::error_k};
    // mdb_set_compare(txn, &dbi_, compare_keys);
    res = mdb_cursor_open(txn, dbi_, &cursor);
    if (res) {
        mdb_txn_abort(txn);
        return {0, operation_status_t::error_k};
    }
    res = mdb_cursor_get(cursor, &key_slice, &val_slice, MDB_FIRST);
    if (res) {
        mdb_txn_abort(txn);
        return {1, operation_status_t::not_found_k};
    }

    size_t scanned_records_count = 0;
    while (!res) {
        memcpy(single_value.data(), val_slice.mv_data, val_slice.mv_size);
        res = mdb_cursor_get(cursor, &key_slice, &val_slice, MDB_NEXT);
        ++scanned_records_count;
    }

    mdb_cursor_close(cursor);
    mdb_txn_abort(txn);
    return {scanned_records_count, operation_status_t::ok_k};
}

void lmdb_t::flush() {
    // Nothing to do
}

size_t lmdb_t::size_on_disk() const {
    return ucsb::size_on_disk(dir_path_);
}

std::unique_ptr<transaction_t> lmdb_t::create_transaction() {
    return {};
}

bool lmdb_t::load_config(config_t& config) {
    if (!fs::exists(config_path_))
        return false;

    std::ifstream i_config(config_path_);
    nlohmann::json j_config;
    i_config >> j_config;

    config.map_size = j_config.value("map_size", size_t(0));
    config.no_sync = j_config.value("no_sync", true);
    config.no_meta_sync = j_config.value("no_meta_sync", false);
    config.no_read_a_head = j_config.value("no_read_a_head", false);
    config.write_map = j_config.value("write_map", false);

    return true;
}

} // namespace symas
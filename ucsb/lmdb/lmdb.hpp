#pragma once

#include <string>
#include <vector>
#include <sys/stat.h>
#include <fmt/format.h>
#include <nlohmann/json.hpp>

#include <lmdb.h>

#include "ucsb/core/types.hpp"
#include "ucsb/core/db.hpp"

namespace symas {

namespace fs = ucsb::fs;

using key_t = ucsb::key_t;
using keys_span_t = ucsb::keys_span_t;
using value_span_t = ucsb::value_span_t;
using value_spanc_t = ucsb::value_spanc_t;
using operation_status_t = ucsb::operation_status_t;
using operation_result_t = ucsb::operation_result_t;

struct lmdb_t : public ucsb::db_t {
  public:
    inline lmdb_t() : env_(nullptr) {}
    ~lmdb_t() override = default;

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
    struct config_t {
        size_t map_size = 0;
        bool no_sync = false;
        bool no_meta_sync = false;
        bool no_read_a_head = false;
        bool write_map = false;
    };

    bool load_config(fs::path const& config_path, config_t& config);

    MDB_env* env_;
    MDB_dbi dbi_;
    std::vector<std::byte> value_buffer_;
};

bool lmdb_t::init(fs::path const& config_path, fs::path const& dir_path) {

    config_t config;
    if (!load_config(config_path, config))
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

    int ret = mdb_env_create(&env_);
    if (ret)
        return false;
    if (config.map_size >= 0) {
        ret = mdb_env_set_mapsize(env_, config.map_size);
        if (ret)
            return false;
    }

    ret = mdb_env_open(env_, dir_path.c_str(), env_opt, 0664);
    if (ret)
        return false;

    MDB_txn* txn;
    ret = mdb_txn_begin(env_, nullptr, 0, &txn);
    if (ret)
        return false;
    ret = mdb_open(txn, nullptr, 0, &dbi_);
    if (ret)
        return false;
    ret = mdb_txn_commit(txn);
    if (ret)
        return false;

    return true;
}

void lmdb_t::destroy() {
    mdb_close(env_, dbi_);
    mdb_env_close(env_);
    env_ = nullptr;
}

operation_result_t lmdb_t::insert(key_t key, value_spanc_t value) {

    MDB_txn* txn = nullptr;
    MDB_val key_slice, val_slice;

    std::string str_key = std::to_string(key);
    key_slice.mv_data = static_cast<void*>(str_key.data());
    key_slice.mv_size = str_key.size();

    std::string data(reinterpret_cast<char const*>(value.data()), value.size());
    val_slice.mv_data = static_cast<void*>(const_cast<char*>(data.data()));
    val_slice.mv_size = data.size();

    int ret = mdb_txn_begin(env_, nullptr, 0, &txn);
    if (ret)
        return {0, operation_status_t::error_k};
    ret = mdb_put(txn, dbi_, &key_slice, &val_slice, 0);
    if (ret)
        return {0, operation_status_t::error_k};
    ret = mdb_txn_commit(txn);
    if (ret)
        return {0, operation_status_t::error_k};

    return {1, operation_status_t::ok_k};
}

operation_result_t lmdb_t::update(key_t key, value_spanc_t value) {

    MDB_txn* txn = nullptr;
    MDB_val key_slice, val_slice;

    std::string str_key = std::to_string(key);
    key_slice.mv_data = static_cast<void*>(str_key.data());
    key_slice.mv_size = str_key.size();

    int ret = mdb_txn_begin(env_, nullptr, MDB_RDONLY, &txn);
    if (ret)
        return {0, operation_status_t::error_k};
    ret = mdb_get(txn, dbi_, &key_slice, &val_slice);
    if (ret)
        return {0, operation_status_t::error_k};

    std::string data(reinterpret_cast<char const*>(value.data()), value.size());
    val_slice.mv_data = static_cast<void*>(const_cast<char*>(data.data()));
    val_slice.mv_size = data.size();

    ret = mdb_put(txn, dbi_, &key_slice, &val_slice, 0);
    if (ret)
        return {0, operation_status_t::error_k};

    ret = mdb_txn_commit(txn);
    if (ret)
        return {0, operation_status_t::error_k};

    return {1, operation_status_t::ok_k};
}

operation_result_t lmdb_t::remove(key_t key) {

    MDB_txn* txn = nullptr;
    MDB_val key_slice;

    std::string str_key = std::to_string(key);
    key_slice.mv_data = static_cast<void*>(str_key.data());
    key_slice.mv_size = str_key.size();

    int ret = mdb_txn_begin(env_, nullptr, 0, &txn);
    if (ret)
        return {0, operation_status_t::error_k};
    ret = mdb_del(txn, dbi_, &key_slice, nullptr);
    if (ret)
        return {0, operation_status_t::error_k};
    ret = mdb_txn_commit(txn);
    if (ret)
        return {0, operation_status_t::error_k};
    return {1, operation_status_t::ok_k};
}

operation_result_t lmdb_t::read(key_t key, value_span_t value) const {

    MDB_txn* txn = nullptr;
    MDB_val key_slice, val_slice;

    std::string str_key = std::to_string(key);
    key_slice.mv_data = static_cast<void*>(str_key.data());
    key_slice.mv_size = str_key.size();

    int ret = mdb_txn_begin(env_, nullptr, MDB_RDONLY, &txn);
    if (ret)
        return {0, operation_status_t::error_k};
    ret = mdb_get(txn, dbi_, &key_slice, &val_slice);
    if (ret)
        return {0, operation_status_t::error_k};
    memcpy(value.data(), val_slice.mv_data, val_slice.mv_size);
    mdb_txn_abort(txn);

    return {1, operation_status_t::ok_k};
}

operation_result_t lmdb_t::batch_read(keys_span_t keys) const {

    // Note: imitation of batch read!
    for (auto const& key : keys) {
        MDB_txn* txn = nullptr;
        MDB_val key_slice, val_slice;

        std::string str_key = std::to_string(key);
        key_slice.mv_data = static_cast<void*>(str_key.data());
        key_slice.mv_size = str_key.size();

        int ret = mdb_txn_begin(env_, nullptr, MDB_RDONLY, &txn);
        if (ret)
            return {0, operation_status_t::error_k};
        ret = mdb_get(txn, dbi_, &key_slice, &val_slice);
        if (ret)
            return {0, operation_status_t::error_k};

        if (val_slice.mv_size > value_buffer_.size())
            value_buffer_ = std::vector<std::byte>(val_slice.mv_size);
        memcpy(value_buffer_.data(), val_slice.mv_data, val_slice.mv_size);
        mdb_txn_abort(txn);
    }
    return {keys.size(), operation_status_t::ok_k};
}

operation_result_t lmdb_t::range_select(key_t key, size_t length, value_span_t single_value) const {

    MDB_txn* txn = nullptr;
    MDB_cursor* cursor = nullptr;
    MDB_val key_slice, val_slice;

    std::string str_key = std::to_string(key);
    key_slice.mv_data = static_cast<void*>(str_key.data());
    key_slice.mv_size = str_key.size();

    int ret = mdb_txn_begin(env_, nullptr, 0, &txn);
    if (ret)
        return {0, operation_status_t::error_k};
    ret = mdb_cursor_open(txn, dbi_, &cursor);
    if (ret)
        return {0, operation_status_t::error_k};
    ret = mdb_cursor_get(cursor, &key_slice, &val_slice, MDB_SET);
    assert(ret != MDB_NOTFOUND);
    if (ret)
        return {0, operation_status_t::error_k};

    size_t selected_records_count = 0;
    for (int i = 0; !ret && i < length; i++) {
        memcpy(single_value.data(), val_slice.mv_data, val_slice.mv_size);
        ret = mdb_cursor_get(cursor, &key_slice, &val_slice, MDB_NEXT);
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

    int ret = mdb_txn_begin(env_, nullptr, 0, &txn);
    if (ret)
        return {0, operation_status_t::error_k};
    ret = mdb_cursor_open(txn, dbi_, &cursor);
    if (ret)
        return {0, operation_status_t::error_k};
    ret = mdb_cursor_get(cursor, &key_slice, &val_slice, MDB_FIRST);
    assert(ret != MDB_NOTFOUND);
    if (ret)
        return {0, operation_status_t::error_k};

    size_t scanned_records_count = 0;
    while (!ret) {
        memcpy(single_value.data(), val_slice.mv_data, val_slice.mv_size);
        ret = mdb_cursor_get(cursor, &key_slice, &val_slice, MDB_NEXT);
        ++scanned_records_count;
    }

    mdb_cursor_close(cursor);
    mdb_txn_abort(txn);
    return {scanned_records_count, operation_status_t::ok_k};
}

bool lmdb_t::load_config(fs::path const& config_path, config_t& config) {
    if (!fs::exists(config_path.c_str()))
        return false;

    std::ifstream i_config(config_path);
    nlohmann::json j_config;
    i_config >> j_config;

    config.map_size = j_config.value("map_size", -1);
    config.no_sync = j_config.value("no_sync", false);
    config.no_meta_sync = j_config.value("no_meta_sync", false);
    config.no_read_a_head = j_config.value("no_read_a_head", false);
    config.write_map = j_config.value("write_map", false);

    return true;
}

} // namespace symas
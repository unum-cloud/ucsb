#pragma once

#include <string>
#include <vector>
#include <sys/stat.h>

#include <fmt/format.h>
#include <nlohmann/json.hpp>

#include <lmdb.h>

#include "src/core/types.hpp"
#include "src/core/db.hpp"
#include "src/core/helper.hpp"

namespace ucsb::symas {

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
 * @brief LMDB wrapper for the UCSB benchmark.
 * https://github.com/LMDB/lmdb
 */
class lmdb_t : public ucsb::db_t {
  public:
    inline lmdb_t() : env_(nullptr), dbi_(0) {}
    ~lmdb_t() { close(); }

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
        size_t map_size = 0;
        bool no_sync = false;
        bool no_meta_sync = false;
        bool no_read_a_head = false;
        bool write_map = false;
    };

    bool load_config(config_t& config);

    fs::path config_path_;
    fs::path main_dir_path_;
    std::vector<fs::path> storage_dir_paths_;

    MDB_env* env_;
    MDB_dbi dbi_;
};

inline static int compare_keys(MDB_val const* left, MDB_val const* right) noexcept {
    key_t left_key = *reinterpret_cast<key_t const*>(left->mv_data);
    key_t right_key = *reinterpret_cast<key_t const*>(right->mv_data);
    return left_key < right_key ? -1 : left_key > right_key;
}

void lmdb_t::set_config(fs::path const& config_path,
                        fs::path const& main_dir_path,
                        std::vector<fs::path> const& storage_dir_paths,
                        [[maybe_unused]] db_hints_t const& hints) {
    config_path_ = config_path;
    main_dir_path_ = main_dir_path;
    storage_dir_paths_ = storage_dir_paths;
}

bool lmdb_t::open(std::string& error) {
    if (env_)
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
    if (res) {
        error = "Failed to create environment";
        return false;
    }
    if (config.map_size > 0) {
        res = mdb_env_set_mapsize(env_, config.map_size);
        if (res) {
            close();
            error = "Failed to apply config";
            return false;
        }
    }

    res = mdb_env_open(env_, main_dir_path_.c_str(), env_opt, 0664);
    if (res) {
        close();
        error = "Failed to open environment";
        return false;
    }

    MDB_txn* txn;
    res = mdb_txn_begin(env_, nullptr, 0, &txn);
    if (res) {
        close();
        error = "Failed to begin transaction";
        return false;
    }
    res = mdb_open(txn, nullptr, 0, &dbi_);
    if (res) {
        close();
        error = "Failed to open DB";
        return false;
    }
    res = mdb_txn_commit(txn);
    if (res) {
        close();
        error = "Failed to commit transaction";
        return false;
    }

    return true;
}

void lmdb_t::close() {
    if (!env_)
        return;

    if (dbi_)
        mdb_close(env_, dbi_);
    mdb_env_close(env_);
    dbi_ = 0;
    env_ = nullptr;
}

operation_result_t lmdb_t::upsert(key_t key, value_spanc_t value) {

    MDB_txn* txn = nullptr;
    MDB_val key_slice, val_slice;

    key_slice.mv_data = &key;
    key_slice.mv_size = sizeof(key_t);

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
    return {size_t(res == 0), res == 0 ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t lmdb_t::update(key_t key, value_spanc_t value) {

    MDB_txn* txn = nullptr;
    MDB_val key_slice, val_slice;

    key_slice.mv_data = &key;
    key_slice.mv_size = sizeof(key_t);

    int res = mdb_txn_begin(env_, nullptr, MDB_RDONLY, &txn);
    if (res)
        return {0, operation_status_t::error_k};
    // mdb_set_compare(txn, &dbi_, compare_keys);
    res = mdb_get(txn, dbi_, &key_slice, &val_slice);
    if (res) {
        mdb_txn_abort(txn);
        return {0, operation_status_t::not_found_k};
    }

    val_slice.mv_data = const_cast<void*>(reinterpret_cast<void const*>(value.data()));
    val_slice.mv_size = value.size();

    res = mdb_put(txn, dbi_, &key_slice, &val_slice, 0);
    if (res) {
        mdb_txn_abort(txn);
        return {0, operation_status_t::error_k};
    }

    res = mdb_txn_commit(txn);
    return {size_t(res == 0), res == 0 ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t lmdb_t::remove(key_t key) {

    MDB_txn* txn = nullptr;
    MDB_val key_slice;

    key_slice.mv_data = &key;
    key_slice.mv_size = sizeof(key_t);

    int res = mdb_txn_begin(env_, nullptr, 0, &txn);
    if (res)
        return {0, operation_status_t::error_k};
    // mdb_set_compare(txn, &dbi_, compare_keys);
    res = mdb_del(txn, dbi_, &key_slice, nullptr);
    if (res) {
        mdb_txn_abort(txn);
        return {0, operation_status_t::not_found_k};
    }
    res = mdb_txn_commit(txn);
    return {size_t(res == 0), res == 0 ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t lmdb_t::read(key_t key, value_span_t value) const {

    MDB_txn* txn = nullptr;
    MDB_val key_slice, val_slice;

    key_slice.mv_data = &key;
    key_slice.mv_size = sizeof(key_t);

    int res = mdb_txn_begin(env_, nullptr, MDB_RDONLY, &txn);
    if (res)
        return {0, operation_status_t::error_k};
    // mdb_set_compare(txn, &dbi_, compare_keys);
    res = mdb_get(txn, dbi_, &key_slice, &val_slice);
    if (res) {
        mdb_txn_abort(txn);
        return {0, operation_status_t::not_found_k};
    }
    memcpy(value.data(), val_slice.mv_data, val_slice.mv_size);
    mdb_txn_abort(txn);

    return {1, operation_status_t::ok_k};
}

operation_result_t lmdb_t::batch_upsert(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) {

    MDB_txn* txn = nullptr;

    int res = mdb_txn_begin(env_, nullptr, 0, &txn);
    if (res)
        return {0, operation_status_t::error_k};
    // mdb_set_compare(txn, &dbi_, compare_keys);

    size_t offset = 0;
    for (size_t idx = 0; idx < keys.size(); ++idx) {
        MDB_val key_slice, val_slice;
        auto key = keys[idx];
        key_slice.mv_data = &key;
        key_slice.mv_size = sizeof(key_t);
        val_slice.mv_data = const_cast<void*>(reinterpret_cast<void const*>(values.data() + offset));
        val_slice.mv_size = sizes[idx];

        res = mdb_put(txn, dbi_, &key_slice, &val_slice, 0);
        if (res) {
            mdb_txn_abort(txn);
            return {0, operation_status_t::error_k};
        }
        offset += sizes[idx];
    }

    res = mdb_txn_commit(txn);
    return {keys.size(), res == 0 ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t lmdb_t::batch_read(keys_spanc_t keys, values_span_t values) const {

    MDB_txn* txn = nullptr;
    MDB_val key_slice, val_slice;

    int res = mdb_txn_begin(env_, nullptr, MDB_RDONLY, &txn);
    if (res)
        return {0, operation_status_t::error_k};
    // mdb_set_compare(txn, &dbi_, compare_keys);

    // Note: imitation of batch read!
    size_t offset = 0;
    size_t found_cnt = 0;
    for (auto key : keys) {
        key_slice.mv_data = &key;
        key_slice.mv_size = sizeof(key_t);
        res = mdb_get(txn, dbi_, &key_slice, &val_slice);
        if (res == 0) {
            memcpy(values.data() + offset, val_slice.mv_data, val_slice.mv_size);
            offset += val_slice.mv_size;
            ++found_cnt;
        }
    }

    mdb_txn_abort(txn);
    return {found_cnt, operation_status_t::ok_k};
}

operation_result_t lmdb_t::bulk_load(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) {
    // Currently this DB doesn't have bulk upsert so instead we do batch upsert
    return batch_upsert(keys, values, sizes);
}

operation_result_t lmdb_t::range_select(key_t key, size_t length, values_span_t values) const {

    MDB_txn* txn = nullptr;
    MDB_cursor* cursor = nullptr;
    MDB_val key_slice, val_slice;

    key_slice.mv_data = &key;
    key_slice.mv_size = sizeof(key_t);

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
        return {0, operation_status_t::not_found_k};
    }

    size_t offset = 0;
    size_t selected_records_count = 0;
    for (size_t i = 0; res == 0 && i < length; i++) {
        memcpy(values.data() + offset, val_slice.mv_data, val_slice.mv_size);
        offset += val_slice.mv_size;
        res = mdb_cursor_get(cursor, &key_slice, &val_slice, MDB_NEXT);
        ++selected_records_count;
    }

    mdb_cursor_close(cursor);
    mdb_txn_abort(txn);
    return {selected_records_count, operation_status_t::ok_k};
}

operation_result_t lmdb_t::scan(key_t key, size_t length, value_span_t single_value) const {

    MDB_txn* txn = nullptr;
    MDB_cursor* cursor = nullptr;
    MDB_val key_slice, val_slice;

    key_slice.mv_data = &key;
    key_slice.mv_size = sizeof(key_t);

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
        return {0, operation_status_t::not_found_k};
    }

    size_t scanned_records_count = 0;
    for (size_t i = 0; res == 0 && i < length; i++) {
        memcpy(single_value.data(), val_slice.mv_data, val_slice.mv_size);
        res = mdb_cursor_get(cursor, &key_slice, &val_slice, MDB_NEXT);
        ++scanned_records_count;
    }

    mdb_cursor_close(cursor);
    mdb_txn_abort(txn);
    return {scanned_records_count, operation_status_t::ok_k};
}

std::string lmdb_t::info() { return fmt::format("v{}.{}.{}", MDB_VERSION_MAJOR, MDB_VERSION_MINOR, MDB_VERSION_PATCH); }

void lmdb_t::flush() {
    // Nothing to do
}

size_t lmdb_t::size_on_disk() const { return ucsb::size_on_disk(main_dir_path_); }

std::unique_ptr<transaction_t> lmdb_t::create_transaction() { return {}; }

bool lmdb_t::load_config(config_t& config) {
    if (!fs::exists(config_path_))
        return false;

    std::ifstream i_config(config_path_);
    nlohmann::json j_config;
    i_config >> j_config;

    config.map_size = j_config.value<size_t>("map_size", size_t(0));
    config.no_sync = j_config.value<bool>("no_sync", true);
    config.no_meta_sync = j_config.value<bool>("no_meta_sync", false);
    config.no_read_a_head = j_config.value<bool>("no_read_a_head", false);
    config.write_map = j_config.value<bool>("write_map", false);

    return true;
}

} // namespace ucsb::symas
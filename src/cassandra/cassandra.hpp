#pragma once

#include <atomic>
#include <cassert>
#include <cstring>
#include <memory>
#include <string>
#include <vector>
#include <iostream>
#include <chrono>  // To print timing statements

#include <fmt/format.h>
#include <cassandra.h>

#include "src/core/types.hpp"
#include "src/core/db.hpp"
#include "src/core/helper.hpp"
#include "src/core/data_accessor.hpp"
#include "src/cassandra/cassandra_helpers.hpp"
#include "cassandra_transaction.hpp"

namespace ucsb::cassandra {

namespace fs = ucsb::fs;

using key_t = ucsb::key_t;
using keys_spanc_t = ucsb::keys_spanc_t;
using value_span_t = ucsb::value_span_t;
using value_spanc_t = ucsb::value_spanc_t;
using values_span_t = ucsb::values_span_t;
using values_spanc_t = ucsb::values_spanc_t;
using value_lengths_spanc_t = ucsb::value_lengths_spanc_t;
using operation_result_t = ucsb::operation_result_t;
using operation_status_t = ucsb::operation_status_t;
using db_hints_t = ucsb::db_hints_t;
using transaction_t = ucsb::transaction_t;

/**
 * @brief Cassandra wrapper for the UCSB benchmark.
 *
 * Keys and values are stored as TEXT in Cassandra. To mimic the RocksDB behavior and ensure ordering,
 * we serialize and store keys as 8-byte binary values encoded in hex or base64.
 * For simplicity, we store keys as TEXT representing the 64-bit integer key (in big-endian form).
 * Values are stored as raw bytes in a text column (base64-encoding might be needed, but here we assume ASCII or raw).
 *
 * Schema:
 *   CREATE KEYSPACE IF NOT EXISTS ucsb_keyspace
 *       WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
 *   USE ucsb_keyspace;
 *   CREATE TABLE IF NOT EXISTS ucsb_table (key text PRIMARY KEY, value text);
 */

// Helper functions:

//inline std::string key_to_string(key_t key) {
//    // Convert key from little-endian to big-endian to preserve lexical order as numeric order
//    key = __builtin_bswap64(key);
//    // Store as a hex string (16 hex chars)
//    char buffer[17];
//    snprintf(buffer, sizeof(buffer), "%016lx", static_cast<unsigned long>(key));
//    return std::string(buffer);
//}
//
//inline std::string value_to_string(value_spanc_t value) {
//    // Treat value as text. If values are binary, consider using base64.
//    return std::string(reinterpret_cast<const char*>(value.data()), value.size());
//}

// TODO: Add failure prints to all functions

class cassandra_t : public ucsb::db_t {
  public:
    inline cassandra_t()
        : cluster_(nullptr), session_(nullptr) {}
    ~cassandra_t() { close(); }

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
    fs::path config_path_;
    fs::path main_dir_path_;
    std::vector<fs::path> storage_dir_paths_;
    db_hints_t hints_;

    CassCluster* cluster_;
    CassSession* session_;
    std::string keyspace_;
    std::string table_;

    bool connect(std::string& error);
    bool create_schema(std::string& error) const;
    bool execute_simple(std::string const& query) const;
    bool execute_query_with_result(std::string const& query, std::vector<std::pair<std::string, std::string>>& results) const;
    bool execute_single_read(std::string const& key, std::string& value_str) const;
};

void cassandra_t::set_config(fs::path const& config_path,
                             fs::path const& main_dir_path,
                             std::vector<fs::path> const& storage_dir_paths,
                             db_hints_t const& hints) {
    config_path_ = config_path;
    main_dir_path_ = main_dir_path;
    storage_dir_paths_ = storage_dir_paths;
    hints_ = hints;

    keyspace_ = "ucsb_keyspace";
    table_ = "ucsb_table";
}

bool cassandra_t::open(std::string& error) {
    if (session_)
        return true;

    cluster_ = cass_cluster_new();
    session_ = cass_session_new();

    // Set protocol version to v4 (compatible with Cassandra 3.11)
    cass_cluster_set_protocol_version(cluster_, CASS_PROTOCOL_VERSION_V4);

    // Host and port could be read from config, here hardcoded:
    cass_cluster_set_contact_points(cluster_, "127.0.0.1");

    if (!connect(error)) {
        return false;
    }

    if (!create_schema(error)) {
        return false;
    }

    return true;
}

void cassandra_t::close() {
    if (session_) {
        CassFuture* close_future = cass_session_close(session_);
        cass_future_wait(close_future);
        cass_future_free(close_future);
        cass_session_free(session_);
        session_ = nullptr;
    }
    if (cluster_) {
        cass_cluster_free(cluster_);
        cluster_ = nullptr;
    }
}

std::string cassandra_t::info() {
    return "Cassandra C/C++ Driver";
}

bool cassandra_t::connect(std::string& error) {
    CassFuture* connect_future = cass_session_connect(session_, cluster_);
    CassError rc = cass_future_error_code(connect_future);
    if (rc != CASS_OK) {
        const char* msg;
        size_t msg_len;
        cass_future_error_message(connect_future, &msg, &msg_len);
        error.assign(msg, msg_len);
    }
    cass_future_free(connect_future);
    return rc == CASS_OK;
}

bool cassandra_t::create_schema(std::string& error) const {
    std::string create_keyspace = "CREATE KEYSPACE IF NOT EXISTS " + keyspace_ +
                                  " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};";
    if (!execute_simple(create_keyspace)) {
        error = "Failed to create keyspace";
        return false;
    }

    if (!execute_simple("USE " + keyspace_ + ";")) {
        error = "Failed to use keyspace";
        return false;
    }

    std::string create_table = "CREATE TABLE IF NOT EXISTS " + table_ + " (key text PRIMARY KEY, value text);";
    if (!execute_simple(create_table)) {
        error = "Failed to create table";
        return false;
    }

    return true;
}

bool cassandra_t::execute_simple(std::string const& query) const {
    CassStatement* statement = cass_statement_new(query.c_str(), 0);
    CassFuture* future = cass_session_execute(session_, statement);
    cass_statement_free(statement);
    cass_future_wait(future);
    CassError rc = cass_future_error_code(future);
    cass_future_free(future);
    return rc == CASS_OK;
}

bool cassandra_t::execute_single_read(std::string const& key, std::string& value_str) const {
    std::string query = "SELECT value FROM " + table_ + " WHERE key = ?;";
    CassStatement* statement = cass_statement_new(query.c_str(), 1);
    cass_statement_bind_string(statement, 0, key.c_str());

    CassFuture* future = cass_session_execute(session_, statement);
    cass_statement_free(statement);
    cass_future_wait(future);

    CassError rc = cass_future_error_code(future);
    if (rc != CASS_OK) {
        cass_future_free(future);
        return false;
    }

    const CassResult* result = cass_future_get_result(future);
    cass_future_free(future);

    const CassRow* row = cass_result_first_row(result);
    if (!row) {
        cass_result_free(result);
        return false; // Not found
    }

    const CassValue* val = cass_row_get_column_by_name(row, "value");
    const char* val_str;
    size_t val_str_length;
    cass_value_get_string(val, &val_str, &val_str_length);
    value_str.assign(val_str, val_str_length);

    cass_result_free(result);
    return true;
}

operation_result_t cassandra_t::upsert(key_t key, value_spanc_t value) {
    std::string query = "INSERT INTO " + table_ + " (key, value) VALUES (?, ?);";
    CassStatement* statement = cass_statement_new(query.c_str(), 2);

    std::string key_str = key_to_string(key);
    std::string val_str = value_to_string(value);

    cass_statement_bind_string(statement, 0, key_str.c_str());
    cass_statement_bind_string(statement, 1, val_str.c_str());

    CassFuture* future = cass_session_execute(session_, statement);
    cass_statement_free(statement);
    cass_future_wait(future);
    bool ok = (cass_future_error_code(future) == CASS_OK);
    cass_future_free(future);

    return {size_t(ok), ok ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t cassandra_t::update(key_t key, value_spanc_t value) {
    // We must check if the key exists first
    std::string value_str;
    bool found = execute_single_read(key_to_string(key), value_str);
    if (!found)
        return {0, operation_status_t::not_found_k};

    return upsert(key, value);
}

operation_result_t cassandra_t::remove(key_t key) {
    std::string query = "DELETE FROM " + table_ + " WHERE key = ?;";
    CassStatement* statement = cass_statement_new(query.c_str(), 1);

    std::string key_str = key_to_string(key);
    cass_statement_bind_string(statement, 0, key_str.c_str());

    CassFuture* future = cass_session_execute(session_, statement);
    cass_statement_free(statement);
    cass_future_wait(future);
    bool ok = (cass_future_error_code(future) == CASS_OK);
    cass_future_free(future);

    return {size_t(ok), ok ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t cassandra_t::read(key_t key, value_span_t value) const {
    std::string val_str;
    bool found = execute_single_read(key_to_string(key), val_str);
    if (!found) {
        printf("[CASSANDRA] Could not find entry for key = %lu %s\n", key, key_to_string(key).c_str());
        return {0, operation_status_t::not_found_k};
    }
    if (val_str.size() > value.size()) {
        printf("[CASSANDRA] Not enough space for value of key = %lu %s\n", key, key_to_string(key).c_str());
        return {0, operation_status_t::error_k}; // not enough space
    }
    memcpy(value.data(), val_str.data(), val_str.size());
    return {1, operation_status_t::ok_k};
}

operation_result_t cassandra_t::batch_upsert(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) {
  	// Print how many keys in total
//    std::cout << "[CASSANDRA] batch_upsert: " << keys.size() << " keys to insert.\n";

    // Use a logged batch
    CassBatch* batch = cass_batch_new(CASS_BATCH_TYPE_LOGGED);
    size_t offset = 0;
    auto start = std::chrono::steady_clock::now();
    for (size_t i = 0; i < keys.size(); ++i) {
//        std::cout << "[CASSANDRA] Inserting key #" << i << " with value size = " << sizes[i] << " bytes\n";
        std::string query = "INSERT INTO " + table_ + " (key, value) VALUES (?, ?);";
        CassStatement* statement = cass_statement_new(query.c_str(), 2);

        std::string key_str = key_to_string(keys[i]);
//        std::cout << "key_str = " << key_str << " , keys[i] = " << keys[i] << std::endl;
//        printf("C-style padded hex: %lx\n", keys[i]);
        std::string val_str(reinterpret_cast<const char*>(values.data() + offset), sizes[i]);
//        std::cout << "key: " << key_str << " value: " << val_str << std::endl;
        offset += sizes[i];

        cass_statement_bind_string(statement, 0, key_str.c_str());
        cass_statement_bind_string(statement, 1, val_str.c_str());

        cass_batch_add_statement(batch, statement);
        cass_statement_free(statement);
    }
    auto end = std::chrono::steady_clock::now();
    std::chrono::duration<double> elapsed_seconds = end - start;
//    std::cout << "Elapsed time (for loop): " << elapsed_seconds.count() << " seconds\n";

    auto execute_start = std::chrono::steady_clock::now();
    CassFuture* future = cass_session_execute_batch(session_, batch);
    cass_batch_free(batch);
    cass_future_wait(future);

    CassError rc = cass_future_error_code(future);
	if (rc != CASS_OK) {
    	// Retrieve the Cassandra driver’s error message
    	const char* msg = nullptr;
    	size_t msg_len = 0;
    	cass_future_error_message(future, &msg, &msg_len);

    	std::cerr << "[CASSANDRA] Error code: " << rc
              << ", message: " << std::string(msg, msg_len) << std::endl;
	}

    bool ok = (rc == CASS_OK);
//    printf("ok = %d\n", ok);
    cass_future_free(future);
    auto execute_end = std::chrono::steady_clock::now();
    std::chrono::duration<double> execute_elapsed_seconds = execute_end - execute_start;
//    std::cout << "Elapsed time (execution): " << execute_elapsed_seconds.count() << " seconds\n";

//    bool ok = true;
    return {ok ? keys.size() : 0, ok ? operation_status_t::ok_k : operation_status_t::error_k};
}

operation_result_t cassandra_t::batch_read(keys_spanc_t keys, values_span_t values) const {
    // Cassandra doesn't have a MultiGet equivalent. We'll just do multiple reads sequentially.
    size_t found_cnt = 0;
    size_t offset = 0;
    for (auto k : keys) {
        std::string val_str;
        bool found = execute_single_read(key_to_string(k), val_str);
        if (!found)
            continue;
        if (offset + val_str.size() > values.size())
            return {found_cnt, operation_status_t::error_k};
        memcpy(values.data() + offset, val_str.data(), val_str.size());
        offset += val_str.size();
        ++found_cnt;
    }
    return {found_cnt, operation_status_t::ok_k};
}

operation_result_t cassandra_t::bulk_load(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) {
    // Cassandra doesn't have an external bulk load like sst files. Just do a batch insert.
//    printf("[CASSANDRA] bulk_load\n");
    return batch_upsert(keys, values, sizes);
}

operation_result_t cassandra_t::range_select(key_t key, size_t length, values_span_t values) const {
    // Naive scan using ALLOW FILTERING (inefficient)
    std::string key_str = key_to_string(key);
    std::string query = "SELECT value FROM " + table_ + " WHERE key >= '" + key_str +
                        "' LIMIT " + std::to_string(length) + " ALLOW FILTERING;";

    std::vector<std::pair<std::string, std::string>> result_pairs;
    if (!execute_query_with_result(query, result_pairs))
        return {0, operation_status_t::error_k};

    size_t count = 0;
    size_t offset = 0;
    for (auto& kv : result_pairs) {
        if (offset + kv.second.size() > values.size())
            break;
        memcpy(values.data() + offset, kv.second.data(), kv.second.size());
        offset += kv.second.size();
        count++;
    }

    return {count, operation_status_t::ok_k};
}

operation_result_t cassandra_t::scan(key_t key, size_t length, value_span_t single_value) const {
    // Similar to range_select, but we copy only the last retrieved value into single_value.
    std::string key_str = key_to_string(key);
    std::string query = "SELECT value FROM " + table_ + " WHERE key >= '" + key_str +
                        "' LIMIT " + std::to_string(length) + " ALLOW FILTERING;";

    std::vector<std::pair<std::string, std::string>> result_pairs;
    if (!execute_query_with_result(query, result_pairs))
        return {0, operation_status_t::error_k};

    size_t i = 0;
    for (auto& kv : result_pairs) {
        if (kv.second.size() <= single_value.size()) {
            memcpy(single_value.data(), kv.second.data(), kv.second.size());
        }
        i++;
        if (i == length)
            break;
    }

    return {i, operation_status_t::ok_k};
}

void cassandra_t::flush() {
    // Cassandra is always "flushed" after each statement execution.
    // Nothing to do here.
}

size_t cassandra_t::size_on_disk() const {
    // Cassandra’s data size is not trivial to compute from the client.
    // We can attempt to measure the size of data directories if known.
    // For simplicity, return 0 or measure main_dir_path_ if it points to Cassandra data directory.
    return ucsb::size_on_disk(main_dir_path_);
}

std::unique_ptr<transaction_t> cassandra_t::create_transaction() {
    // Create a Cassandra transaction (just a logged batch that will be committed at the end)
    return std::make_unique<cassandra_transaction_t>(session_, keyspace_, table_);
}

bool cassandra_t::execute_query_with_result(std::string const& query, std::vector<std::pair<std::string, std::string>>& results) const {
    CassStatement* statement = cass_statement_new(query.c_str(), 0);
    CassFuture* future = cass_session_execute(session_, statement);
    cass_statement_free(statement);
    cass_future_wait(future);

    CassError rc = cass_future_error_code(future);
    if (rc != CASS_OK) {
        cass_future_free(future);
        return false;
    }

    const CassResult* result = cass_future_get_result(future);
    cass_future_free(future);

    CassIterator* it = cass_iterator_from_result(result);
    while (cass_iterator_next(it)) {
        const CassRow* row = cass_iterator_get_row(it);
        const CassValue* val_val = cass_row_get_column_by_name(row, "value");
        const char* val_str;
        size_t val_len;
        cass_value_get_string(val_val, &val_str, &val_len);
        results.emplace_back("", std::string(val_str, val_len));
    }

    cass_iterator_free(it);
    cass_result_free(result);
    return true;
}

} // namespace ucsb::cassandra
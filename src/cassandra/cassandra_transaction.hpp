#pragma once

#include <memory>
#include <vector>
#include <string>

#include <cassandra.h>

#include "src/core/types.hpp"
#include "src/core/data_accessor.hpp"
#include "src/core/db.hpp"

namespace ucsb::cassandra {

using key_t = ucsb::key_t;
using keys_spanc_t = ucsb::keys_spanc_t;
using value_span_t = ucsb::value_span_t;
using value_spanc_t = ucsb::value_spanc_t;
using values_span_t = ucsb::values_span_t;
using values_spanc_t = ucsb::values_spanc_t;
using value_lengths_spanc_t = ucsb::value_lengths_spanc_t;
using operation_result_t = ucsb::operation_result_t;
using operation_status_t = ucsb::operation_status_t;

inline std::string key_to_string(key_t key) {
    key = __builtin_bswap64(key);
    char buffer[17];
    snprintf(buffer, sizeof(buffer), "%016lx", static_cast<unsigned long>(key));
    return std::string(buffer);
}

inline std::string value_to_string(value_spanc_t value) {
    return std::string(reinterpret_cast<const char*>(value.data()), value.size());
}

/**
 * @brief Cassandra "transaction" implemented using a logged batch.
 * Cassandra doesn't support transactions like RocksDB. Here we emulate
 * it by batching all writes in a single logged batch and executing at
 * destruction. Reads are performed immediately using the session.
 */
class cassandra_transaction_t : public ucsb::transaction_t {
  public:
    inline cassandra_transaction_t(CassSession* session, std::string const& keyspace, std::string const& table)
        : session_(session), keyspace_(keyspace), table_(table), batch_(cass_batch_new(CASS_BATCH_TYPE_LOGGED)) {}
    ~cassandra_transaction_t();

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
    CassSession* session_;
    std::string keyspace_;
    std::string table_;
    CassBatch* batch_;

    bool execute_single_read(std::string const& key, std::string& value_str) const;
    void commit() const;
};

cassandra_transaction_t::~cassandra_transaction_t() {
    commit();
    cass_batch_free(batch_);
}

void cassandra_transaction_t::commit() const {
    CassFuture* future = cass_session_execute_batch(session_, batch_);
    cass_future_wait(future);
    // We don't handle errors here strictly since we assert successful commit in RocksDB
    cass_future_free(future);
}

bool cassandra_transaction_t::execute_single_read(std::string const& key, std::string& value_str) const {
    // Reads happen outside the batch (Cassandra doesn't allow selects in a batch)
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

operation_result_t cassandra_transaction_t::upsert(key_t key, value_spanc_t value) {
    std::string query = "INSERT INTO " + table_ + " (key, value) VALUES (?, ?);";
    CassStatement* statement = cass_statement_new(query.c_str(), 2);
    std::string key_str = key_to_string(key);
    std::string val_str = value_to_string(value);

    cass_statement_bind_string(statement, 0, key_str.c_str());
    cass_statement_bind_string(statement, 1, val_str.c_str());

    cass_batch_add_statement(batch_, statement);
    cass_statement_free(statement);
    return {1, operation_status_t::ok_k};
}

operation_result_t cassandra_transaction_t::update(key_t key, value_spanc_t value) {
    std::string temp_val;
    bool found = execute_single_read(key_to_string(key), temp_val);
    if (!found)
        return {0, operation_status_t::not_found_k};
    return upsert(key, value);
}

operation_result_t cassandra_transaction_t::remove(key_t key) {
    std::string query = "DELETE FROM " + table_ + " WHERE key = ?;";
    CassStatement* statement = cass_statement_new(query.c_str(), 1);
    std::string key_str = key_to_string(key);
    cass_statement_bind_string(statement, 0, key_str.c_str());

    cass_batch_add_statement(batch_, statement);
    cass_statement_free(statement);
    return {1, operation_status_t::ok_k};
}

operation_result_t cassandra_transaction_t::read(key_t key, value_span_t value) const {
    std::string val_str;
    bool found = execute_single_read(key_to_string(key), val_str);
    if (!found)
        return {0, operation_status_t::not_found_k};
    if (val_str.size() > value.size())
        return {0, operation_status_t::error_k};
    memcpy(value.data(), val_str.data(), val_str.size());
    return {1, operation_status_t::ok_k};
}

operation_result_t cassandra_transaction_t::batch_upsert(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) {
    size_t offset = 0;
    for (size_t i = 0; i < keys.size(); ++i) {
        std::string query = "INSERT INTO " + table_ + " (key, value) VALUES (?, ?);";
        CassStatement* statement = cass_statement_new(query.c_str(), 2);

        std::string key_str = key_to_string(keys[i]);
        std::string val_str(reinterpret_cast<const char*>(values.data() + offset), sizes[i]);
        offset += sizes[i];

        cass_statement_bind_string(statement, 0, key_str.c_str());
        cass_statement_bind_string(statement, 1, val_str.c_str());

        cass_batch_add_statement(batch_, statement);
        cass_statement_free(statement);
    }
    return {keys.size(), operation_status_t::ok_k};
}

operation_result_t cassandra_transaction_t::batch_read(keys_spanc_t keys, values_span_t values) const {
    // Reads can’t be batched with writes in Cassandra transactions.
    // Perform them one-by-one.
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

operation_result_t cassandra_transaction_t::bulk_load(keys_spanc_t keys, values_spanc_t values, value_lengths_spanc_t sizes) {
    return batch_upsert(keys, values, sizes);
}

operation_result_t cassandra_transaction_t::range_select(key_t key, size_t length, values_span_t values) const {
    // Similar to the main DB, do an ALLOW FILTERING select
    std::string key_str = key_to_string(key);
    std::string query = "SELECT value FROM " + table_ + " WHERE key >= '" + key_str +
                        "' LIMIT " + std::to_string(length) + " ALLOW FILTERING;";

    // We'll run this read outside of batch
    CassStatement* statement = cass_statement_new(query.c_str(), 0);
    CassFuture* future = cass_session_execute(session_, statement);
    cass_statement_free(statement);
    cass_future_wait(future);
    CassError rc = cass_future_error_code(future);
    if (rc != CASS_OK) {
        cass_future_free(future);
        return {0, operation_status_t::error_k};
    }

    const CassResult* result = cass_future_get_result(future);
    cass_future_free(future);

    CassIterator* it = cass_iterator_from_result(result);
    size_t count = 0;
    size_t offset = 0;
    while (cass_iterator_next(it) && count < length) {
        const CassRow* row = cass_iterator_get_row(it);
        const CassValue* val_val = cass_row_get_column_by_name(row, "value");
        const char* val_str;
        size_t val_len;
        cass_value_get_string(val_val, &val_str, &val_len);
        if (offset + val_len > values.size()) {
            break;
        }
        memcpy(values.data() + offset, val_str, val_len);
        offset += val_len;
        count++;
    }
    cass_iterator_free(it);
    cass_result_free(result);
    return {count, operation_status_t::ok_k};
}

operation_result_t cassandra_transaction_t::scan(key_t key, size_t length, value_span_t single_value) const {
    // Similar to range_select, but store only the last value
    std::string key_str = key_to_string(key);
    std::string query = "SELECT value FROM " + table_ + " WHERE key >= '" + key_str +
                        "' LIMIT " + std::to_string(length) + " ALLOW FILTERING;";

    CassStatement* statement = cass_statement_new(query.c_str(), 0);
    CassFuture* future = cass_session_execute(session_, statement);
    cass_statement_free(statement);
    cass_future_wait(future);
    CassError rc = cass_future_error_code(future);
    if (rc != CASS_OK) {
        cass_future_free(future);
        return {0, operation_status_t::error_k};
    }

    const CassResult* result = cass_future_get_result(future);
    cass_future_free(future);

    CassIterator* it = cass_iterator_from_result(result);
    size_t i = 0;
    while (cass_iterator_next(it) && i < length) {
        const CassRow* row = cass_iterator_get_row(it);
        const CassValue* val_val = cass_row_get_column_by_name(row, "value");
        const char* val_str;
        size_t val_len;
        cass_value_get_string(val_val, &val_str, &val_len);
        if (val_len <= single_value.size()) {
            memcpy(single_value.data(), val_str, val_len);
        }
        i++;
    }

    cass_iterator_free(it);
    cass_result_free(result);
    return {i, operation_status_t::ok_k};
}

} // namespace ucsb::cassandra
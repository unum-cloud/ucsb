#include <cassandra.h>
#include <iostream>
#include <string>

int main() {
    // Configure and connect to the cluster
    CassCluster* cluster = cass_cluster_new();
    cass_cluster_set_contact_points(cluster, "127.0.0.1");

    CassSession* session = cass_session_new();

    // Set protocol version to v4 (compatible with Cassandra 3.11)
    cass_cluster_set_protocol_version(cluster, CASS_PROTOCOL_VERSION_V4);

    CassFuture* connect_future = cass_session_connect(session, cluster);

    // Check connection
    if (cass_future_error_code(connect_future) != CASS_OK) {
        const char* message;
        size_t message_length;
        cass_future_error_message(connect_future, &message, &message_length);
        std::cerr << "Unable to connect to Cassandra: " << std::string(message, message_length) << "\n";
        cass_future_free(connect_future);
        cass_session_free(session);
        cass_cluster_free(cluster);
        return 1;
    }
    cass_future_free(connect_future);
    std::cout << "Connected to Cassandra successfully!\n";

    // Create keyspace
    const char* create_keyspace =
        "CREATE KEYSPACE IF NOT EXISTS test_ks WITH replication = "
        "{'class': 'SimpleStrategy', 'replication_factor': '1'};";
    CassFuture* future = cass_session_execute(session, cass_statement_new(create_keyspace, 0));
    if (cass_future_error_code(future) != CASS_OK) {
        std::cerr << "Failed to create keyspace.\n";
        cass_future_free(future);
        cass_session_free(session);
        cass_cluster_free(cluster);
        return 1;
    }
    cass_future_free(future);

    // Use keyspace
    future = cass_session_execute(session, cass_statement_new("USE test_ks;", 0));
    if (cass_future_error_code(future) != CASS_OK) {
        std::cerr << "Failed to use keyspace.\n";
        cass_future_free(future);
        cass_session_free(session);
        cass_cluster_free(cluster);
        return 1;
    }
    cass_future_free(future);

    // Create table
    const char* create_table =
        "CREATE TABLE IF NOT EXISTS test_table ("
        " key text PRIMARY KEY,"
        " value text"
        ");";
    future = cass_session_execute(session, cass_statement_new(create_table, 0));
    if (cass_future_error_code(future) != CASS_OK) {
        std::cerr << "Failed to create table.\n";
        cass_future_free(future);
        cass_session_free(session);
        cass_cluster_free(cluster);
        return 1;
    }
    cass_future_free(future);

    std::cout << "Keyspace and table created or already exist.\n";

    // Insert a row
    std::string insert_query = "INSERT INTO test_table (key, value) VALUES (?, ?);";
    {
        CassStatement* stmt = cass_statement_new(insert_query.c_str(), 2);
        cass_statement_bind_string(stmt, 0, "test_key");
        cass_statement_bind_string(stmt, 1, "test_value");
        future = cass_session_execute(session, stmt);
        cass_statement_free(stmt);

        if (cass_future_error_code(future) != CASS_OK) {
            std::cerr << "Failed to insert a row.\n";
            cass_future_free(future);
            cass_session_free(session);
            cass_cluster_free(cluster);
            return 1;
        }
        cass_future_free(future);
    }

    // Select the inserted row
    std::string select_query = "SELECT value FROM test_table WHERE key = ?;";
    {
        CassStatement* stmt = cass_statement_new(select_query.c_str(), 1);
        cass_statement_bind_string(stmt, 0, "test_key");

        future = cass_session_execute(session, stmt);
        cass_statement_free(stmt);

        if (cass_future_error_code(future) == CASS_OK) {
            const CassResult* result = cass_future_get_result(future);
            const CassRow* row = cass_result_first_row(result);

            if (row) {
                const CassValue* val = cass_row_get_column_by_name(row, "value");
                const char* val_str;
                size_t val_str_length;
                cass_value_get_string(val, &val_str, &val_str_length);

                std::cout << "Row fetched: key = test_key, value = "
                          << std::string(val_str, val_str_length) << "\n";
            } else {
                std::cerr << "No row found.\n";
            }
            cass_result_free(result);
        } else {
            std::cerr << "Failed to select from table.\n";
        }

        cass_future_free(future);
    }

    // Cleanup
    CassFuture* close_future = cass_session_close(session);
    cass_future_wait(close_future);
    cass_future_free(close_future);
    cass_session_free(session);
    cass_cluster_free(cluster);

    return 0;
}

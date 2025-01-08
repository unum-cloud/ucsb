#include <cassandra.h>
#include <iostream>

int main() {
    CassCluster* cluster = cass_cluster_new();
    CassSession* session = cass_session_new();

    // Set protocol version to v4 (compatible with Cassandra 3.11)
    cass_cluster_set_protocol_version(cluster, CASS_PROTOCOL_VERSION_V4);

    cass_cluster_set_contact_points(cluster, "127.0.0.1");

    CassFuture* connect_future = cass_session_connect(session, cluster);

    if (cass_future_error_code(connect_future) == CASS_OK) {
        std::cout << "Connected to Cassandra!" << std::endl;
    } else {
        const char* message;
        size_t message_length;
        cass_future_error_message(connect_future, &message, &message_length);
        std::cerr << "Error: " << std::string(message, message_length) << std::endl;
    }

    cass_future_free(connect_future);
    cass_cluster_free(cluster);
    cass_session_free(session);
    return 0;
}

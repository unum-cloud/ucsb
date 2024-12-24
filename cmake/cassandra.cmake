# Attempt to find Cassandra driver
find_path(CASSANDRA_INCLUDE_DIR cassandra.h PATHS /usr/include /usr/local/include)
find_library(CASSANDRA_LIBRARY NAMES cassandra PATHS /usr/lib /usr/local/lib)

if (CASSANDRA_INCLUDE_DIR AND CASSANDRA_LIBRARY)
    message(STATUS "Found Cassandra: ${CASSANDRA_LIBRARY}")
    include_directories(${CASSANDRA_INCLUDE_DIR})
    list(APPEND UCSB_DB_LIBS ${CASSANDRA_LIBRARY})
    target_compile_definitions(ucsb_bench PUBLIC UCSB_HAS_CASSANDRA=1)
else()
    message(WARNING "Cassandra not found. Cassandra support disabled.")
endif()
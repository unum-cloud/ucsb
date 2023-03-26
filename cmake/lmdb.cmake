# lmdb:
# https://github.com/LMDB/lmdb/blob/mdb.master/libraries/liblmdb/Makefile

set(UKV_PREFIX_DIR ${CMAKE_BINARY_DIR}/_deps)

include(ExternalProject)

ExternalProject_Add(
    lmdb_external

    GIT_REPOSITORY "https://github.com/LMDB/lmdb.git"
    GIT_TAG LMDB_0.9.29
    GIT_SHALLOW 1
    GIT_PROGRESS 0
    
    PREFIX "${UKV_PREFIX_DIR}"
    DOWNLOAD_DIR "${UKV_PREFIX_DIR}/lmdb-src"
    LOG_DIR "${UKV_PREFIX_DIR}/lmdb-log"
    STAMP_DIR "${UKV_PREFIX_DIR}/lmdb-stamp"
    TMP_DIR "${UKV_PREFIX_DIR}/lmdb-tmp"
    SOURCE_DIR "${UKV_PREFIX_DIR}/lmdb-src"
    INSTALL_DIR "${UKV_PREFIX_DIR}/lmdb-install"
    BINARY_DIR "${UKV_PREFIX_DIR}/lmdb-build"

    CONFIGURE_COMMAND ""
    UPDATE_COMMAND ""
    INSTALL_COMMAND ""
    BUILD_ALWAYS 0

    BUILD_COMMAND make CXXFLAGS="-Wno-implicit-fallthrough" -C "${UKV_PREFIX_DIR}/lmdb-src/libraries/liblmdb"
)

set(lmdb_INCLUDE_DIR ${UKV_PREFIX_DIR}/lmdb-src/libraries/liblmdb)
set(lmdb_LIBRARY_PATH ${UKV_PREFIX_DIR}/lmdb-src/libraries/liblmdb/liblmdb.a)

file(MAKE_DIRECTORY ${lmdb_INCLUDE_DIR})
add_library(lmdb STATIC IMPORTED)

set_property(TARGET lmdb PROPERTY IMPORTED_LOCATION ${lmdb_LIBRARY_PATH})
set_property(TARGET lmdb APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${lmdb_INCLUDE_DIR})

include_directories(${lmdb_INCLUDE_DIR})
add_dependencies(lmdb lmdb_external)
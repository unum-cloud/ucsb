
include(ExternalProject)
find_package(Git REQUIRED)
find_program(MAKE_EXE NAMES gmake nmake make)

ExternalProject_Add(
    lmdb
    PREFIX "_deps/lmdb"
    GIT_REPOSITORY "https://github.com/lmdb/lmdb.git"
    GIT_TAG LMDB_0.9.29
    TIMEOUT 100
    CONFIGURE_COMMAND ""
    BUILD_IN_SOURCE TRUE
    BUILD_COMMAND cd "libraries/liblmdb" && make liblmdb.a
    UPDATE_COMMAND ""
    INSTALL_COMMAND ""
)

ExternalProject_Get_Property(lmdb SOURCE_DIR)
set(liblmdb_INCLUDE_DIR ${SOURCE_DIR}/libraries/liblmdb/)
set(liblmdb_LIBRARY_PATH ${SOURCE_DIR}/libraries/liblmdb/liblmdb.a)
add_library(liblmdb STATIC IMPORTED)

set_property(TARGET liblmdb PROPERTY IMPORTED_LOCATION ${liblmdb_LIBRARY_PATH})
set_property(TARGET liblmdb APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${liblmdb_INCLUDE_DIR})

# Dependencies
add_dependencies(liblmdb lmdb)
include_directories(${liblmdb_INCLUDE_DIR})
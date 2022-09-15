
include(ExternalProject)
find_package(Git REQUIRED)
find_program(MAKE_EXE NAMES gmake nmake make)

ExternalProject_Add(
    liblmdb
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

ExternalProject_Get_Property(liblmdb source_dir)
set(lmdb_INCLUDE_DIR ${source_dir}/libraries/liblmdb/)
set(lmdb_LIBRARY_PATH ${source_dir}/libraries/liblmdb/liblmdb.a)
add_library(lmdb STATIC IMPORTED)

set_property(TARGET lmdb PROPERTY IMPORTED_LOCATION ${lmdb_LIBRARY_PATH})
set_property(TARGET lmdb APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${lmdb_INCLUDE_DIR})

# Dependencies
add_dependencies(lmdb liblmdb)
include_directories(${lmdb_INCLUDE_DIR})
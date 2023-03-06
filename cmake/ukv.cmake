# UKV:
# https://github.com/unum-cloud/ukv/blob/main/CMakeLists.txt

include(ExternalProject)

ExternalProject_Add(
    ukv_external

    GIT_REPOSITORY "https://github.com/unum-cloud/ukv"
    GIT_TAG dev
    GIT_SHALLOW 1
    GIT_PROGRESS 0
    
    PREFIX "_deps"
    DOWNLOAD_DIR "_deps/ukv-src"
    LOG_DIR "_deps/ukv-log"
    STAMP_DIR "_deps/ukv-stamp"
    TMP_DIR "_deps/ukv-tmp"
    SOURCE_DIR "_deps/ukv-src"
    INSTALL_DIR "_deps/ukv-install"
    BINARY_DIR "_deps/ukv-build"

    BUILD_ALWAYS 0
    UPDATE_COMMAND ""

    CMAKE_ARGS
    -DCMAKE_INSTALL_PREFIX:PATH=${CMAKE_BINARY_DIR}/_deps/ukv-install
    -DCMAKE_INSTALL_LIBDIR=lib
    -DCMAKE_INSTALL_RPATH:PATH=<INSTALL_DIR>/lib
    -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
    -DENABLE_STATIC:STRING=ON
    -DENABLE_CPPSUITE:BOOL=OFF
    -DCMAKE_C_FLAGS=-Wno-maybe-uninitialized
)

set(ukv_INCLUDE_DIR ${CMAKE_BINARY_DIR}/_deps/ukv-install/include)
set(ukv_LIBRARY_PATH ${CMAKE_BINARY_DIR}/_deps/ukv-install/lib/libwiredtiger.a)

file(MAKE_DIRECTORY ${ukv_INCLUDE_DIR})
add_library(ukv STATIC IMPORTED)

set_property(TARGET ukv PROPERTY IMPORTED_LOCATION ${ukv_LIBRARY_PATH})
set_property(TARGET ukv APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${ukv_INCLUDE_DIR})

include_directories(${ukv_INCLUDE_DIR})
add_dependencies(ukv ukv_external)
# WiredTiger:
# https://github.com/wiredtiger/wiredtiger/blob/develop/CMakeLists.txt

include(ExternalProject)

ExternalProject_Add(
    wiredtiger_external

    GIT_REPOSITORY "https://github.com/wiredtiger/wiredtiger.git"
    GIT_TAG 11.1.0
    GIT_SHALLOW 1
    GIT_PROGRESS 0
    
    PREFIX "_deps"
    DOWNLOAD_DIR "_deps/wiredtiger-src"
    LOG_DIR "_deps/wiredtiger-log"
    STAMP_DIR "_deps/wiredtiger-stamp"
    TMP_DIR "_deps/wiredtiger-tmp"
    SOURCE_DIR "_deps/wiredtiger-src"
    INSTALL_DIR "_deps/wiredtiger-install"
    BINARY_DIR "_deps/wiredtiger-build"

    BUILD_ALWAYS 0
    UPDATE_COMMAND ""

    CMAKE_ARGS
    -DCMAKE_INSTALL_PREFIX:PATH=${CMAKE_BINARY_DIR}/_deps/wiredtiger-install
    -DCMAKE_INSTALL_LIBDIR=lib
    -DCMAKE_INSTALL_RPATH:PATH=<INSTALL_DIR>/lib
    -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
    -DENABLE_STATIC:STRING=ON
    -DENABLE_CPPSUITE:BOOL=OFF
    -DCMAKE_C_FLAGS=-Wno-maybe-uninitialized
)

set(wiredtiger_INCLUDE_DIR ${CMAKE_BINARY_DIR}/_deps/wiredtiger-install/include)
set(wiredtiger_LIBRARY_PATH ${CMAKE_BINARY_DIR}/_deps/wiredtiger-install/lib/libwiredtiger.a)

file(MAKE_DIRECTORY ${wiredtiger_INCLUDE_DIR})
add_library(wiredtiger STATIC IMPORTED)

set_property(TARGET wiredtiger PROPERTY IMPORTED_LOCATION ${wiredtiger_LIBRARY_PATH})
set_property(TARGET wiredtiger APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${wiredtiger_INCLUDE_DIR})

include_directories(${wiredtiger_INCLUDE_DIR})
add_dependencies(wiredtiger wiredtiger_external)
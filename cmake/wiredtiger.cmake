# WiredTiger:
# https://github.com/wiredtiger/wiredtiger/blob/develop/CMakeLists.txt

set(PREFIX_DIR ${CMAKE_BINARY_DIR}/_deps)

include(ExternalProject)

ExternalProject_Add(
    wiredtiger_external

    GIT_REPOSITORY "https://github.com/wiredtiger/wiredtiger.git"
    GIT_TAG 11.1.0
    GIT_SHALLOW 1
    GIT_PROGRESS 0
    
    PREFIX "${PREFIX_DIR}"
    DOWNLOAD_DIR "${PREFIX_DIR}/wiredtiger-src"
    LOG_DIR "${PREFIX_DIR}/wiredtiger-log"
    STAMP_DIR "${PREFIX_DIR}/wiredtiger-stamp"
    TMP_DIR "${PREFIX_DIR}/wiredtiger-tmp"
    SOURCE_DIR "${PREFIX_DIR}/wiredtiger-src"
    INSTALL_DIR "${PREFIX_DIR}/wiredtiger-install"
    BINARY_DIR "${PREFIX_DIR}/wiredtiger-build"

    BUILD_ALWAYS 0
    UPDATE_COMMAND ""

    CMAKE_ARGS
    -DCMAKE_INSTALL_PREFIX:PATH=${PREFIX_DIR}/wiredtiger-install
    -DCMAKE_INSTALL_LIBDIR=lib
    -DCMAKE_INSTALL_RPATH:PATH=<INSTALL_DIR>/lib
    -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
    -DENABLE_STATIC:STRING=ON
    -DENABLE_CPPSUITE:BOOL=OFF
    -DCMAKE_C_FLAGS=-Wno-maybe-uninitialized
    -DCMAKE_CXX_FLAGS=-Wno-unused-variable
)

set(wiredtiger_INCLUDE_DIR ${PREFIX_DIR}/wiredtiger-install/include)
set(wiredtiger_LIBRARY_PATH ${PREFIX_DIR}/wiredtiger-install/lib/libwiredtiger.a)

file(MAKE_DIRECTORY ${wiredtiger_INCLUDE_DIR})
add_library(wiredtiger STATIC IMPORTED)

set_property(TARGET wiredtiger PROPERTY IMPORTED_LOCATION ${wiredtiger_LIBRARY_PATH})
set_property(TARGET wiredtiger APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${wiredtiger_INCLUDE_DIR})

include_directories(${wiredtiger_INCLUDE_DIR})
add_dependencies(wiredtiger wiredtiger_external)
# uring:
# https://github.com/axboe/liburing/blob/master/Makefile

set(PREFIX_DIR ${CMAKE_BINARY_DIR}/_deps)

include(ExternalProject)

ExternalProject_Add(
    uring_external

    GIT_REPOSITORY "https://github.com/axboe/liburing.git"
    GIT_TAG liburing-2.3
    GIT_SHALLOW 1
    GIT_PROGRESS 0
    
    PREFIX "${PREFIX_DIR}"
    DOWNLOAD_DIR "${PREFIX_DIR}/uring-src"
    LOG_DIR "${PREFIX_DIR}/uring-log"
    STAMP_DIR "${PREFIX_DIR}/uring-stamp"
    TMP_DIR "${PREFIX_DIR}/uring-tmp"
    SOURCE_DIR "${PREFIX_DIR}/uring-src"
    INSTALL_DIR "${PREFIX_DIR}/uring-install"
    BINARY_DIR "${PREFIX_DIR}/uring-build"

    CONFIGURE_COMMAND ""
    UPDATE_COMMAND ""
    INSTALL_COMMAND ""
    BUILD_ALWAYS 0

    BUILD_COMMAND make install prefix=${PREFIX_DIR}/uring-install -C "${PREFIX_DIR}/uring-src"
)

set(uring_INCLUDE_DIR ${PREFIX_DIR}/uring-src/src/include)
set(uring_LIBRARY_PATH ${PREFIX_DIR}/uring-install/lib/liburing.a)

file(MAKE_DIRECTORY ${uring_INCLUDE_DIR})
add_library(uring STATIC IMPORTED)

set_property(TARGET uring PROPERTY IMPORTED_LOCATION ${uring_LIBRARY_PATH})
set_property(TARGET uring APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${uring_INCLUDE_DIR})

include_directories(${uring_INCLUDE_DIR})
add_dependencies(uring uring_external)
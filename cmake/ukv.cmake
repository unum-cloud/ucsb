# UKV:
# https://github.com/unum-cloud/ukv/blob/main/CMakeLists.txt

set(ENGINE_NAME UMEM)
set(ENGINE_UDISK_PATH "${CMAKE_BINARY_DIR}/build/lib/libudisk.a")

list(APPEND UKV_BUILD_ARGS "-DUKV_BUILD_ENGINE_${ENGINE_NAME}=1" "-DUKV_ENGINE_UDISK_PATH=${ENGINE_UDISK_PATH}" "-DUKV_BUILD_BUNDLES=1" "-DUKV_BUILD_TESTS=0" "-DUKV_BUILD_BENCHMARKS=0")
string(TOLOWER ${ENGINE_NAME} ENGINE_LIBNAME)
set(UKV_PREFIX_DIR ${CMAKE_BINARY_DIR}/_deps)

include(ExternalProject)

ExternalProject_Add(
    ukv_external

    GIT_REPOSITORY "https://github.com/unum-cloud/ukv"
    GIT_TAG config
    GIT_SHALLOW 1
    GIT_PROGRESS 0
    
    PREFIX "${UKV_PREFIX_DIR}"
    DOWNLOAD_DIR "${UKV_PREFIX_DIR}/ukv-src"
    LOG_DIR "${UKV_PREFIX_DIR}/ukv-log"
    STAMP_DIR "${UKV_PREFIX_DIR}/ukv-stamp"
    TMP_DIR "${UKV_PREFIX_DIR}/ukv-tmp"
    SOURCE_DIR "${UKV_PREFIX_DIR}/ukv-src"
    INSTALL_DIR "${UKV_PREFIX_DIR}/ukv-install"
    BINARY_DIR "${UKV_PREFIX_DIR}/ukv-build"

    BUILD_ALWAYS 0
    UPDATE_COMMAND ""

    CMAKE_ARGS
    -DCMAKE_INSTALL_PREFIX:PATH=${UKV_PREFIX_DIR}/ukv-install
    -DCMAKE_INSTALL_LIBDIR=lib
    -DCMAKE_INSTALL_RPATH:PATH=<INSTALL_DIR>/lib
    -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
    -DENABLE_STATIC:STRING=ON
    -DENABLE_CPPSUITE:BOOL=OFF
    
    "${UKV_BUILD_ARGS}"
)

list(APPEND ukv_INCLUDE_DIRS ${UKV_PREFIX_DIR}/ukv-src/include ${UKV_PREFIX_DIR}/ukv-src/src)
set(ukv_LIBRARY_PATH ${UKV_PREFIX_DIR}/ukv-build/build/lib/libukv_${ENGINE_LIBNAME}_bundle.a)
file(MAKE_DIRECTORY ${ukv_INCLUDE_DIRS})

add_library(ukv STATIC IMPORTED)
if(${ENGINE_NAME} STREQUAL "UDISK")
    target_link_libraries(ukv INTERFACE dl pthread explain uring numa tbb)
endif()
target_compile_definitions(ukv INTERFACE UKV_ENGINE_IS_${ENGINE_NAME}=1) 

set_property(TARGET ukv PROPERTY IMPORTED_LOCATION ${ukv_LIBRARY_PATH})
set_property(TARGET ukv APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${ukv_INCLUDE_DIRS})

include_directories(${ukv_INCLUDE_DIRS})
add_dependencies(ukv ukv_external)
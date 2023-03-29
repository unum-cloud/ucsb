# UKV:
# https://github.com/unum-cloud/ukv/blob/main/CMakeLists.txt

# Note: This is set in top CMakeLists.txt
option(UKV_ENGINE_NAME "Choose engine")

set(REPOSITORY_BRANCH "main-dev")
list(APPEND BUILD_ARGS "-DUKV_BUILD_BUNDLES=1" "-DUKV_BUILD_TESTS=0" "-DUKV_BUILD_BENCHMARKS=0")

if(UKV_ENGINE_NAME STREQUAL "FLIGHT_CLIENT")
    list(APPEND BUILD_ARGS "-DUKV_BUILD_API_FLIGHT=1")
elseif(UKV_ENGINE_NAME STREQUAL "UDISK")
    set(ENGINE_UDISK_PATH "${CMAKE_BINARY_DIR}/build/lib/libudisk.a")
    list(APPEND BUILD_ARGS "-DUKV_BUILD_ENGINE_${UKV_ENGINE_NAME}=1" "-DUKV_ENGINE_NAME_UDISK_PATH=${ENGINE_UDISK_PATH}")
else()
    list(APPEND BUILD_ARGS "-DUKV_BUILD_ENGINE_${UKV_ENGINE_NAME}=1")
endif()

string(TOLOWER ${UKV_ENGINE_NAME} LOWERCASE_ENGINE_NAME)
set(PREFIX_DIR ${CMAKE_BINARY_DIR}/_deps)

set(VERSION_URL "https://raw.githubusercontent.com/unum-cloud/ukv/${REPOSITORY_BRANCH}/VERSION")
file(DOWNLOAD "${VERSION_URL}" "${PREFIX_DIR}/ukv-src/VERSION")
file(STRINGS "${PREFIX_DIR}/ukv-src/VERSION" UKV_VERSION)

include(ExternalProject)

ExternalProject_Add(
    ukv_external

    GIT_REPOSITORY "https://github.com/unum-cloud/ukv"
    GIT_TAG "${REPOSITORY_BRANCH}"
    GIT_SHALLOW 1
    GIT_PROGRESS 0
    
    PREFIX "${PREFIX_DIR}"
    DOWNLOAD_DIR "${PREFIX_DIR}/ukv-src"
    LOG_DIR "${PREFIX_DIR}/ukv-log"
    STAMP_DIR "${PREFIX_DIR}/ukv-stamp"
    TMP_DIR "${PREFIX_DIR}/ukv-tmp"
    SOURCE_DIR "${PREFIX_DIR}/ukv-src"
    INSTALL_DIR "${PREFIX_DIR}/ukv-install"
    BINARY_DIR "${PREFIX_DIR}/ukv-build"

    BUILD_ALWAYS 0
    UPDATE_COMMAND ""

    CMAKE_ARGS
    -DCMAKE_INSTALL_PREFIX:PATH=${PREFIX_DIR}/ukv-install
    -DCMAKE_INSTALL_LIBDIR=lib
    -DCMAKE_INSTALL_RPATH:PATH=<INSTALL_DIR>/lib
    -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
    -DENABLE_STATIC:STRING=ON
    -DENABLE_CPPSUITE:BOOL=OFF
    -DCMAKE_C_FLAGS=-Wno-maybe-uninitialized -Wno-implicit-fallthrough
    
    "${BUILD_ARGS}"
)

list(APPEND ukv_INCLUDE_DIRS ${PREFIX_DIR}/ukv-src/include ${PREFIX_DIR}/ukv-src/src)
set(ukv_LIBRARY_PATH ${PREFIX_DIR}/ukv-build/build/lib/libukv_${LOWERCASE_ENGINE_NAME}_bundle.a)
file(MAKE_DIRECTORY ${ukv_INCLUDE_DIRS})

add_library(ukv STATIC IMPORTED)
if(UKV_ENGINE_NAME STREQUAL "UDISK")
    target_link_libraries(ukv INTERFACE dl pthread explain uring numa tbb)
endif()

target_compile_definitions(ukv INTERFACE UKV_VERSION="${UKV_VERSION}") 
target_compile_definitions(ukv INTERFACE UKV_ENGINE_NAME_IS_${UKV_ENGINE_NAME}=1) 
target_compile_definitions(ukv INTERFACE UKV_ENGINE_NAME="${LOWERCASE_ENGINE_NAME}") 

set_property(TARGET ukv PROPERTY IMPORTED_LOCATION ${ukv_LIBRARY_PATH})
set_property(TARGET ukv APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${ukv_INCLUDE_DIRS})

include_directories(${ukv_INCLUDE_DIRS})
add_dependencies(ukv ukv_external)
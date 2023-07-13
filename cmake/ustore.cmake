# USTORE:
# https://github.com/unum-cloud/ustore/blob/main/CMakeLists.txt

# Note: This is set in top CMakeLists.txt
option(USTORE_ENGINE_NAME "Choose engine")

set(REPOSITORY_BRANCH "main-dev")
list(APPEND BUILD_ARGS "-DUSTORE_BUILD_BUNDLES=1" "-DUSTORE_BUILD_TESTS=0" "-DUSTORE_BUILD_BENCHMARKS=0" "-DUSTORE_BUILD_SANITIZE=0")

if(USTORE_ENGINE_NAME STREQUAL "FLIGHT_CLIENT")
    list(APPEND BUILD_ARGS "-DUSTORE_BUILD_API_FLIGHT=1")
    
    target_compile_definitions(ucsb_bench PUBLIC USTORE_ENGINE_IS_FLIGHT_CLIENT=1) 
elseif(USTORE_ENGINE_NAME STREQUAL "UDISK")
    set(ENGINE_UDISK_PATH "${CMAKE_BINARY_DIR}/build/lib/libudisk.a")
    list(APPEND BUILD_ARGS "-DUSTORE_BUILD_ENGINE_${USTORE_ENGINE_NAME}=1" "-DUSTORE_ENGINE_NAME_UDISK_PATH=${ENGINE_UDISK_PATH}")
else()
    list(APPEND BUILD_ARGS "-DUSTORE_BUILD_ENGINE_${USTORE_ENGINE_NAME}=1")
endif()

string(TOLOWER ${USTORE_ENGINE_NAME} LOWERCASE_ENGINE_NAME)
set(PREFIX_DIR ${CMAKE_BINARY_DIR}/_deps)

set(VERSION_URL "https://raw.githubusercontent.com/unum-cloud/ustore/${REPOSITORY_BRANCH}/VERSION")
file(DOWNLOAD "${VERSION_URL}" "${PREFIX_DIR}/ustore-src/VERSION")
file(STRINGS "${PREFIX_DIR}/ustore-src/VERSION" USTORE_VERSION)

include(ExternalProject)

ExternalProject_Add(
    ustore_external

    GIT_REPOSITORY "https://github.com/unum-cloud/ukv"
    GIT_TAG "${REPOSITORY_BRANCH}"
    GIT_SHALLOW 1
    GIT_PROGRESS 0
    GIT_SUBMODULES ""
    
    PREFIX "${PREFIX_DIR}"
    DOWNLOAD_DIR "${PREFIX_DIR}/ustore-src"
    LOG_DIR "${PREFIX_DIR}/ustore-log"
    STAMP_DIR "${PREFIX_DIR}/ustore-stamp"
    TMP_DIR "${PREFIX_DIR}/ustore-tmp"
    SOURCE_DIR "${PREFIX_DIR}/ustore-src"
    INSTALL_DIR "${PREFIX_DIR}/ustore-install"
    BINARY_DIR "${PREFIX_DIR}/ustore-build"

    BUILD_ALWAYS 0
    UPDATE_COMMAND ""

    CMAKE_ARGS
    -DCMAKE_INSTALL_PREFIX:PATH=${PREFIX_DIR}/ustore-install
    -DCMAKE_INSTALL_LIBDIR=lib
    -DCMAKE_INSTALL_RPATH:PATH=<INSTALL_DIR>/lib
    -DCMAKE_BUILD_TYPE:STRING=${CMAKE_BUILD_TYPE}
    -DENABLE_STATIC:STRING=ON
    -DENABLE_CPPSUITE:BOOL=OFF
    -DCMAKE_C_FLAGS=-Wno-maybe-uninitialized -Wno-implicit-fallthrough
    
    "${BUILD_ARGS}"
)

list(APPEND ustore_INCLUDE_DIRS ${PREFIX_DIR}/ustore-src/include ${PREFIX_DIR}/ustore-src/src)
set(ustore_LIBRARY_PATH ${PREFIX_DIR}/ustore-build/build/lib/libustore_${LOWERCASE_ENGINE_NAME}_bundle.a)
file(MAKE_DIRECTORY ${ustore_INCLUDE_DIRS})

add_library(ustore_intermediate STATIC IMPORTED)
if(USTORE_ENGINE_NAME STREQUAL "UDISK")
    target_link_libraries(ustore_intermediate INTERFACE dl pthread explain uring numa tbb)
endif()

target_compile_definitions(ustore_intermediate INTERFACE USTORE_VERSION="${USTORE_VERSION}") 
target_compile_definitions(ustore_intermediate INTERFACE USTORE_ENGINE_NAME_IS_${USTORE_ENGINE_NAME}=1) 
target_compile_definitions(ustore_intermediate INTERFACE USTORE_ENGINE_NAME="${LOWERCASE_ENGINE_NAME}") 

set_property(TARGET ustore_intermediate PROPERTY IMPORTED_LOCATION ${ustore_LIBRARY_PATH})
set_property(TARGET ustore_intermediate APPEND PROPERTY INTERFACE_INCLUDE_DIRECTORIES ${ustore_INCLUDE_DIRS})

include_directories(${ustore_INCLUDE_DIRS})
add_dependencies(ustore_intermediate ustore_external)

# Create intermidiat library to hide dependences
add_library(ustore INTERFACE)
target_compile_definitions(ustore INTERFACE ${ustore_DEFINITIONS})
target_include_directories(ustore INTERFACE ${ustore_INCLUDE_DIRS})
target_link_libraries(ustore INTERFACE ustore_intermediate)
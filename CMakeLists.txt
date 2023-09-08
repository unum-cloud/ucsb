#######################################################################################################################
# Set cmake minimum required version
#######################################################################################################################

cmake_minimum_required(VERSION 3.20.1)

#######################################################################################################################
# Enable/Disable Databases
#######################################################################################################################

option(UCSB_BUILD_USTORE "Build USTORE for the benchmark" ON)
option(UCSB_BUILD_ROCKSDB "Build RocksDB for the benchmark" ON)
option(UCSB_BUILD_LEVELDB "Build LevelDB for the benchmark" ON)
option(UCSB_BUILD_WIREDTIGER "Build WiredTiger for the benchmark" ON)
option(UCSB_BUILD_MONGODB "Build MongoDB for the benchmark" OFF)
option(UCSB_BUILD_REDIS "Build Redis for the benchmark" OFF)
option(UCSB_BUILD_LMDB "Build LMDB for the benchmark" OFF)

#######################################################################################################################
# Set compiler
#######################################################################################################################

message(STATUS "CMAKE_C_COMPILER: ${CMAKE_C_COMPILER}")
message(STATUS "CMAKE_C_COMPILER_ID: ${CMAKE_C_COMPILER_ID}")
message(STATUS "CMAKE_CXX_COMPILER: ${CMAKE_CXX_COMPILER}")
message(STATUS "CMAKE_CXX_COMPILER_ID: ${CMAKE_CXX_COMPILER_ID}")

#######################################################################################################################
# Set project environment
#######################################################################################################################

project(UCSB)
include_directories(${PROJECT_SOURCE_DIR} )
link_directories("/usr/local/lib/")

set(CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/")
set(CMAKE_CACHEFILE_DIR "${CMAKE_SOURCE_DIR}/build")
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/build/lib")
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/build/lib")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/build/bin")

message(STATUS "CMAKE_ROOT: ${CMAKE_ROOT}")
message(STATUS "CMAKE_SYSTEM_VERSION: ${CMAKE_SYSTEM_VERSION}")
message(STATUS "CMAKE_BINARY_DIR: ${CMAKE_BINARY_DIR}")
message(STATUS "CMAKE_CURRENT_BINARY_DIR: ${CMAKE_CURRENT_BINARY_DIR}")
message(STATUS "CMAKE_SOURCE_DIR: ${CMAKE_SOURCE_DIR}")
message(STATUS "CMAKE_CXX_COMPILER_ID: ${CMAKE_CXX_COMPILER_ID}")
message(STATUS "CMAKE_SYSTEM_PROCESSOR: ${CMAKE_SYSTEM_PROCESSOR}")
message(STATUS "Python3_SITELIB: ${Python3_SITELIB}")

#######################################################################################################################
# Selecting a modern standard version
#######################################################################################################################

# Export the compilation report, that can be later be reused by static 
# analysis tools and other apps like SourceTrail.
set(CMAKE_EXPORT_COMPILE_COMMANDS OFF)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE RELEASE)
  message(STATUS "Build type not specified: Use RELEASE by default")
endif()

if(CMAKE_BUILD_TYPE EQUAL "RELEASE")
  set(CMAKE_INTERPROCEDURAL_OPTIMIZATION ON)
  set(CMAKE_UNITY_BUILD ON)
endif()

#######################################################################################################################
# Macros and additional flags
#######################################################################################################################

# General flags.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wnon-virtual-dtor -Woverloaded-virtual")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-missing-field-initializers")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-memcmp")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wreorder -Wunused-parameter")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -Wno-unknown-pragmas")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -frtti -fPIC")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wattributes")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wdeprecated-copy")

# Debugging flags.
# https://code.visualstudio.com/docs/cpp/faq-cpp#_how-do-i-enable-debug-symbols
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -DDEBUG_SAFE_MODE -g")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DDEBUG_SAFE_MODE -g")
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -DNDEBUG -O2 -g")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -DNDEBUG -O2 -g")

if(${CMAKE_SYSTEM_PROCESSOR} MATCHES "aarch64")
  message("- Enabling SIMD for ARM")
else()
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ")
endif()

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ")

# Compiler-specific flags.
if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
  set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -fstrict-vtable-pointers")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ferror-limit=1")
  # Riscky Performance Optimization, that require tuning.
  set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -momit-leaf-frame-pointer")
  set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -funsafe-math-optimizations")
  set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -fno-omit-frame-pointer")
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fmax-errors=1")  
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "Intel")
elseif (CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
else ()
  message(STATUS "Unknown CMAKE_CXX_COMPILER_ID: ${CMAKE_CXX_COMPILER_ID}")
endif()

include("${CMAKE_MODULE_PATH}/benchmark.cmake")
include("${CMAKE_MODULE_PATH}/tabulate.cmake")
include("${CMAKE_MODULE_PATH}/fmt.cmake")
include("${CMAKE_MODULE_PATH}/argparse.cmake")
include("${CMAKE_MODULE_PATH}/nlohmannjson.cmake")
include("${CMAKE_MODULE_PATH}/uring.cmake")

set(UCSB_DB_LIBS)
add_executable(ucsb_bench ./src/bench.cxx)

if(${UCSB_BUILD_USTORE})
  # Choose engine. Available engines: UCSET, ROCKSDB, LEVELDB, UDISK
  set(USTORE_ENGINE_NAME UCSET) 

  include("${CMAKE_MODULE_PATH}/ustore.cmake")
  list(APPEND UCSB_DB_LIBS "ustore")
  target_compile_definitions(ucsb_bench PUBLIC UCSB_HAS_USTORE=1) 
endif()

if(${UCSB_BUILD_ROCKSDB})
  include("${CMAKE_MODULE_PATH}/rocksdb.cmake")
  list(APPEND UCSB_DB_LIBS "rocksdb")
  target_compile_definitions(ucsb_bench PUBLIC UCSB_HAS_ROCKSDB=1) 
endif()

if(${UCSB_BUILD_LEVELDB})
  include("${CMAKE_MODULE_PATH}/leveldb.cmake")
  list(APPEND UCSB_DB_LIBS "leveldb")
  target_compile_definitions(ucsb_bench PUBLIC UCSB_HAS_LEVELDB=1) 
endif()

if(${UCSB_BUILD_WIREDTIGER})
  include("${CMAKE_MODULE_PATH}/wiredtiger.cmake")
  list(APPEND UCSB_DB_LIBS "wiredtiger")
  target_compile_definitions(ucsb_bench PUBLIC UCSB_HAS_WIREDTIGER=1) 
endif()

if(${UCSB_BUILD_MONGODB})
  include("${CMAKE_MODULE_PATH}/mongodb.cmake")
  list(APPEND UCSB_DB_LIBS "mongodb")
  target_compile_definitions(ucsb_bench PUBLIC UCSB_HAS_MONGODB=1) 
endif()

if(${UCSB_BUILD_REDIS})
  include("${CMAKE_MODULE_PATH}/redis.cmake")
  list(APPEND UCSB_DB_LIBS "redis")
  target_compile_definitions(ucsb_bench PUBLIC UCSB_HAS_REDIS=1) 
endif()

if(${UCSB_BUILD_LMDB})
  include("${CMAKE_MODULE_PATH}/lmdb.cmake")
  list(APPEND UCSB_DB_LIBS "lmdb")
  target_compile_definitions(ucsb_bench PUBLIC UCSB_HAS_LMDB=1) 
endif()

set(CXX_TARGET_LINK_LIBRARIES z uring benchmark fmt ${UCSB_DB_LIBS})

set(CMAKE_THREAD_LIBS_INIT "-lpthread")
set(CMAKE_HAVE_THREADS_LIBRARY 1)
set(CMAKE_USE_WIN32_THREADS_INIT 0)
set(CMAKE_USE_PTHREADS_INIT 1)
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

#######################################################################################################################
# Build UCSB
#######################################################################################################################

target_link_libraries(ucsb_bench${ARTIFACT_SUFFIX} 
  ${CXX_TARGET_LINK_LIBRARIES}
  ${PKG_PACKAGES_LIBRARIES}
  ${CMAKE_DL_LIBS}
)

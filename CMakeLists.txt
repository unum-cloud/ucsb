#######################################################################################################################
# Set cmake minimum required version
#######################################################################################################################

cmake_minimum_required(VERSION 3.20.1)

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
set(CONAN_DISABLE_CHECK_COMPILER ON)

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
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Winline")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wattributes")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wdeprecated-copy")

# Debugging flags.
# https://code.visualstudio.com/docs/cpp/faq-cpp#_how-do-i-enable-debug-symbols
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -DDEBUG_SAFE_MODE -g")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DDEBUG_SAFE_MODE -g")
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -DNDEBUG -O2")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -DNDEBUG -O2")

# Custom flags.
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -DHAS_FMT -DHAS_GTEST -DHAS_BENCHMARK -DHAS_NLOHMANN_JSON -DHAS_ROCKSDB -DHAS_LEVELDB -DHAS_LMDB")
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -DHAS_FMT -DHAS_GTEST -DHAS_BENCHMARK -DHAS_NLOHMANN_JSON -DHAS_ROCKSDB -DHAS_LEVELDB -DHAS_LMDB")
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DHAS_FMT -DHAS_GTEST -DHAS_BENCHMARK -DHAS_NLOHMANN_JSON -DHAS_ROCKSDB -DHAS_LEVELDB -DHAS_LMDB")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -DHAS_FMT -DHAS_GTEST -DHAS_BENCHMARK -DHAS_NLOHMANN_JSON -DHAS_ROCKSDB -DHAS_LEVELDB -DHAS_LMDB")

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

set(PKG_CONFIG ON)
if (${PKG_CONFIG})
  find_package(PkgConfig REQUIRED)

  pkg_check_modules(PKG_PACKAGES REQUIRED wiredtiger)
  set(PKG_LIBRARIES ${PKG_PACKAGES_LIBRARIES})
endif()

include(${CMAKE_SOURCE_DIR}/cmake/conanbuildinfo.cmake)
conan_basic_setup()

set(CMAKE_THREAD_LIBS_INIT "-lpthread")
set(CMAKE_HAVE_THREADS_LIBRARY 1)
set(CMAKE_USE_WIN32_THREADS_INIT 0)
set(CMAKE_USE_PTHREADS_INIT 1)
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

#######################################################################################################################
# Build tests benchmarks
#######################################################################################################################

add_executable(ucsb_bench ./src/bench.cxx)
target_link_libraries(ucsb_bench${ARTIFACT_SUFFIX} 
  ${PKG_PACKAGES_LIBRARIES}
  ${CONAN_LIBS}
)

# benchmark:
# https://github.com/google/benchmark/blob/main/CMakeLists.txt

include(FetchContent)
FetchContent_Declare(
    benchmark
    GIT_REPOSITORY https://github.com/google/benchmark.git
    GIT_TAG v1.7.0
    GIT_SHALLOW TRUE
)

FetchContent_GetProperties(benchmark)

if(NOT benchmark_POPULATED)
    set(BENCHMARK_ENABLE_TESTING OFF CACHE INTERNAL "")
    set(BENCHMARK_ENABLE_INSTALL OFF CACHE INTERNAL "")
    set(BENCHMARK_ENABLE_DOXYGEN OFF CACHE INTERNAL "")
    set(BENCHMARK_INSTALL_DOCS OFF CACHE INTERNAL "")
    set(BENCHMARK_DOWNLOAD_DEPENDENCIES ON CACHE INTERNAL "")
    set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE INTERNAL "")
    set(BENCHMARK_USE_BUNDLED_GTEST ON CACHE INTERNAL "")

    # Should not be set globally, but just for this target!
    FetchContent_Populate(benchmark)
    add_subdirectory(${benchmark_SOURCE_DIR} ${benchmark_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()

include_directories(${benchmark_SOURCE_DIR}/include)

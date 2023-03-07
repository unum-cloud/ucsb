# LevelDB:
# https://github.com/google/leveldb/blob/main/CMakeLists.txt

include(FetchContent)
FetchContent_Declare(
    leveldb
    GIT_REPOSITORY https://github.com/google/leveldb.git
    GIT_TAG 1.23
    GIT_SHALLOW TRUE
)

FetchContent_GetProperties(leveldb)

if(NOT leveldb_POPULATED)
    # Fetch the content using previously declared details
    set(LEVELDB_BUILD_TESTS OFF CACHE BOOL "Build LevelDB's unit tests")
    set(LEVELDB_BUILD_BENCHMARKS OFF CACHE BOOL "Build LevelDB's benchmarks")
    set(HAVE_SNAPPY OFF CACHE BOOL "Build with snappy compression library")

    # Enable RTTI (Note: LevelDB forcibly disables RTTI in CMakeList.txt:CMAKE_CXX_FLAGS)
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -frtti")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -frtti")

    # Should not be set globally, but just for this target!
    FetchContent_Populate(leveldb)
    add_subdirectory(${leveldb_SOURCE_DIR} ${leveldb_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()

include_directories(${leveldb_SOURCE_DIR}/include)

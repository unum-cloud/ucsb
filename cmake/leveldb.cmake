# LevelDB:
# https://github.com/google/leveldb/blob/main/CMakeLists.txt

include(FetchContent)
FetchContent_Declare(
    leveldb
    GIT_REPOSITORY https://github.com/google/leveldb.git
    GIT_TAG main
    GIT_SHALLOW TRUE
)

FetchContent_GetProperties(leveldb)

if(NOT leveldb_POPULATED)
    set(LEVELDB_BUILD_TESTS OFF CACHE INTERNAL "")
    set(LEVELDB_BUILD_BENCHMARKS OFF CACHE INTERNAL "")
    set(HAVE_SNAPPY OFF CACHE INTERNAL "")

    # Enable RTTI (Note: LevelDB forcibly disables RTTI in CMakeList.txt:CMAKE_CXX_FLAGS)
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -frtti")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -frtti")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-unused-parameter")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-implicit-fallthrough")

    # Should not be set globally, but just for this target!
    FetchContent_Populate(leveldb)
    add_subdirectory(${leveldb_SOURCE_DIR} ${leveldb_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()

include_directories(${leveldb_SOURCE_DIR}/include)

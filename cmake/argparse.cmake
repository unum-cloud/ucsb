# argparse:
# https://github.com/p-ranav/argparse/blob/master/CMakeLists.txt

include(FetchContent)
FetchContent_Declare(
    argparse
    GIT_REPOSITORY https://github.com/p-ranav/argparse.git
    GIT_TAG v2.9
    GIT_SHALLOW TRUE
)

FetchContent_GetProperties(argparse)

if(NOT argparse_POPULATED)
    set(ARGPARSE_BUILD_TESTS OFF CACHE INTERNAL "")

    # Should not be set globally, but just for this target!
    FetchContent_Populate(argparse)
    add_subdirectory(${argparse_SOURCE_DIR} ${argparse_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()

include_directories(${argparse_SOURCE_DIR}/include)

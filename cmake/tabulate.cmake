# tabulate:
# https://github.com/p-ranav/tabulate/blob/master/CMakeLists.txt

include(FetchContent)
FetchContent_Declare(
    tabulate
    GIT_REPOSITORY https://github.com/p-ranav/tabulate.git
    GIT_TAG v1.5
    GIT_SHALLOW TRUE
)

FetchContent_GetProperties(tabulate)

if(NOT tabulate_POPULATED)
    # Should not be set globally, but just for this target!
    FetchContent_Populate(tabulate)
    add_subdirectory(${tabulate_SOURCE_DIR} ${tabulate_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()

include_directories(${tabulate_SOURCE_DIR}/include)

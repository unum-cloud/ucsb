# fmt:
# https://github.com/fmtlib/fmt/blob/master/CMakeLists.txt

include(FetchContent)
FetchContent_Declare(
    fmt
    GIT_REPOSITORY https://github.com/fmtlib/fmt.git
    GIT_TAG 9.1.0
    GIT_SHALLOW TRUE
)

FetchContent_GetProperties(fmt)

if(NOT fmt_POPULATED)
    # Should not be set globally, but just for this target!
    FetchContent_Populate(fmt)
    add_subdirectory(${fmt_SOURCE_DIR} ${fmt_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()

include_directories(${fmt_SOURCE_DIR}/include)

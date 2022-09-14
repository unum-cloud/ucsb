
FetchContent_Declare(
    wiredtiger
    GIT_REPOSITORY https://github.com/wiredtiger/wiredtiger.git
    GIT_TAG 11.0.0
    GIT_SHALLOW TRUE
)

FetchContent_Populate(wiredtiger)

include_directories(${wiredtiger_SOURCE_DIR}/include)

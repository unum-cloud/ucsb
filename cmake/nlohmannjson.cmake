# nlohmannjson:
# https://github.com/nlohmann/json/blob/develop/CMakeLists.txt

include(FetchContent)
FetchContent_Declare(
    nlohmannjson
    GIT_REPOSITORY https://github.com/nlohmann/json.git
    GIT_TAG v3.11.2
    GIT_SHALLOW TRUE
)

FetchContent_GetProperties(nlohmannjson)

if(NOT nlohmannjson_POPULATED)
    # Should not be set globally, but just for this target!
    FetchContent_Populate(nlohmannjson)
    add_subdirectory(${nlohmannjson_SOURCE_DIR} ${nlohmannjson_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()

include_directories(${nlohmannjson_SOURCE_DIR}/include)

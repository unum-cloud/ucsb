# mongodb:
# https://github.com/mongodb/mongo-c-driver/blob/master/CMakeLists.txt
# https://github.com/mongodb/mongo-cxx-driver/blob/master/CMakeLists.txt

include(FetchContent)

# Build mongo-c-driver
FetchContent_Declare(
    mongoc
    GIT_REPOSITORY https://github.com/mongodb/mongo-c-driver.git
    GIT_TAG 1.23.2
    GIT_SHALLOW TRUE
)

FetchContent_GetProperties(mongoc)

if(NOT mongoc_POPULATED)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-implicit-function-declaration")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-deprecated-declarations")

    # Should not be set globally, but just for this target!
    FetchContent_Populate(mongoc)
    add_subdirectory(${mongoc_SOURCE_DIR} ${mongoc_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()


# Build mongo-cxx-driver
FetchContent_Declare(
    mongocxx
    GIT_REPOSITORY https://github.com/mongodb/mongo-cxx-driver.git
    GIT_TAG r3.7.0
    GIT_SHALLOW TRUE
    )
    
    FetchContent_GetProperties(mongocxx)
    
if(NOT mongocxx_POPULATED)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-deprecated")

    # Should not be set globally, but just for this target!
    FetchContent_Populate(mongocxx)
    add_subdirectory(${mongocxx_SOURCE_DIR} ${mongocxx_BINARY_DIR} EXCLUDE_FROM_ALL)
endif()

include_directories(${mongocxx_SOURCE_DIR}/src)
include_directories(${mongocxx_BINARY_DIR}/src)

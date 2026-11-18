cmake_minimum_required(VERSION 3.16)
project(winject_fec LANGUAGES C CXX)

if(CMAKE_SOURCE_DIR STREQUAL PROJECT_SOURCE_DIR)
    set(WINJECT_FEC_TOP_LEVEL ON)
else()
    set(WINJECT_FEC_TOP_LEVEL OFF)
endif()
option(WINJECT_FEC_BUILD_TESTS "Build winject-fec host tests" ${WINJECT_FEC_TOP_LEVEL})

if(WINJECT_FEC_TOP_LEVEL)
    set(CMAKE_CXX_STANDARD 17)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)
endif()

include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/bfc.cmake)
include(${CMAKE_CURRENT_SOURCE_DIR}/cmake/isal_ec.cmake)
winject_fec_add_isal()

add_library(winject_fec STATIC
    src/wire.cpp
    src/rs_common.cpp
    src/rs_encode_scalar.cpp
    src/block_encoder.cpp
    src/block_decoder.cpp
    src/write_stream.cpp
    src/read_stream.cpp
)
add_library(winject::fec ALIAS winject_fec)
set_target_properties(winject_fec PROPERTIES POSITION_INDEPENDENT_CODE ON)
target_compile_features(winject_fec PUBLIC cxx_std_17)
target_include_directories(winject_fec
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${BFC_INCLUDE_DIRS}
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/src
)
find_package(Threads REQUIRED)
target_link_libraries(winject_fec PRIVATE winject_fec_isal PUBLIC Threads::Threads)
target_compile_options(winject_fec PRIVATE -Wall -Wextra -Wpedantic)

if(WINJECT_FEC_BUILD_TESTS)
    enable_testing()
    set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
    include(FetchContent)
    FetchContent_Declare(googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG        v1.14.0
        GIT_SHALLOW    TRUE
    )
    FetchContent_MakeAvailable(googletest)
    add_subdirectory(tests)
endif()

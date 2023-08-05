cmake_minimum_required(VERSION 3.24)
project(gazellemq_client_publisher)

set(CMAKE_CXX_STANDARD 20)

add_executable(${PROJECT_NAME} main.cpp ../client_lib/include/PublisherClient.hpp json/simdjson.hpp json/simdjson.cpp)

find_package(PkgConfig REQUIRED)
pkg_check_modules (JEMALLOC jemalloc)

pkg_search_module(JEMALLOC REQUIRED jemalloc)
include_directories(${JEMALLOC_INCLUDE_DIRS})

target_link_libraries(${PROJECT_NAME} ${JEMALLOC_LIBRARIES} client_lib)

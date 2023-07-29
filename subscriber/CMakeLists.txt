cmake_minimum_required(VERSION 3.24)
project(gazellemq_client_subscriber)

set(CMAKE_CXX_STANDARD 20)

add_executable(${PROJECT_NAME} main.cpp ../client_lib/include/SubscriberClient.hpp)

find_package(PkgConfig REQUIRED)
pkg_check_modules (JEMALLOC jemalloc)

pkg_search_module(JEMALLOC REQUIRED jemalloc)
include_directories(${JEMALLOC_INCLUDE_DIRS})

target_link_libraries(${PROJECT_NAME} ${JEMALLOC_LIBRARIES} client_lib)

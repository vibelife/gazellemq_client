cmake_minimum_required(VERSION 3.24)
project(client_lib)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O3")

add_library(${PROJECT_NAME} include/MPMCQueue/MPMCQueue.hpp include/PublisherClient.hpp)

set_target_properties(${PROJECT_NAME} PROPERTIES LINKER_LANGUAGE CXX)


find_package(PkgConfig REQUIRED)

find_package(Threads)
link_libraries(${Threads})

pkg_check_modules(URING liburing)
find_library(URING uring)
link_libraries(${URING})

find_library(ANL anl)
link_libraries(${ANL})


pkg_check_modules (JEMALLOC jemalloc)

pkg_search_module(JEMALLOC REQUIRED jemalloc)
include_directories(${JEMALLOC_INCLUDE_DIRS})

target_link_libraries(${PROJECT_NAME} ${JEMALLOC_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT} ${URING} ${ANL})

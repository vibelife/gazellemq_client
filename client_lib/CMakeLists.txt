cmake_minimum_required(VERSION 3.24)
project(client_lib)

set(CMAKE_CXX_STANDARD 20)
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

target_link_libraries(${PROJECT_NAME} ${CMAKE_THREAD_LIBS_INIT} ${URING} ${ANL})

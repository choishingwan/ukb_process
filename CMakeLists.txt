cmake_minimum_required(VERSION 2.8)
set(CMAKE_CXX_STANDARD 11)
project(ukb_process)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
add_library(lib_sqlite3
    ${CMAKE_SOURCE_DIR}/lib/sqlite3.c)
include_directories(${CMAKE_SOURCE_DIR}/lib)
add_executable(${PROJECT_NAME} main.cpp)
target_link_libraries( ${PROJECT_NAME} PRIVATE lib_sqlite3 )

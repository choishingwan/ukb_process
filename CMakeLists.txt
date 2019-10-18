cmake_minimum_required(VERSION 2.8)
set(CMAKE_CXX_STANDARD 11)
project(ukb_process)
find_package (Threads)
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
set(EXECUTABLE_OUTPUT_PATH ${CMAKE_SOURCE_DIR}/bin)
add_library(lib_sqlite3
    ${CMAKE_SOURCE_DIR}/lib/sqlite3.c)
add_library(lib_misc
    ${CMAKE_SOURCE_DIR}/misc.cpp)
include_directories(${CMAKE_SOURCE_DIR}/lib)
add_executable(${PROJECT_NAME} main.cpp sql.cpp)
target_link_libraries( ${PROJECT_NAME} PRIVATE lib_sqlite3 )
target_link_libraries( ${PROJECT_NAME} PRIVATE lib_misc)

target_link_libraries (lib_sqlite3 PRIVATE ${CMAKE_THREAD_LIBS_INIT} ${CMAKE_DL_LIBS})
target_link_libraries (${PROJECT_NAME} PRIVATE ${CMAKE_THREAD_LIBS_INIT} ${CMAKE_DL_LIBS})

cmake_minimum_required(VERSION 3.14)

project(project CXX)
set(CMAKE_CXX_STANDARD 17)

set(CALICODB_BuildTests OFF CACHE INTERNAL "")
add_subdirectory(${CALICODB_SOURCE}
        ${CMAKE_CURRENT_BINARY_DIR}/calicodb)

add_executable(project main.cpp)
target_link_libraries(project calicodb)
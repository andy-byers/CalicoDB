cmake_minimum_required(VERSION 3.14)

project(project CXX)
set(CMAKE_CXX_STANDARD 17)

include(FetchContent)
get_filename_component(GIT_REPOSITORY_DIRECTORY
        "../../../.." ABSOLUTE)
FetchContent_Declare(calicodb
        GIT_REPOSITORY "${GIT_REPOSITORY_DIRECTORY}"
        GIT_TAG HEAD)
FetchContent_MakeAvailable(calicodb)

add_executable(project main.cpp)
target_link_libraries(project calicodb)

add_library(calicodb_tools
        tools.cpp
        tools.h
        env_helpers.cpp
        env_helpers.h)
target_link_libraries(calicodb_tools
        PUBLIC calicodb)
target_include_directories(calicodb_tools
        PUBLIC "${PROJECT_SOURCE_DIR}/src"
               .)
target_compile_options(calicodb_tools
        PRIVATE "${CALICODB_WARNINGS}")
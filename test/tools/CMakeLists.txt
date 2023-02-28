
add_library(calicodb_tools
        tools.cpp
        tools.h)
target_link_libraries(calicodb_tools
        PUBLIC calicodb)
target_include_directories(calicodb_tools
        PRIVATE "${PROJECT_SOURCE_DIR}/src"
        PUBLIC .)
target_compile_options(calicodb_tools
        PRIVATE "${CALICODB_WARNINGS}")
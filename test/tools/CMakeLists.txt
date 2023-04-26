
add_library(calicodb_tools
        STATIC tools.cpp
               tools.h
               env_helpers.cpp
               env_helpers.h)
# These options are needed by all executables linking to this helper library.
target_link_libraries(calicodb_tools
        PUBLIC calicodb)
target_include_directories(calicodb_tools
        PUBLIC "${CALICODB_SOURCE_DIR}"
               "${CALICODB_TOOLS_DIR}")
target_compile_options(calicodb_tools
        PUBLIC "${CALICODB_WARNINGS}")
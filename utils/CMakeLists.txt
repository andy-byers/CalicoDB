
add_library(calicodb_utils
        STATIC common.h
               debug.cpp
               fake_env.cpp
               fake_env.h
               model.cpp
               model.h)
# These options are needed by all executables linking to this helper library.
target_link_libraries(calicodb_utils
        PUBLIC calicodb)
target_include_directories(calicodb_utils
        PUBLIC "${PROJECT_SOURCE_DIR}/src"
               "${PROJECT_SOURCE_DIR}/utils")
target_compile_options(calicodb_utils
        PUBLIC ${CALICODB_OPTIONS}
               -Wall
               -Werror)

add_subdirectory(stest)
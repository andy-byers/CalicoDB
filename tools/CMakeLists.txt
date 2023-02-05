
add_library(calico_tools
        tools.cpp
        tools.h)
target_link_libraries(calico_tools
        PUBLIC calico spdlog pthread)
target_include_directories(calico_tools
        PRIVATE "${CALICO_SOURCE_DIR}"
        PUBLIC  .)
target_compile_options(calico_tools
        PRIVATE "${CALICO_WARNINGS}")
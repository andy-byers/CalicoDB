add_test(NAME cmake_custom_string_configure
        COMMAND ${CMAKE_COMMAND}
        -G "${CMAKE_GENERATOR}"
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        -DCALICODB_SOURCE=${PROJECT_SOURCE_DIR}
        "${CMAKE_CURRENT_SOURCE_DIR}/project")
add_test(NAME cmake_custom_string_build
        COMMAND ${CMAKE_COMMAND} --build .)


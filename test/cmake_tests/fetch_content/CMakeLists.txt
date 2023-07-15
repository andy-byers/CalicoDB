add_test(NAME cmake_fetch_content_configure
        COMMAND ${CMAKE_COMMAND}
        -G "${CMAKE_GENERATOR}"
        -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
        "${CMAKE_CURRENT_SOURCE_DIR}/project")
add_test(NAME cmake_fetch_content_build
        COMMAND ${CMAKE_COMMAND} --build .)
cmake_minimum_required(VERSION 3.14)

set(CMAKE_CXX_FLAGS_RELWITHASSERTIONS "-O3"
        CACHE STRING "C++ compiler flags for a RelWithAssertions build" FORCE)
set(CMAKE_C_FLAGS_RELWITHASSERTIONS "-O3"
        CACHE STRING "C compiler flags for a RelWithAssertions build" FORCE)
set(CMAKE_TRY_COMPILE_TARGET_TYPE "STATIC_LIBRARY")

project(calicodb
        LANGUAGES CXX
        VERSION 0.0.1)
set(CMAKE_CXX_STANDARD 17)

set(CALICODB_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/include")

set(MAIN_PROJECT Off)
if (CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_SOURCE_DIR)
    set(MAIN_PROJECT On)
endif()

option(CALICODB_BuildTests "Build the tests" ${MAIN_PROJECT})
option(CALICODB_Install "Install the CMake targets during the install step" ${MAIN_PROJECT})

add_subdirectory(src)

if(CALICODB_BuildTests)
    include(FetchContent)
    include(CTest)
    enable_testing()
    add_subdirectory(test)
endif()

include(GNUInstallDirs)

set(TARGETS_NAME ${PROJECT_NAME}Targets)
set(INSTALL_DIR "${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}")
set(CONFIG_FILE_IN "${PROJECT_SOURCE_DIR}/cmake/${PROJECT_NAME}Config.cmake.in")
set(CONFIG_FILE_OUT "${PROJECT_BINARY_DIR}/cmake/${PROJECT_NAME}Config.cmake")
set(VERSION_FILE "${PROJECT_BINARY_DIR}/cmake/${PROJECT_NAME}ConfigVersion.cmake")

include(CMakePackageConfigHelpers)
configure_package_config_file(
        "${CONFIG_FILE_IN}" "${CONFIG_FILE_OUT}"
        INSTALL_DESTINATION "${INSTALL_DIR}")
write_basic_package_version_file("${VERSION_FILE}"
        COMPATIBILITY SameMajorVersion)

if(CALICODB_Install)
    install(TARGETS calicodb
            EXPORT "${TARGETS_NAME}"
            RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}"
            LIBRARY DESTINATION "${CMAKE_INSTALL_LIBDIR}"
            ARCHIVE DESTINATION "${CMAKE_INSTALL_LIBDIR}")
    install(DIRECTORY "${CALICODB_INCLUDE_DIR}/calicodb"
            DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}")

    install(EXPORT "${TARGETS_NAME}"
            NAMESPACE "${PROJECT_NAME}::"
            DESTINATION "${INSTALL_DIR}")
    install(FILES "${CONFIG_FILE_OUT}" "${VERSION_FILE}"
            DESTINATION "${INSTALL_DIR}")
endif()

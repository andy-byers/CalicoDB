
set(CUB_FILES '')

file(
    GLOB_RECURSE
    CUB_FILES
    "*.cpp"
    "*.h"
)

list(
    APPEND
    CUB_FILES
    ${CUB_INCLUDE_DIR}/cub/bytes.h
    ${CUB_INCLUDE_DIR}/cub/common.h
    ${CUB_INCLUDE_DIR}/cub/cub.h
    ${CUB_INCLUDE_DIR}/cub/cursor.h
    ${CUB_INCLUDE_DIR}/cub/database.h
    ${CUB_INCLUDE_DIR}/cub/exception.h
    ${CUB_INCLUDE_DIR}/cub/lock.h
)

# Build the core static library.
add_library("${CUB_NAME}" STATIC "${CUB_FILES}")
target_include_directories("${CUB_NAME}" PRIVATE "${CUB_SOURCE_DIR}")
target_include_directories("${CUB_NAME}" PRIVATE "${CUB_INCLUDE_DIR}")
target_include_directories("${CUB_NAME}" SYSTEM INTERFACE "${CUB_INCLUDE_DIR}")

# Always compile the library with warnings.
target_compile_options("${CUB_NAME}" PRIVATE "${CUB_WARNINGS}")



set(
    CUB_FILES
        storage/file.cpp
        storage/file.h
        storage/interface.h
        storage/system.cpp
        storage/system.h
    ./utils/slice.h
)

# Build the core static library.
add_library("${CUB_NAME}" STATIC "${CUB_FILES}")
target_include_directories("${CUB_NAME}" PRIVATE "${CUB_SOURCE_DIR}")
target_include_directories("${CUB_NAME}" PUBLIC "${CUB_INCLUDE_DIR}")
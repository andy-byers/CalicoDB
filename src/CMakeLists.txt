
set(
    CUB_FILES

    pool/frame.cpp
    pool/frame.h
    pool/interface.h

    page/cell.cpp
    page/cell.h
    page/link.cpp
    page/link.h
    page/node.cpp
    page/node.h
    page/page.cpp
    page/page.h

    storage/file.cpp
    storage/file.h
    storage/interface.h
    storage/system.cpp
    storage/system.h

    utils/assert.h
    utils/encoding.h
    utils/layout.h
    utils/scratch.cpp
    utils/scratch.h
    utils/types.cpp
    utils/types.h
    utils/slice.h
)

# Build the core static library.
add_library("${CUB_NAME}" STATIC "${CUB_FILES}")
target_include_directories("${CUB_NAME}" PRIVATE "${CUB_SOURCE_DIR}")
target_include_directories("${CUB_NAME}" PUBLIC "${CUB_INCLUDE_DIR}")
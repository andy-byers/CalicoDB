
set(
    CUB_FILES

    pool/frame.cpp
    pool/frame.h
    pool/pager.cpp
    pool/pager.h
    pool/cache.cpp
    pool/cache.h
    pool/buffer_pool.cpp
    pool/buffer_pool.h
    pool/interface.h

    page/cell.cpp
    page/cell.h
    page/link.cpp
    page/link.h
    page/node.cpp
    page/node.h
    page/page.cpp
    page/page.h

    file/file.cpp
    file/file.h
    file/interface.h
    file/system.cpp
    file/system.h

    tree/free_list.cpp
    tree/free_list.h
    tree/interface.h
    tree/rw_lock.h
    tree/tree.cpp
    tree/tree.h

    utils/assert.h
    utils/crc.h
    utils/encoding.h
    utils/layout.h
    utils/scratch.cpp
    utils/scratch.h
    utils/types.cpp
    utils/types.h
    utils/slice.h

    wal/interface.h
    wal/wal_reader.cpp
    wal/wal_reader.h
    wal/wal_record.cpp
    wal/wal_record.h
    wal/wal_writer.cpp
    wal/wal_writer.h
)

# Build the core static library.
add_library("${CUB_NAME}" STATIC "${CUB_FILES}")
target_include_directories("${CUB_NAME}" PRIVATE "${CUB_SOURCE_DIR}")
target_include_directories("${CUB_NAME}" PUBLIC "${CUB_INCLUDE_DIR}")
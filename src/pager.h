// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_PAGER_H
#define CALICODB_PAGER_H

#include "calicodb/env.h"
#include "frames.h"
#include <unordered_set>

namespace calicodb
{

class Env;
class Freelist;
class FrameManager;
class TableSet;
class Wal;

// Freelist management. The freelist is essentially a linked list that is threaded through the database. Each freelist
// link page contains a pointer to the next freelist link page, or to Id::null() if it is the last link. Pages that are
// no longer needed by the tree are placed at the front of the freelist. When more pages are needed, the freelist is
// checked first. Only if it is empty do we allocate a page from the end of the file.
class Freelist
{
    friend class Tree;

    Pager *m_pager = nullptr;
    Id *m_head = nullptr;

public:
    explicit Freelist(Pager &pager, Id &head);
    [[nodiscard]] auto is_empty() const -> bool;
    [[nodiscard]] auto pop(Page &page) -> Status;
    [[nodiscard]] auto push(Page page) -> Status;
};

class Pager final
{
public:
    friend class DBImpl;

    enum Mode {
        kOpen,
        kWrite,
        kDirty,
        kError,
    };

    struct Parameters {
        std::string filename;
        Env *env = nullptr;
        Wal *wal = nullptr;
        LogFile *log = nullptr;
        DBState *state = nullptr;
        std::size_t frame_count = 0;
        std::size_t page_size = 0;
    };

    ~Pager();
    [[nodiscard]] static auto open(const Parameters &param, Pager *&out) -> Status;
    [[nodiscard]] auto mode() const -> Mode;
    [[nodiscard]] auto page_count() const -> std::size_t;
    [[nodiscard]] auto page_size() const -> std::size_t;
    [[nodiscard]] auto bytes_read() const -> std::size_t;
    [[nodiscard]] auto bytes_written() const -> std::size_t;
    [[nodiscard]] auto begin_txn() -> bool;
    [[nodiscard]] auto rollback_txn() -> Status;
    [[nodiscard]] auto commit_txn() -> Status;
    [[nodiscard]] auto checkpoint() -> Status;
    [[nodiscard]] auto flush_to_disk() -> Status;
    [[nodiscard]] auto allocate(Page &page) -> Status;
    [[nodiscard]] auto destroy(Page page) -> Status;
    [[nodiscard]] auto acquire(Id page_id, Page &page) -> Status;
    auto upgrade(Page &page) -> void;
    auto release(Page page) -> void;
    auto set_status(const Status &error) const -> Status;
    auto set_page_count(std::size_t page_count) -> void;
    auto load_state(const FileHeader &header) -> void;

    [[nodiscard]] auto acquire_root() -> Page;

    [[nodiscard]] auto hits() const -> U64
    {
        return m_cache.hits();
    }

    [[nodiscard]] auto misses() const -> U64
    {
        return m_cache.misses();
    }

    auto TEST_validate() const -> void;

private:
    explicit Pager(const Parameters &param, File &file, AlignedBuffer buffer);
    [[nodiscard]] auto initialize_root(bool fresh_pager) -> Status;
    [[nodiscard]] auto populate_entry(CacheEntry &out) -> Status;
    [[nodiscard]] auto cache_entry(Id page_id, CacheEntry *&out) -> Status;
    [[nodiscard]] auto read_page_from_file(CacheEntry &entry) const -> Status;
    [[nodiscard]] auto write_page_to_file(const CacheEntry &entry) const -> Status;
    [[nodiscard]] auto ensure_available_frame() -> Status;
    [[nodiscard]] auto wal_checkpoint() -> Status;
    auto purge_state() -> void;
    auto purge_entry(CacheEntry &victim) -> void;
    auto dirtylist_add(CacheEntry &entry) -> void;
    auto dirtylist_remove(CacheEntry &entry) -> CacheEntry *;

    mutable std::size_t m_bytes_read = 0;
    mutable std::size_t m_bytes_written = 0;
    mutable DBState *m_state = nullptr;
    mutable Mode m_mode = kOpen;

    std::string m_filename;
    FrameManager m_frames;
    Freelist m_freelist;
    PageCache m_cache;

    // The root page is always acquired. Keep info about it here.
    CacheEntry m_root;

    // List of dirty page cache entries. Linked by the "prev" and "next"
    // CacheEntry members.
    CacheEntry *m_dirty = nullptr;

    // True if a checkpoint operation is being run, false otherwise. Used
    // to indicate failure during a checkpoint.
    bool m_in_ckpt = false;

    LogFile *m_log = nullptr;
    File *m_file = nullptr;
    Env *m_env = nullptr;
    Wal *m_wal = nullptr;
    std::size_t m_page_count = 0;
    std::size_t m_saved_count = 0;
};

struct PointerMap {
    enum Type : char {
        kTreeNode,
        kTreeRoot,
        kOverflowHead,
        kOverflowLink,
        kFreelistLink,
    };

    struct Entry {
        Id back_ptr;
        Type type;
    };

    // Find the page ID of the pointer map page that holds the back pointer for page "page_id".
    [[nodiscard]] static auto lookup(const Pager &pager, Id page_id) -> Id;

    // Read an entry from the pointer map.
    [[nodiscard]] static auto read_entry(Pager &pager, Id page_id, Entry &entry) -> Status;

    // Write an entry to the pointer map.
    [[nodiscard]] static auto write_entry(Pager &pager, Id page_id, Entry entry) -> Status;
};

} // namespace calicodb

#endif // CALICODB_PAGER_H
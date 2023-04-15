// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_PAGER_H
#define CALICODB_PAGER_H

#include "bufmgr.h"
#include "calicodb/env.h"
#include <unordered_set>

namespace calicodb
{

class Env;
class Freelist;
class BufferManager;
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

// Pager state transitions:
//
//     StateA --> StateB  Method      LockA -------> LockB
//    ----------------------------------------------------------
//     kOpen ---> kRead   lock        <none> ------> kShared
//     kRead ---> kWrite  begin       kShared -----> kExclusive
//     kWrite --> kDirty  mark_dirty  kExclusive --> kExclusive
//     kDirty --> kWrite  commit      kExclusive --> kExclusive
//     kDirty --> kWrite  rollback    kExclusive --> kExclusive
//     ****** --> kOpen   unlock      ****** ------> <none>
//     ****** --> kError  set_state   ****** ------> ******
//
class Pager final
{
public:
    friend class DBImpl;
    friend class Tree;

    enum Mode {
        kOpen,
        kRead,
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

    struct Statistics {
        std::size_t bytes_read = 0;
        std::size_t bytes_written = 0;
    };

    ~Pager();
    [[nodiscard]] static auto open(const Parameters &param, Pager *&out) -> Status;
    [[nodiscard]] auto mode() const -> Mode;
    [[nodiscard]] auto page_count() const -> std::size_t;
    [[nodiscard]] auto page_size() const -> std::size_t;
    [[nodiscard]] auto statistics() const -> const Statistics &;
    [[nodiscard]] auto begin_txn() -> bool;
    [[nodiscard]] auto rollback_txn() -> Status;
    [[nodiscard]] auto commit_txn() -> Status;
    [[nodiscard]] auto checkpoint() -> Status;
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
        return m_bufmgr.hits();
    }

    [[nodiscard]] auto misses() const -> U64
    {
        return m_bufmgr.misses();
    }

    auto TEST_validate() const -> void;

private:
    explicit Pager(const Parameters &param, File &file);
    [[nodiscard]] auto initialize_root(bool fresh_pager) -> Status;
    [[nodiscard]] auto read_page(PageRef &out) -> Status;
    [[nodiscard]] auto read_page_from_file(PageRef &ref) const -> Status;
    [[nodiscard]] auto write_page_to_file(const PageRef &entry) const -> Status;
    [[nodiscard]] auto ensure_available_buffer() -> Status;
    [[nodiscard]] auto wal_checkpoint() -> Status;
    [[nodiscard]] auto flush_all_pages() -> Status;
    auto purge_page(PageRef &victim) -> void;
    auto purge_all_pages() -> void;

    mutable Statistics m_statistics;
    mutable DBState *m_state = nullptr;
    mutable Mode m_mode = kOpen;

    std::string m_filename;
    Dirtylist m_dirtylist;
    Freelist m_freelist;
    Bufmgr m_bufmgr;

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

    // Return the page ID of the pointer map page that holds the back pointer for page "page_id",
    // Id::null() otherwise.
    [[nodiscard]] static auto lookup(const Pager &pager, Id page_id) -> Id;

    // Return true if page "page_id" is a pointer map page, false otherwise.
    [[nodiscard]] static auto is_map(const Pager &pager, Id page_id) -> bool;

    // Read an entry from the pointer map.
    [[nodiscard]] static auto read_entry(Pager &pager, Id page_id, Entry &entry) -> Status;

    // Write an entry to the pointer map.
    [[nodiscard]] static auto write_entry(Pager &pager, Id page_id, Entry entry) -> Status;
};

} // namespace calicodb

#endif // CALICODB_PAGER_H
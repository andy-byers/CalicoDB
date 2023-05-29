// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_PAGER_H
#define CALICODB_PAGER_H

#include "bufmgr.h"
#include <unordered_set>

namespace calicodb
{

class Env;
class Wal;
struct WalStatistics;

class Pager final
{
public:
    friend class DBImpl;
    friend class Tree;
    friend class TxnImpl;

    enum Mode {
        kOpen,
        kRead,
        kWrite,
        kDirty,
        kError,
    };

    struct Parameters {
        const char *db_name;
        const char *wal_name;
        File *db_file;
        Env *env;
        Sink *log;
        Status *status;
        BusyHandler *busy;
        std::size_t frame_count;
        bool sync;
    };

    struct Statistics {
        std::size_t bytes_read = 0;
        std::size_t bytes_written = 0;
    };

    ~Pager();
    [[nodiscard]] static auto open(const Parameters &param, Pager *&out) -> Status;
    [[nodiscard]] auto close() -> Status;
    [[nodiscard]] auto mode() const -> Mode;
    [[nodiscard]] auto page_count() const -> std::size_t;
    [[nodiscard]] auto statistics() const -> const Statistics &;
    [[nodiscard]] auto wal_statistics() const -> WalStatistics;

    [[nodiscard]] auto start_reader() -> Status;
    [[nodiscard]] auto start_writer() -> Status;
    [[nodiscard]] auto commit() -> Status;
    auto rollback() -> void;
    auto finish() -> void;

    [[nodiscard]] auto checkpoint(bool reset) -> Status;
    [[nodiscard]] auto allocate(Page &page) -> Status;
    [[nodiscard]] auto destroy(Page page) -> Status;
    [[nodiscard]] auto acquire(Id page_id, Page &page) -> Status;
    auto mark_dirty(Page &page) -> void;
    auto set_page_count(std::size_t page_count) -> void;
    [[nodiscard]] auto acquire_root() -> Page;
    [[nodiscard]] auto hits() const -> U64;
    [[nodiscard]] auto misses() const -> U64;
    auto assert_state() const -> bool;
    auto purge_cached_pages() -> void;
    auto initialize_root() -> void;

    // Action to take when a page is released
    // Actions other than kKeep exist as optimizations. Using kKeep for every
    // release would not cause incorrect behavior.
    // |ReleaseAction | Purpose                                                  |
    // |--------------|----------------------------------------------------------|
    // | kKeep        | Normal release, reference stays in the cache. This is    |
    // |              | the default, and should be used for nodes, pointer maps, |
    // |              | and freelist trunks.                                     |
    // | kNoCache     | Erase the cached reference on release. This              |
    // |              | action is used for overflow pages, which may need to be  |
    // |              | written out, but are accessed relatively infrequently.   |
    // | kDiscard     | Same as kNoCache, except the page is never written to    |
    // |              | the WAL. This action is used for freelist leaf pages.    |
    // NOTE: kNoCache and kDiscard are used to limit how much a routine will mess
    // up the cache. For example, overflow pages are released with kNoCache, so
    // when an overflow chain is traversed, the same page is reused for each link.
    enum ReleaseAction { kKeep,
                         kNoCache,
                         kDiscard };

    // Release a referenced page back to the pager
    // This routine is a NOOP if page was already released.
    auto release(Page page, ReleaseAction action = kKeep) -> void;

    auto set_status(const Status &error) const -> Status
    {
        if (m_status->is_ok() && (error.is_io_error() || error.is_corruption())) {
            *m_status = error;
            m_mode = kError;
        }
        return error;
    }

private:
    explicit Pager(const Parameters &param);
    [[nodiscard]] auto dirtylist_contains(const PageRef &ref) const -> bool;
    [[nodiscard]] auto refresh_state() -> Status;
    [[nodiscard]] auto open_wal() -> Status;
    [[nodiscard]] auto read_page(PageRef &out, size_t *size_out) -> Status;
    [[nodiscard]] auto read_page_from_file(PageRef &ref, std::size_t *size_out) const -> Status;
    [[nodiscard]] auto ensure_available_buffer() -> Status;
    [[nodiscard]] auto flush_dirty_pages() -> Status;
    auto purge_page(PageRef &victim) -> void;

    mutable Statistics m_statistics;
    mutable Status *m_status;
    mutable Mode m_mode = kOpen;
    mutable Mode m_save = kOpen;

    const char *m_db_name;
    const char *m_wal_name;
    const bool m_sync;

    Dirtylist m_dirtylist;
    Bufmgr m_bufmgr;

    // True if the in-memory root page needs to be refreshed, false otherwise.
    bool m_refresh_root = false;

    Sink *m_log = nullptr;
    File *m_file;
    Env *m_env;
    Wal *m_wal = nullptr;
    BusyHandler *m_busy = nullptr;
    U32 m_page_count = 0;
    U32 m_save_count = 0;
};

// The first pointer map page is always on page 2, right after the root page.
static constexpr std::size_t kFirstMapPage = 2;

struct PointerMap {
    enum Type : char {
        kTreeNode,
        kTreeRoot,
        kOverflowHead,
        kOverflowLink,
        kFreelistTrunk,
        kFreelistLeaf,
    };

    struct Entry {
        Id back_ptr;
        Type type = kTreeNode;
    };

    // Return the page ID of the pointer map page that holds the back pointer for page "page_id",
    // Id::null() otherwise.
    [[nodiscard]] static auto lookup(Id page_id) -> Id;

    // Return true if page "page_id" is a pointer map page, false otherwise.
    [[nodiscard]] static auto is_map(Id page_id) -> bool;

    // Read an entry from the pointer map.
    [[nodiscard]] static auto read_entry(Pager &pager, Id page_id, Entry &entry) -> Status;

    // Write an entry to the pointer map.
    [[nodiscard]] static auto write_entry(Pager &pager, Id page_id, Entry entry) -> Status;
};

} // namespace calicodb

#endif // CALICODB_PAGER_H
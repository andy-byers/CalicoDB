// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_PAGER_H
#define CALICODB_PAGER_H

#include "buffer.h"
#include "bufmgr.h"
#include "stat.h"
#include "wal.h"

namespace calicodb
{

class Env;

class Pager final
{
public:
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
        Logger *log;
        Status *status;
        Stat *stat;
        BusyHandler *busy;
        uint32_t page_size;
        size_t cache_size;
        Options::SyncMode sync_mode;
        Options::LockMode lock_mode;
        bool persistent;
    };

    ~Pager();

    [[nodiscard]] auto page_size() const -> uint32_t
    {
        return m_page_size;
    }

    [[nodiscard]] auto page_count() const -> uint32_t
    {
        CALICODB_EXPECT_LT(kOpen, m_mode);
        return m_page_count;
    }

    [[nodiscard]] auto mode() const -> Mode
    {
        return m_mode;
    }

    static auto open(const Parameters &param, Pager *&out) -> Status;
    auto close() -> void;

    auto start_reader() -> Status;
    auto start_writer() -> Status;
    auto commit() -> Status;
    auto finish() -> void;

    auto checkpoint(bool reset) -> Status;
    auto auto_checkpoint(size_t frame_limit) -> Status;

    auto allocate(PageRef *&page_out) -> Status;
    auto acquire(Id page_id, PageRef *&page_out) -> Status;
    auto mark_dirty(PageRef &page) -> void;
    [[nodiscard]] auto get_root() -> PageRef &;

    auto set_page_count(uint32_t page_count) -> void;
    auto assert_state() const -> bool;
    auto purge_pages(bool purge_all) -> void;
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
    enum ReleaseAction {
        kDiscard,
        kNoCache,
        kKeep,
    };

    // Release a referenced page back to the pager
    // This routine is a NOOP if page was already released.
    auto release(PageRef *&page, ReleaseAction action = kKeep) -> void;

    auto get_unused_page(PageRef *&page_out) -> Status;
    auto move_page(PageRef &page, Id location) -> void;

    auto status() const -> Status
    {
        return *m_status;
    }

    auto set_status(const Status &error) const -> void;

private:
    friend class Alloc;
    explicit Pager(const Parameters &param);

    auto open_wal() -> Status;
    auto close_wal() -> Status;
    auto refresh_state() -> Status;
    auto set_page_size(uint32_t value) -> Status;
    auto read_page(PageRef &out, size_t *size_out) -> Status;
    auto read_page_from_file(PageRef &ref, size_t *size_out) const -> Status;
    auto ensure_available_buffer() -> Status;
    auto flush_dirty_pages() -> Status;
    auto purge_page(PageRef &victim) -> void;

    static auto undo_callback(void *arg, Id id) -> void;

    mutable Mode m_mode = kOpen;

    Bufmgr m_bufmgr;
    Dirtylist m_dirtylist;
    Buffer<char> m_scratch;

    Status *const m_status;
    Logger *const m_log;
    Env *const m_env;
    File *const m_file;
    Stat *const m_stat;
    BusyHandler *const m_busy;

    const Options::LockMode m_lock_mode;
    const Options::SyncMode m_sync_mode;
    const bool m_persistent;
    const char *const m_db_name;
    const char *const m_wal_name;

    Wal *m_wal = nullptr;

    uint32_t m_page_size = 0;
    uint32_t m_page_count = 0;
    uint32_t m_saved_page_count = 0;
    bool m_refresh = true;
};

} // namespace calicodb

#endif // CALICODB_PAGER_H
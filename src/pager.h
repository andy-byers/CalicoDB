// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_PAGER_H
#define CALICODB_PAGER_H

#include "buffer.h"
#include "bufmgr.h"

namespace calicodb
{

class Env;
class Wal;

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
        Wal *wal;
        Logger *log;
        Status *status;
        Stats *stat;
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
    void close();

    auto lock_reader(bool *changed_out) -> Status;
    auto begin_writer() -> Status;
    auto commit() -> Status;
    void finish();

    auto checkpoint(CheckpointMode mode, CheckpointInfo *info_out) -> Status;
    auto auto_checkpoint(size_t frame_limit) -> Status;

    auto allocate(PageRef *&page_out) -> Status;
    auto acquire(Id page_id, PageRef *&page_out) -> Status;
    void mark_dirty(PageRef &page);
    [[nodiscard]] auto get_root() -> PageRef &;

    void set_page_count(uint32_t page_count);
    auto assert_state() const -> bool;
    void purge_pages(bool purge_all);

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
    void release(PageRef *&page, ReleaseAction action = kKeep);

    auto get_unused_page(PageRef *&page_out) -> Status;
    void move_page(PageRef &page, Id location);

    auto status() const -> Status
    {
        return *m_status;
    }

    auto scratch() -> char *
    {
        return m_scratch.data();
    }

    void set_status(const Status &error) const;

private:
    friend class Mem;
    explicit Pager(const Parameters &param);

    void initialize_root();
    auto lock_reader_impl(bool refresh) -> Status;
    auto get_page_count(uint32_t &value_out) -> Status;
    auto open_wal_if_present() -> Status;
    auto open_wal() -> Status;
    auto close_wal() -> Status;
    auto refresh_state() -> Status;
    auto set_page_size(uint32_t value) -> Status;
    auto read_page(PageRef &out, size_t *size_out) -> Status;
    auto read_page_from_file(PageRef &ref, size_t *size_out) const -> Status;
    auto ensure_available_buffer() -> Status;
    auto flush_dirty_pages() -> Status;
    void purge_page(PageRef &victim);

    static void undo_callback(void *arg, uint32_t id);

    mutable Mode m_mode = kOpen;

    Bufmgr m_bufmgr;
    Dirtylist m_dirtylist;
    Buffer<char> m_scratch;

    Status *const m_status;
    Logger *const m_log;
    Env *const m_env;
    Wal *const m_user_wal;
    File *const m_file;
    Stats *const m_stats;
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

template <class Operation>
auto pager_read(Pager &pager, const Operation &operation) -> Status
{
    CALICODB_EXPECT_GE(pager.mode(), Pager::kRead);
    CALICODB_EXPECT_LT(pager.mode(), Pager::kError);
    if (pager.page_count() == 0) {
        return Status::invalid_argument("database is empty");
    }
    return operation();
}

template <class Operation>
auto pager_write(Pager &pager, const Operation &operation) -> Status
{
    Status s;
    if (pager.mode() < Pager::kWrite) {
        s = Status::not_supported("transaction is readonly");
    } else if (!pager.status().is_ok()) {
        s = pager.status();
    } else {
        s = operation();
        if (!s.is_ok()) {
            pager.set_status(s);
        }
    }
    return s;
}

} // namespace calicodb

#endif // CALICODB_PAGER_H
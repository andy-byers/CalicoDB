// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_PAGER_H
#define CALICODB_PAGER_H

#include <unordered_set>

#include "page_cache.h"
#include "pager.h"

#include "wal_record.h"

namespace calicodb
{

class Env;
class FrameManager;
class WriteAheadLog;
class TableSet;

class Pager
{
public:
    friend class DBImpl;

    struct Parameters {
        std::string filename;
        Env *env {};
        WriteAheadLog *wal {};
        InfoLogger *info_log {};
        DBState *state {};
        std::size_t frame_count {};
        std::size_t page_size {};
    };

    ~Pager() = default;

    [[nodiscard]] static auto open(const Parameters &param, Pager **out) -> Status;
    [[nodiscard]] auto page_count() const -> std::size_t;
    [[nodiscard]] auto page_size() const -> std::size_t;
    [[nodiscard]] auto recovery_lsn() const -> Lsn;
    [[nodiscard]] auto bytes_read() const -> std::size_t;
    [[nodiscard]] auto bytes_written() const -> std::size_t;
    [[nodiscard]] auto truncate(std::size_t page_count) -> Status;
    [[nodiscard]] auto flush(Lsn target_lsn = Lsn::null()) -> Status;
    [[nodiscard]] auto sync() -> Status;
    [[nodiscard]] auto allocate(Page &page) -> Status;
    [[nodiscard]] auto acquire(Id page_id, Page &page) -> Status;
    auto upgrade(Page &page, int important = -1) -> void;
    auto release(Page page) -> void;
    auto save_state(FileHeader &header) -> void;
    auto load_state(const FileHeader &header) -> void;

private:
    explicit Pager(const Parameters &param, Editor &file, AlignedBuffer buffer);
    [[nodiscard]] auto pin_frame(Id pid) -> Status;
    [[nodiscard]] auto do_pin_frame(Id pid) -> Status;
    [[nodiscard]] auto make_frame_available() -> bool;
    auto clean_page(PageCache::Entry &entry) -> DirtyTable::Iterator;

    std::string m_filename;
    FrameManager m_frames;
    DirtyTable m_dirty;
    PageCache m_cache;
    WriteAheadLog *m_wal {};
    Env *m_env {};
    InfoLogger *m_info_log {};
    DBState *m_state {};
};

} // namespace calicodb

#endif // CALICODB_PAGER_H
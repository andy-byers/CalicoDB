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
class FrameManager;
class WriteAheadLog;
class TableSet;
class Wal;

class Pager final
{
public:
    friend class DBImpl;

    struct Parameters {
        std::string filename;
        Env *env = nullptr;
        Wal *wal = nullptr;
        LogFile *info_log = nullptr;
        DBState *state = nullptr;
        std::size_t frame_count = 0;
        std::size_t page_size = 0;
    };

    ~Pager();
    [[nodiscard]] static auto open(const Parameters &param, Pager **out) -> Status;
    [[nodiscard]] auto page_count() const -> std::size_t;
    [[nodiscard]] auto page_size() const -> std::size_t;
    [[nodiscard]] auto bytes_read() const -> std::size_t;
    [[nodiscard]] auto bytes_written() const -> std::size_t;
    [[nodiscard]] auto truncate(std::size_t page_count) -> Status;
    [[nodiscard]] auto flush() -> Status;
    [[nodiscard]] auto commit() -> Status;
    [[nodiscard]] auto checkpoint() -> Status;
    [[nodiscard]] auto allocate(Page &page) -> Status;
    [[nodiscard]] auto acquire(Id page_id, Page &page) -> Status;
    auto upgrade(Page &page) -> void;
    auto release(Page page) -> void;
    auto load_state(const FileHeader &header) -> void;

    [[nodiscard]] auto hits() const -> std::uint64_t
    {
        return m_cache.hits();
    }

    [[nodiscard]] auto misses() const -> std::uint64_t
    {
        return m_cache.misses();
    }

private:
    explicit Pager(const Parameters &param, File &file, AlignedBuffer buffer);
    [[nodiscard]] auto fetch_page(Id page_id, CacheEntry *&out) -> Status;
    [[nodiscard]] auto read_page_from_file(Id page_id, char *out) const -> Status;
    [[nodiscard]] auto write_page_to_file(Id pid, const Slice &in) const -> Status;
    [[nodiscard]] auto ensure_available_frame() -> Status;
    auto clean_page(CacheEntry &entry) -> void;

    mutable std::size_t m_bytes_read = 0;
    mutable std::size_t m_bytes_written = 0;

    std::string m_filename;
    FrameManager m_frames;
    std::set<Id> m_dirty;
    PageCache m_cache;
    LogFile *m_info_log = nullptr;
    File *m_file = nullptr;
    Env *m_env = nullptr;
    Wal *m_wal = nullptr;
    DBState *m_state = nullptr;
    std::size_t m_page_count = 0;
};

} // namespace calicodb

#endif // CALICODB_PAGER_H
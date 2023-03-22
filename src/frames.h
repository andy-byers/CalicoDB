// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_FRAMES_H
#define CALICODB_FRAMES_H

#include "delta.h"
#include "utils.h"
#include <list>
#include <map>
#include <optional>
#include <unordered_map>
#include <vector>

namespace calicodb
{

struct FileHeader;
class Page;
class Pager;
class Editor;
class Env;

// Manages the set of dirty pages.
//
// When a page is first made dirty, it should be added to this table along
// with the current page LSN.
class DirtyTable final
{
public:
    using Iterator = std::multimap<Lsn, Id>::iterator;

    // Return the number of dirty pages.
    [[nodiscard]] auto size() const -> std::size_t;

    // Iterate over dirty pages, ordered by increasing record LSNs.
    [[nodiscard]] auto begin() -> Iterator;
    [[nodiscard]] auto end() -> Iterator;

    // Add a recently-modified page to the table.
    [[nodiscard]] auto insert(Id page_id, Lsn record_lsn) -> Iterator;

    // Remove a recently-flushed or recently-truncated page from the table.
    auto remove(Iterator itr) -> Iterator;

    // Find the oldest non-NULL LSN currently in the table.
    [[nodiscard]] auto recovery_lsn() const -> Lsn;

private:
    // There may be multiple Lsn::null() in the dirty set.
    std::multimap<Lsn, Id> m_dirty;
};

struct CacheEntry {
    using Token = std::optional<DirtyTable::Iterator>;

    Id page_id;
    std::size_t index {};
    unsigned refcount {};

    // If the page represented by this entry is dirty, this should point
    // into the corresponding dirty table entry.
    Token token;
};

// Mapping from page IDs to frame indices.
//
// If the page is made dirty, the cache entry should be updated to contain the
// iterator returned by DirtyTable::insert().
class PageCache final
{
public:
    // query() does not alter the cache ordering.
    [[nodiscard]] auto query(Id page_id) -> CacheEntry *;

    // Return the number of entries in the cache.
    [[nodiscard]] auto size() const -> std::size_t;

    // Return a pointer to a specific cache entry, if it exists.
    //
    // This method alters the cache ordering if, and only if, it finds an entry.
    [[nodiscard]] auto get(Id page_id) -> CacheEntry *;

    // Evict the next unreferenced entry based on the cache replacement policy, if
    // one exists.
    [[nodiscard]] auto evict() -> std::optional<CacheEntry>;

    // Add an entry to the cache, which must not already exist.
    auto put(CacheEntry entry) -> CacheEntry *;

    // Erase a specific entry, if it exists.
    auto erase(Id page_id) -> bool;

private:
    using MapEntry = std::list<CacheEntry>::iterator;

    std::unordered_map<Id, MapEntry, Id::Hash> m_map;
    std::list<CacheEntry> m_list;
    std::uint64_t m_misses {};
    std::uint64_t m_hits {};
};

struct Frame {
    explicit Frame(char *buffer);
    [[nodiscard]] auto lsn() const -> Id;

    Id page_id;
    char *data;
};

class AlignedBuffer
{
public:
    explicit AlignedBuffer(std::size_t size, std::size_t alignment);

    [[nodiscard]] auto get() -> char *
    {
        return m_data.get();
    }

    [[nodiscard]] auto get() const -> const char *
    {
        return m_data.get();
    }

private:
    struct Deleter {
        auto operator()(char *ptr) const -> void
        {
            operator delete[](ptr, alignment);
        }

        std::align_val_t alignment;
    };

    std::unique_ptr<char[], Deleter> m_data;
};

class FrameManager final
{
public:
    friend class DBImpl;
    friend class Pager;

    explicit FrameManager(Editor &file, AlignedBuffer buffer, std::size_t page_size, std::size_t frame_count);
    ~FrameManager() = default;
    [[nodiscard]] auto write_back(std::size_t index) -> Status;
    [[nodiscard]] auto sync() -> Status;
    [[nodiscard]] auto pin(Id page_id, CacheEntry &entry) -> Status;
    [[nodiscard]] auto ref(CacheEntry &entry, Page &out) -> Status;
    auto unpin(CacheEntry &entry) -> void;
    auto unref(CacheEntry &entry) -> void;
    auto upgrade(Page &page) -> void;

    [[nodiscard]] auto page_size() const -> std::size_t
    {
        return m_page_size;
    }

    [[nodiscard]] auto available() const -> std::size_t
    {
        return m_unpinned.size();
    }

    [[nodiscard]] auto get_frame(std::size_t index) const -> const Frame &
    {
        CALICODB_EXPECT_LT(index, m_frames.size());
        return m_frames[index];
    }

    [[nodiscard]] auto bytes_read() const -> std::size_t
    {
        return m_bytes_read;
    }

    [[nodiscard]] auto bytes_written() const -> std::size_t
    {
        return m_bytes_written;
    }

    // Disable move and copy.
    auto operator=(FrameManager &) -> void = delete;
    FrameManager(FrameManager &) = delete;

private:
    [[nodiscard]] auto read_page_from_file(Id page_id, char *out) const -> Status;
    [[nodiscard]] auto write_page_to_file(Id page_id, const char *in) const -> Status;

    AlignedBuffer m_buffer;
    std::vector<Frame> m_frames;
    std::list<std::size_t> m_unpinned;
    std::unique_ptr<Editor> m_file;
    std::size_t m_page_size {};
    std::size_t m_refsum {};

    mutable std::size_t m_bytes_read {};
    mutable std::size_t m_bytes_written {};
};

} // namespace calicodb

#endif // CALICODB_FRAMES_H

// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_FRAMES_H
#define CALICODB_FRAMES_H

#include "utils.h"
#include <list>
#include <map>
#include <optional>
#include <set>
#include <unordered_map>
#include <vector>

namespace calicodb
{

struct FileHeader;
class Page;
class Pager;
class File;
class Env;

struct CacheEntry {
    Id page_id;
    std::size_t index = 0;
    unsigned refcount = 0;
    bool is_dirty = false;
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

    // Attempt to evict the next unreferenced entry based on the cache replacement
    // policy.
    [[nodiscard]] auto evict() -> std::optional<CacheEntry>;

    // Add an entry to the cache, which must not already exist.
    auto put(CacheEntry entry) -> CacheEntry *;

    // Erase a specific entry, if it exists.
    auto erase(Id page_id) -> bool;

    [[nodiscard]] auto hits() const -> std::uint64_t;
    [[nodiscard]] auto misses() const -> std::uint64_t;

private:
    using MapEntry = std::list<CacheEntry>::iterator;

    std::unordered_map<Id, MapEntry, Id::Hash> m_map;
    std::list<CacheEntry> m_list;
    std::uint64_t m_misses = 0;
    std::uint64_t m_hits = 0;
};

class AlignedBuffer
{
    struct Deleter {
        auto operator()(char *ptr) const -> void
        {
            operator delete[](ptr, alignment);
        }

        std::align_val_t alignment;
    };

    std::unique_ptr<char[], Deleter> m_data;

public:
    explicit AlignedBuffer(std::size_t size, std::size_t alignment);

    char *data;
};

class FrameManager final
{
public:
    friend class DBImpl;
    friend class Pager;

    explicit FrameManager(AlignedBuffer buffer, std::size_t page_size, std::size_t frame_count);
    ~FrameManager() = default;
    [[nodiscard]] auto get_frame(std::size_t index) const -> Slice;
    [[nodiscard]] auto pin(Id page_id, CacheEntry &entry) -> char *;
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

    // Disable move and copy.
    auto operator=(FrameManager &) -> void = delete;
    FrameManager(FrameManager &) = delete;

private:
    [[nodiscard]] auto get_frame_pointer(std::size_t index) -> char *;

    AlignedBuffer m_buffer;
    std::list<std::size_t> m_unpinned;
    std::size_t m_frame_count = 0;
    std::size_t m_page_size = 0;
    std::size_t m_refsum = 0;
};

} // namespace calicodb

#endif // CALICODB_FRAMES_H

// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_BUFMGR_H
#define CALICODB_BUFMGR_H

#include "page.h"
#include "utils.h"
#include <list>
#include <map>
#include <optional>
#include <set>
#include <unordered_map>
#include <vector>

namespace calicodb
{

class Pager;
class File;
class Env;

// Manages database pages that have been read from stable storage
class Bufmgr final
{
public:
    explicit Bufmgr(std::size_t frame_count);
    ~Bufmgr();

    // Get a reference to the root page, which is always in-memory, but is not
    // addressable in the cache
    // Note that it is a logic error to attempt to get a reference to the root page
    // using a different method. This method must be used.
    [[nodiscard]] auto root() -> PageRef *
    {
        return &m_root;
    }

    // Return a pointer to a specific cache entry, if it exists, nullptr otherwise
    // This method may alter the cache ordering.
    [[nodiscard]] auto get(Id page_id) -> PageRef *;

    // Similar to get(), except that the cache ordering is not altered
    [[nodiscard]] auto query(Id page_id) -> PageRef *;

    // Determine the next unreferenced page that should be evicted based on the
    // cache replacement policy
    [[nodiscard]] auto next_victim() -> PageRef *;

    // Create a new cache entry for page `page_id` which must not already exist
    // Returns the address of the cache entry, which is guaranteed to remain constant
    // as long the entry exists in the cache (until Bufmgr::erase() is called on
    // `page_id`).
    [[nodiscard]] auto alloc(Id page_id) -> PageRef *;

    // Erase a specific entry, if it exists
    // This is the only way that an entry can be removed from the cache. Eviction
    // works by first calling "next_victim()" and then erasing the returned entry.
    // Returns true if the entry was erased, false otherwise.
    auto erase(Id page_id) -> bool;

    // Increment the reference count associated with a page reference
    auto ref(PageRef &ref) -> void;

    // Decrement the reference count associated with a page reference
    // REQUIRES: Refcount of `ref` is not already 0
    auto unref(PageRef &ref) -> void;

    // Return the number of entries in the cache
    [[nodiscard]] auto occupied() const -> std::size_t
    {
        return m_map.size();
    }

    // Return the number of available buffers
    [[nodiscard]] auto available() const -> std::size_t
    {
        return m_available.size();
    }

    // Return the number of live page references
    [[nodiscard]] auto refsum() const -> unsigned
    {
        return m_refsum;
    }

    auto swap_refs(PageRef &lhs, PageRef &rhs) -> void;

    // Disable move and copy.
    void operator=(Bufmgr &) = delete;
    Bufmgr(Bufmgr &) = delete;

    U64 cache_hits = 0;
    U64 cache_misses = 0;

private:
    [[nodiscard]] auto buffer_slot(std::size_t index) -> char *
    {
        CALICODB_EXPECT_LT(index, m_frame_count);
        return m_buffer + index * kPageSize;
    }

    // Pin an available buffer to a page reference
    auto pin(PageRef &ref) -> void;

    // Return a page reference's backing buffer to the pool of unused buffers
    auto unpin(PageRef &ref) -> void;

    // LRU cache state variables. The storage for PageRef instances generally
    // resides in "m_list". Pointers are handed out by various methods, which
    // should remain stable until the underlying entry is erased.
    using MapEntry = std::list<PageRef>::iterator;
    std::unordered_map<Id, MapEntry, Id::Hash> m_map;
    std::list<PageRef> m_list;

    // Root page is stored separately. It is accessed very often, so it makes
    // sense to keep it in a dedicated location rather than having to find it
    // in the hash map each time.
    PageRef m_root;

    // Storage for cached pages. Aligned to the page size.
    char *m_buffer = nullptr;

    // List of pointers to available buffer slots.
    std::list<char *> m_available;

    // Used to perform bounds checking in assertions.
    std::size_t m_frame_count = 0;

    unsigned m_refsum = 0;
};

struct Dirtylist {
    auto remove(PageRef &ref) -> PageRef *;
    auto add(PageRef &ref) -> void;
    auto sort() -> void;

    PageRef *head = nullptr;
};

} // namespace calicodb

#endif // CALICODB_BUFMGR_H

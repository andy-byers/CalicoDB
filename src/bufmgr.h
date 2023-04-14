// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_BUFMGR_H
#define CALICODB_BUFMGR_H

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

struct PageRef final {
    Id page_id;

    // Pointer to the start of the buffer slot containing the page data.
    char *page = nullptr;

    // Number of live copies of this page.
    unsigned refcount = 0;

    // Dirty list fields.
    PageRef *prev = nullptr;
    PageRef *next = nullptr;
    bool dirty = false;
};

// Manages database pages that have been read from stable storage
class Bufmgr final
{
public:
    Bufmgr(std::size_t page_size, std::size_t frame_count);
    ~Bufmgr();

    // Return the number of entries in the cache
    [[nodiscard]] auto size() const -> std::size_t;

    // Return a pointer to a specific cache entry, if it exists, nullptr otherwise
    //
    // This method may alter the cache ordering.
    [[nodiscard]] auto get(Id page_id) -> PageRef *;

    // Get a reference to the root page, which is always in-memory, but is not
    // addressable in the cache
    //
    // Note that it is a logic error to attempt to get a reference to the root page
    // using a different method. This method must be used.
    [[nodiscard]] auto root() -> PageRef *;

    // Similar to get(), except that the cache ordering is not altered
    [[nodiscard]] auto query(Id page_id) -> PageRef *;

    // Determine the next unreferenced page that should be evicted based on the
    // cache replacement policy
    [[nodiscard]] auto next_victim() -> PageRef *;

    // Create a new cache entry for page "page_id" which must not already exist
    //
    // Returns the address of the cache entry, which is guaranteed to remain constant
    // as long the entry exists in the cache.
    [[nodiscard]] auto alloc(Id page_id) -> PageRef *;

    // Erase a specific entry, if it exists
    //
    // This is the only way that an entry can be removed from the cache. Eviction
    // works by first calling "next_victim()" and then erasing the returned entry.
    // Returns true if the entry was erased, false otherwise.
    auto erase(Id page_id) -> bool;

    // Increment the reference count associated with a page reference
    auto ref(PageRef &ref) -> void;

    // Decrement the reference count (which must not already be 0) associated with
    // a page reference
    auto unref(PageRef &ref) -> void;

    // Return the size of a database page in bytes
    [[nodiscard]] auto page_size() const -> std::size_t
    {
        return m_page_size;
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

    // Disable move and copy.
    auto operator=(Bufmgr &) -> void = delete;
    Bufmgr(Bufmgr &) = delete;

    [[nodiscard]] auto hits() const -> U64;
    [[nodiscard]] auto misses() const -> U64;

private:
    [[nodiscard]] auto buffer_slot(std::size_t index) -> char *;

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

    // Keep the root page state information separate since it always stays in-
    // memory. The root page may be accessed repeatedly depending on how the
    // DB is being used.
    PageRef m_root;

    // Storage for cached pages. Aligned to the page size.
    char *m_buffer = nullptr;

    // List of pointers to available buffer slots.
    std::list<char *> m_available;

    std::size_t m_frame_count = 0;
    std::size_t m_page_size = 0;
    unsigned m_refsum = 0;
    U64 m_misses = 0;
    U64 m_hits = 0;
};

struct Dirtylist {
    auto remove(PageRef &ref) -> PageRef *;
    auto add(PageRef &ref) -> void;

    PageRef *head = nullptr;
};

} // namespace calicodb

#endif // CALICODB_BUFMGR_H

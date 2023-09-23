// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_BUFMGR_H
#define CALICODB_BUFMGR_H

#include "buffer.h"
#include "internal.h"
#include "page.h"

namespace calicodb
{

class File;
class Env;
class Pager;
struct Stats;

// Manages database pages that have been read from stable storage
class Bufmgr final
{
public:
    friend class Pager;

    explicit Bufmgr(size_t min_buffers, Stats &stat);
    ~Bufmgr();

    // Allocate m_min_buffers page buffers for non-root pages, each of size `page_size`,
    // the root page buffer, and enough hash table slots to accommodate all non-root
    // pages without incurring too many collisions. There must not be any referenced
    // or dirty pages when this method is called.
    [[nodiscard]] auto reallocate(size_t page_size) -> int;

    // Get a reference to the root page, which is always in-memory, but is not
    // addressable in the cache
    // Note that it is a logic error to attempt to get a reference to the root page
    // using a different method. This method must be used.
    [[nodiscard]] auto root() -> PageRef *
    {
        return m_root;
    }

    // Similar to get(), except that the cache ordering is not altered
    [[nodiscard]] auto query(Id page_id) const -> PageRef *;

    // Return a pointer to a specific cache entry, if it exists, nullptr otherwise
    // This method may alter the cache ordering.
    [[nodiscard]] auto lookup(Id page_id) -> PageRef *;

    [[nodiscard]] auto next_victim() -> PageRef *;
    [[nodiscard]] auto allocate(size_t page_size) -> PageRef *;
    auto register_page(PageRef &ref) -> void;
    auto shrink_to_fit() -> void;

    // Erase a specific entry, if it exists
    // This is the only way that an entry can be removed from the cache. Eviction
    // works by first calling "next_victim()" and then erasing the returned entry.
    // Returns true if the entry was erased, false otherwise.
    auto erase(PageRef &ref) -> void;
    auto purge() -> void;

    // Increment the reference count associated with a page reference
    auto ref(PageRef &ref) -> void;

    // Decrement the reference count associated with a page reference
    // REQUIRES: Refcount of `ref` is not already 0
    auto unref(PageRef &ref) -> void;

    // Return the number of live page references
    [[nodiscard]] auto refsum() const -> size_t
    {
        return m_refsum;
    }

    auto assert_state() const -> bool;

    // Disable move and copy.
    void operator=(Bufmgr &) = delete;
    Bufmgr(Bufmgr &) = delete;

private:
    auto free_buffers() -> void;

    // Hash table modified from LevelDB. Maps each cached page ID to a page reference:
    // a structure that contains the page contents from disk, as well as some other
    // metadata. Each page reference in m_map can also be found in either m_lru or
    // m_in_use.
    class PageTable
    {
    public:
        friend class Bufmgr;

        PageTable()
            : m_capacity(0),
              m_length(0),
              m_table(nullptr)
        {
        }

        ~PageTable()
        {
            Mem::deallocate(m_table);
        }

        auto clear() -> void
        {
            std::memset(m_table, 0, m_capacity * sizeof(PageRef *));
        }

        [[nodiscard]] auto lookup(uint32_t key) const -> PageRef *
        {
            return *find_pointer(key);
        }

        [[nodiscard]] auto allocate(size_t min_buffers) -> int;

        // NOTE: PageRef with key `ref->key()` must not exist.
        auto insert(PageRef *ref) -> void;
        auto remove(uint32_t key) -> PageRef *;

    private:
        // The table consists of an array of buckets where each bucket is
        // a linked list of cache entries that hash into the bucket.
        uint32_t m_capacity;
        uint32_t m_length;
        PageRef **m_table;

        // Return a pointer to slot that points to a cache entry that
        // matches key/hash.  If there is no such cache entry, return a
        // pointer to the trailing slot in the corresponding linked list.
        [[nodiscard]] auto find_pointer(uint32_t key) const -> PageRef **
        {
            auto **ptr = &m_table[key & (m_capacity - 1)];
            while (*ptr != nullptr && key != (*ptr)->key()) {
                ptr = &(*ptr)->next_hash;
            }
            return ptr;
        }
    } m_table;

    // List containing page references that have a nonzero refcount field.
    // Unordered.
    PageRef m_in_use;

    // LRU-ordered list containing unreferenced pages. Elements are considered
    // to be in the cache if ref->get_flag(PageRef::kCached) evaluates to true.
    PageRef m_lru;

    // Storage for m_min_buffers database pages and associated metadata.
    Buffer<PageRef> m_metadata;
    Buffer<char> m_backing;

    // Linked list of extra page buffers.
    PageRef *m_extra = nullptr;

    // Root page is stored separately. It is accessed very often, so it makes
    // sense to keep it in a dedicated location rather than having to find it
    // in the hash map each time.
    PageRef *m_root = nullptr;

    Stats *const m_stat;

    const size_t m_min_buffers;
    size_t m_num_buffers = 0;
    size_t m_refsum = 0;
};

class Dirtylist
{
    DirtyHdr m_head;

public:
    explicit Dirtylist()
        : m_head{nullptr, &m_head, &m_head}
    {
    }

    [[nodiscard]] auto begin() -> DirtyHdr *
    {
        return m_head.next_entry;
    }

    [[nodiscard]] auto begin() const -> const DirtyHdr *
    {
        return m_head.next_entry;
    }

    [[nodiscard]] auto end() -> DirtyHdr *
    {
        return &m_head;
    }

    [[nodiscard]] auto end() const -> const DirtyHdr *
    {
        return &m_head;
    }

    [[nodiscard]] auto is_empty() const -> bool;
    auto remove(PageRef &ref) -> DirtyHdr *;
    auto add(PageRef &ref) -> void;
    auto sort() -> DirtyHdr *;

    [[nodiscard]] auto TEST_contains(const PageRef &ref) const -> bool;
};

} // namespace calicodb

#endif // CALICODB_BUFMGR_H

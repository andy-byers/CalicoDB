// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_BUFMGR_H
#define CALICODB_BUFMGR_H

#include "page.h"
#include "utils.h"

namespace calicodb
{

class Pager;
class File;
class Env;
struct Stat;

class PageTable
{
public:
    PageTable()
        : m_capacity(0),
          m_size(0),
          m_table(nullptr)
    {
    }

    ~PageTable()
    {
        Alloc::free(m_table);
    }

    [[nodiscard]] auto preallocate(size_t cache_size) -> int
    {
        CALICODB_EXPECT_EQ(m_capacity, 0);
        uint32_t capacity = 4;
        while (capacity < cache_size) {
            capacity *= 2;
        }
        const auto table_size = capacity * sizeof(PageRef *);
        if (auto *table = static_cast<PageRef **>(
                Alloc::malloc(table_size))) {
            std::memset(table, 0, table_size);
            m_capacity = capacity;
            m_table = table;
            return 0;
        }
        return -1;
    }

    auto clear() -> void
    {
        std::memset(m_table, 0, m_capacity * sizeof(PageRef *));
    }

    auto lookup(uint32_t key) -> PageRef *
    {
        return *find_pointer(key);
    }

    // NOTE: PageRef with key `ref->key()` must not exist.
    auto insert(PageRef *ref) -> void
    {
        auto **ptr = find_pointer(ref->key());
        CALICODB_EXPECT_NE(ptr, nullptr);
        CALICODB_EXPECT_EQ(*ptr, nullptr);
        ref->next_hash = nullptr;
        *ptr = ref;
        ++m_size;
    }

    auto remove(uint32_t key) -> PageRef *
    {
        auto **ptr = find_pointer(key);
        auto *result = *ptr;
        if (result != nullptr) {
            *ptr = result->next_hash;
            --m_size;
        }
        return result;
    }

private:
    // The table consists of an array of buckets where each bucket is
    // a linked list of cache entries that hash into the bucket.
    uint32_t m_capacity;
    uint32_t m_size;
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
};

// Manages database pages that have been read from stable storage
class Bufmgr final
{
public:
    friend class Pager;

    explicit Bufmgr(Stat &stat);
    ~Bufmgr();

    // Allocate `min_buffers` page buffers for non-root pages, the root page buffer,
    // and enough hash table slots to accommodate all non-root pages without incurring
    // too many collisions. This method must only be called once, right after the
    // constructor.
    [[nodiscard]] auto preallocate(size_t min_buffers) -> int;

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
    [[nodiscard]] auto allocate() -> PageRef *;
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
    [[nodiscard]] auto refsum() const -> unsigned
    {
        return m_refsum;
    }

    auto assert_state() const -> bool;

    // Disable move and copy.
    void operator=(Bufmgr &) = delete;
    Bufmgr(Bufmgr &) = delete;

private:
    // Page cache components:
    //     Member   | Purpose
    //    ----------|---------------------------------------------------------------
    //     m_map    | Maps each cached page ID to a page reference: a structure that
    //              | contains the page contents from disk, as well as some other
    //              | metadata. Each page reference in m_map can also be found in
    //              | either m_lru or m_in_use.
    //     m_lru    | LRU-ordered list containing page references. Elements are
    //              | considered valid if ref->get_flag(PageRef::kCached) evaluates
    //              | to true.
    //     m_in_use | List containing page references that have a nonzero refcount
    //              | field. Unordered.
    mutable PageTable m_table;
    PageRef m_in_use;
    PageRef m_lru;

    // Root page is stored separately. It is accessed very often, so it makes
    // sense to keep it in a dedicated location rather than having to find it
    // in the hash map each time.
    PageRef *m_root;

    size_t m_min_buffers;
    size_t m_num_buffers;

    Stat *const m_stat;

    unsigned m_refsum = 0;
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

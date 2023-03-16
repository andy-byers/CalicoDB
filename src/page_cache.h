// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_PAGE_CACHE_H
#define CALICODB_PAGE_CACHE_H

#include "cache.h"
#include "frames.h"
#include <map>

namespace calicodb
{

// Manages the set of pages that are dirty.
//
// When a page is written to, it should be added to this table, along with the
// current page LSN. The oldest LSN in this table marks the oldest WAL record
// that we still need.
class DirtyTable final
{
public:
    using Iterator = std::multimap<Lsn, Id, Lsn::Hash>::iterator;
    using ConstIterator = std::multimap<Lsn, Id, Lsn::Hash>::const_iterator;

    DirtyTable() = default;
    ~DirtyTable() = default;

    [[nodiscard]] auto size() const -> std::size_t
    {
        return m_dirty.size();
    }

    [[nodiscard]] auto begin() -> Iterator
    {
        using std::begin;
        return begin(m_dirty);
    }

    [[nodiscard]] auto begin() const -> ConstIterator
    {
        using std::begin;
        return begin(m_dirty);
    }

    [[nodiscard]] auto end() -> Iterator
    {
        using std::end;
        return end(m_dirty);
    }

    [[nodiscard]] auto end() const -> ConstIterator
    {
        using std::end;
        return end(m_dirty);
    }

    [[nodiscard]] auto insert(const Id &page_id, const Lsn &record_lsn) -> Iterator
    {
        // Use find() to get the insert() overload that returns an iterator, and to
        // assert that non-NULL LSNs are unique. The "record_lsn" is NULL when the
        // page has never been written to.
        auto itr = m_dirty.find(record_lsn);
        CALICODB_EXPECT_TRUE(record_lsn.is_null() || itr == end());
        return m_dirty.insert(itr, {record_lsn, page_id});
    }

    auto remove(Iterator itr) -> Iterator
    {
        return m_dirty.erase(itr);
    }

    // Find the oldest non-NULL LSN currently in the table.
    [[nodiscard]] auto recovery_lsn() const -> Lsn
    {
        auto itr = m_dirty.lower_bound(Lsn::root());
        if (itr == end()) {
            return Lsn::null();
        }
        return itr->first;
    }

private:
    std::multimap<Lsn, Id> m_dirty;
};

// Mapping from page IDs to frame indices.
//
// If the page is made dirty, the cache entry should be updated to contain the
// iterator returned by DirtyTable::insert().
class PageCache final
{
public:
    using Token = std::optional<DirtyTable::Iterator>;

    struct Entry {
        std::size_t index;
        Token token {};
    };

    using Base = Cache<Id, Entry, Id::Hash>;
    using Iterator = Base::Iterator;

    PageCache() = default;
    ~PageCache() = default;

    [[nodiscard]] auto is_empty() const -> std::size_t
    {
        return m_cache.is_empty();
    }

    [[nodiscard]] auto size() const -> std::size_t
    {
        return m_cache.size();
    }

    [[nodiscard]] auto contains(Id pid) const -> bool
    {
        return m_cache.contains(pid);
    }

    [[nodiscard]] auto hits() const -> std::uint64_t
    {
        return m_hits;
    }

    [[nodiscard]] auto misses() const -> std::uint64_t
    {
        return m_misses;
    }

    [[nodiscard]] auto end() -> Iterator
    {
        using std::end;
        return end(m_cache);
    }

    template <class Predicate>
    auto evict(const Predicate &predicate) -> std::optional<Entry>
    {
        using std::rbegin, std::rend;
        for (auto itr = rbegin(m_cache); itr != rend(m_cache); ++itr) {
            if (predicate(itr->key, itr->value)) {
                auto value = std::move(itr->value);
                m_cache.erase(itr->key);
                return value;
            }
        }
        return std::nullopt;
    }

    [[nodiscard]] auto get(Id pid) -> Iterator
    {
        using std::end;
        if (auto itr = m_cache.get(pid); itr != end(m_cache)) {
            ++m_hits;
            return itr;
        }
        ++m_misses;
        return end(*this);
    }

    auto put(Id pid, Entry entry) -> void
    {
        CALICODB_EXPECT_FALSE(m_cache.contains(pid));
        m_cache.put(pid, entry);
    }

    auto erase(Id pid) -> void
    {
        CALICODB_EXPECT_TRUE(m_cache.contains(pid));
        m_cache.erase(pid);
    }

private:
    Base m_cache;
    std::uint64_t m_hits {};
    std::uint64_t m_misses {};
};

} // namespace calicodb

#endif // CALICODB_PAGE_CACHE_H

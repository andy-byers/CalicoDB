// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_PAGE_CACHE_H
#define CALICODB_PAGE_CACHE_H

#include "cache.h"
#include "frames.h"
#include "types.h"

namespace calicodb
{

class PageList final
{
public:
    struct Entry {
        Id pid;
        Lsn record_lsn;
    };

    using Iterator = std::list<Entry>::iterator;
    using ConstIterator = std::list<Entry>::const_iterator;

    PageList() = default;
    ~PageList() = default;

    [[nodiscard]] auto size() const -> std::size_t
    {
        return m_list.size();
    }

    [[nodiscard]] auto begin() -> Iterator
    {
        using std::begin;
        return begin(m_list);
    }

    [[nodiscard]] auto begin() const -> ConstIterator
    {
        using std::begin;
        return begin(m_list);
    }

    [[nodiscard]] auto end() -> Iterator
    {
        using std::end;
        return end(m_list);
    }

    [[nodiscard]] auto end() const -> ConstIterator
    {
        using std::end;
        return end(m_list);
    }

    [[nodiscard]] auto insert(const Id &pid, const Lsn &record_lsn) -> Iterator
    {
        return m_list.emplace(cend(m_list), Entry {pid, record_lsn});
    }

    auto remove(Iterator itr) -> Iterator
    {
        return m_list.erase(itr);
    }

private:
    std::list<Entry> m_list;
};

class PageCache final
{
public:
    using Token = std::optional<PageList::Iterator>;

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
        CDB_EXPECT_FALSE(m_cache.contains(pid));
        m_cache.put(pid, entry);
    }

    auto erase(Id pid) -> void
    {
        CDB_EXPECT_TRUE(m_cache.contains(pid));
        m_cache.erase(pid);
    }

private:
    Base m_cache;
    std::uint64_t m_hits {};
    std::uint64_t m_misses {};
};

} // namespace calicodb

#endif // CALICODB_PAGE_CACHE_H

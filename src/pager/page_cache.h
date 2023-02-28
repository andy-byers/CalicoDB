/*
 * Very loosely based off of https://medium.com/@koushikmohan/an-analysis-of-2q-cache-replacement-algorithms-21acceae672a
 */

#ifndef CALICO_PAGER_CACHE_H
#define CALICO_PAGER_CACHE_H

#include "frames.h"
#include "utils/cache.h"
#include "utils/types.h"

namespace calicodb
{

class Frame;

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

    [[nodiscard]] auto size() const -> Size
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
        Size index;
        Token token {};
    };

    using Base = Cache<Id, Entry, Id::Hash>;
    using Iterator = Base::Iterator;

    PageCache() = default;
    ~PageCache() = default;

    [[nodiscard]] auto is_empty() const -> Size
    {
        return m_cache.is_empty();
    }

    [[nodiscard]] auto size() const -> Size
    {
        return m_cache.size();
    }

    [[nodiscard]] auto contains(Id pid) const -> bool
    {
        return m_cache.contains(pid);
    }

    [[nodiscard]] auto hit_ratio() const -> double
    {
        if (const auto total = static_cast<double>(m_hits + m_misses); total != 0.0) {
            return static_cast<double>(m_hits) / total;
        }
        return 0.0;
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
        CALICO_EXPECT_FALSE(m_cache.contains(pid));
        m_cache.put(pid, entry);
    }

    auto erase(Id pid) -> void
    {
        CALICO_EXPECT_TRUE(m_cache.contains(pid));
        m_cache.erase(pid);
    }

private:
    Base m_cache;
    Size m_hits {};
    Size m_misses {};
};

} // namespace calicodb

#endif // CALICO_PAGER_CACHE_H

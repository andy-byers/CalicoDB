/*
 * Very loosely based off of https://medium.com/@koushikmohan/an-analysis-of-2q-cache-replacement-algorithms-21acceae672a
 */

#ifndef CALICO_PAGER_PAGE_CACHE_H
#define CALICO_PAGER_PAGE_CACHE_H

#include "framer.h"
#include "utils/cache.h"
#include "utils/types.h"

namespace calico {

class Frame;

class PageList final {
public:
    using Iterator = std::list<Id>::iterator;

    PageList() = default;
    ~PageList() = default;

    [[nodiscard]]
    auto size() const -> Size
    {
        return m_list.size();
    }

    [[nodiscard]]
    auto begin() -> Iterator
    {
        using std::begin;
        return begin(m_list);
    }

    [[nodiscard]]
    auto end() -> Iterator
    {
        using std::end;
        return end(m_list);
    }

    [[nodiscard]]
    auto insert(const Id &id) -> Iterator
    {
        return m_list.emplace(cend(m_list), id);
    }

    auto remove(Iterator itr) -> Iterator
    {
        return m_list.erase(itr);
    }

private:
    std::list<Id> m_list;
};

class PageRegistry final {
public:
    using DirtyToken = std::optional<PageList::Iterator>;

    struct Entry {
        Size frame_index;
        Id record_lsn {};
        DirtyToken dirty_token {};
    };

    using Cache = cache<Id, Entry, Id::Hash>;
    using Iterator = Cache::iterator;

    PageRegistry() = default;
    ~PageRegistry() = default;

    [[nodiscard]]
    auto is_empty() const -> Size
    {
        return m_cache.is_empty();
    }

    [[nodiscard]]
    auto size() const -> Size
    {
        return m_cache.size();
    }

    [[nodiscard]]
    auto contains(Id pid) const -> bool
    {
        return m_cache.contains(pid);
    }

    [[nodiscard]]
    auto hit_ratio() const -> double
    {
        if (const auto total = static_cast<double>(m_hits + m_misses); total != 0.0)
            return static_cast<double>(m_hits) / total;
        return 0.0;
    }

    [[nodiscard]]
    auto end() -> Iterator
    {
        using std::end;
        return end(m_cache);
    }

    template<class Predicate>
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

    auto put(Id pid, Entry entry) -> void;
    auto get(Id pid) -> Iterator;
    auto erase(Id pid) -> void;

private:
    Cache m_cache;
    Size m_hits {};
    Size m_misses {};
};

} // namespace calico

#endif // CALICO_PAGER_PAGE_CACHE_H

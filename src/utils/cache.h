#ifndef CALICO_UTILS_CACHE_H
#define CALICO_UTILS_CACHE_H

#include "calico/common.h"
#include <list>
#include <optional>
#include <unordered_map>

namespace Calico {

/*
 * Implements the simplified 2Q replacement policy described in
 *     https://arpitbhayani.me/blogs/2q-cache
 *
 * Uses a single std::list-std::unordered_map combination, along with an iterator into
 * the std::list, to represent the two caches. Stores an extra boolean per entry to
 * indicate "hot" status, and inserts new elements at the iterator, which marks
 * std::end(<hot_queue>)/std::begin(<warm_queue>).
 *
 * Iteration order using begin() and end() members reflects the order of importance.
 * That is, if the cache is not empty, *begin() refers to the most-recently-used hot
 * element, and *std::prev(end()) refers to the element that would be evicted in a
 * call to evict(). Also note that the warm queue will be emptied before any
 * elements are evicted from the hot queue.
 *
 * Note that put() has the ability to change the value of an element. This shouldn't
 * ever happen when using this cache as a page cache.
 */
template<
    class K,
    class V,
    class F = std::hash<K>>
class Cache {
public:
    using Key = K;
    using Value = V;
    using Hash = F;

    struct Entry {
        Key key;
        Value value;
        bool hot {};
    };

    using Iterator = typename std::list<Entry>::iterator;
    using ConstIterator = typename std::list<Entry>::const_iterator;
    using ReverseIterator = typename std::list<Entry>::const_reverse_iterator;
    using ConstReverseIterator = typename std::list<Entry>::const_reverse_iterator;

    Cache() = default;

    [[nodiscard]] auto is_empty() const -> bool
    {
        return m_list.empty();
    }

    [[nodiscard]] auto size() const -> std::size_t
    {
        return m_list.size();
    }

    // NOTE: Use this to ask if an entry exists without altering the cache.
    [[nodiscard]] auto contains(const Key &key) const -> bool
    {
        using std::end;

        return query(key) != end(m_list);
    }

    // NOTE: Use this to ask for an entry without altering the cache.
    [[nodiscard]] auto query(const Key &key) const -> ConstIterator
    {
        using std::end;

        if (auto itr = m_map.find(key); itr != end(m_map)) {
            return itr->second;
        }
        return end(m_list);
    }

    [[nodiscard]] auto get(const Key &key) -> Iterator
    {
        using std::end;

        if (auto itr = m_map.find(key); itr != end(m_map)) {
            return promote(itr);
        }
        return end(m_list);
    }

    template<class T>
    auto put(const Key &key, T &&value) -> std::optional<Value>
    {
        using std::end;

        if (auto itr = m_map.find(key); itr != end(m_map)) {
            auto temp = std::exchange(itr->second->value, std::forward<T>(value));
            promote(itr);
            return temp;
        }
        m_sep = m_list.emplace(m_sep, Entry {key, std::forward<T>(value), false});
        m_map.emplace(key, m_sep);
        return std::nullopt;
    }

    auto erase(ConstIterator itr) -> Iterator
    {
        using std::end;

        // NOTE: itr must be a valid iterator.
        if (auto node = m_map.extract(itr->key)) {
            if (m_sep == itr) {
                m_sep = next(m_sep);
            }
            return m_list.erase(node.mapped());
        }
        return end(m_list);
    }

    auto erase(const Key &key) -> bool
    {
        using std::end;

        if (auto itr = m_map.find(key); itr != end(m_map)) {
            if (m_sep == itr->second) {
                m_sep = next(m_sep);
            }
            m_list.erase(itr->second);
            m_map.erase(itr);
            return true;
        }
        return false;
    }

    [[nodiscard]] auto evict() -> std::optional<Entry>
    {
        using std::end;

        if (is_empty()) {
            return std::nullopt;
        }

        // Adjust the separator. If there are no elements in the "warm" queue, we have
        // to evict the LRU element from the "hot" queue. Remember, the cache is not
        // empty (see guard above).
        auto target = prev(end(m_list));

        if (m_sep == target) {
            m_sep = next(target);
        }

        auto entry = std::move(*target);
        m_list.erase(target);
        m_map.erase(entry.key);
        return entry;
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

    [[nodiscard]] auto rbegin() -> ReverseIterator
    {
        using std::rbegin;
        return rbegin(m_list);
    }

    [[nodiscard]] auto rbegin() const -> ConstReverseIterator
    {
        using std::rbegin;
        return rbegin(m_list);
    }

    [[nodiscard]] auto rend() -> ReverseIterator
    {
        using std::rend;
        return rend(m_list);
    }

    [[nodiscard]] auto rend() const -> ConstReverseIterator
    {
        using std::rend;
        return rend(m_list);
    }

    // Need custom copy contructor/copy-assignment operator to figure out where the new
    // iterator should point. I think this can only be done in linear time (std::list's
    // iterator is not random-access), so we'll just disable copying for now.
    Cache(const Cache &) = delete;
    auto operator=(const Cache &) -> Cache & = delete;

private:
    using Map = std::unordered_map<Key, Iterator, Hash>;
    using List = std::list<Entry>;

    // Make an element the most-important element.
    auto promote(typename Map::iterator itr) -> Iterator
    {
        using std::begin;
        auto &[key, value, hot] = *itr->second;

        // If the entry is not hot, then make it hot. Also, if the entry was already hot, then it must
        // not be equal to the separator.
        if (!hot) {
            if (m_sep == itr->second) {
                m_sep = next(m_sep);
            }
            hot = true;
        }
        // Elements always get promoted to the front of the hot queue.
        m_list.splice(begin(m_list), m_list, itr->second);
        itr->second = begin(m_list);
        return itr->second;
    }

    Map m_map;
    List m_list;
    Iterator m_sep {std::end(m_list)};
};

} // namespace Calico

#endif // CALICO_UTILS_CACHE_H
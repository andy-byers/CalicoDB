#ifndef CALICO_UTILS_CACHE_H
#define CALICO_UTILS_CACHE_H

#include "calico/common.h"
#include <functional>
#include <list>
#include <optional>
#include <unordered_map>

namespace calico {

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
    class Key,
    class Value,
    class Hash = std::hash<Key>
>
class cache { // TODO: Eventually switching over to camel_case...
public:
    using key_t = Key;
    using value_t = Value;
    using hash_t = Hash;

    struct entry {
        key_t key;
        value_t value;
        bool hot {};
    };

    // Disallow changing values through iterator instances.
    using iterator = typename std::list<entry>::iterator;
    using const_iterator = typename std::list<entry>::const_iterator;
    using reverse_iterator = typename std::list<entry>::const_reverse_iterator;
    using const_reverse_iterator = typename std::list<entry>::const_reverse_iterator;

    cache() = default;

    [[nodiscard]]
    auto is_empty() const -> bool
    {
        return m_list.empty();
    }

    [[nodiscard]]
    auto size() const -> std::size_t
    {
        return m_list.size();
    }

    // NOTE: Use this to ask if an entry exists without altering the cache.
    [[nodiscard]]
    auto contains(const key_t &key) const -> bool
    {
        using std::end;
        return query(key) != end(m_list);
    }

    // NOTE: Use this to ask for an entry without altering the cache.
    [[nodiscard]]
    auto query(const key_t &key) const -> const_iterator
    {
        using std::end;

        if (auto itr = m_map.find(key); itr != end(m_map))
            return itr->second;
        return end(m_list);
    }

    [[nodiscard]]
    auto get(const key_t &key) -> iterator
    {
        using std::end;

        if (auto itr = m_map.find(key); itr != end(m_map))
            return promote(itr);
        return end(m_list);
    }

    template<class T>
    auto put(const key_t &key, T &&value) -> std::optional<value_t>
    {
        using std::end;

        if (auto itr = m_map.find(key); itr != end(m_map)) {
            auto temp = std::exchange(itr->second->value, std::forward<T>(value));
            promote(itr);
            return temp;
        }
        m_sep = m_list.emplace(m_sep, entry {key, std::forward<T>(value), false});
        m_map.emplace(key, m_sep);
        return std::nullopt;
    }

    auto erase(const_iterator itr) -> iterator
    {
        using std::end;

        // NOTE: itr must be a valid iterator.
        if (auto node = m_map.extract(itr->key)) {
            if (m_sep == itr)
                m_sep = next(m_sep);
            return m_list.erase(node.mapped());
        }
        return end(m_list);
    }

    auto erase(const key_t &key) -> bool
    {
        using std::end;

        if (auto itr = m_map.find(key); itr != end(m_map)) {
            if (m_sep == itr->second)
                m_sep = next(m_sep);
            m_list.erase(itr->second);
            m_map.erase(itr);
            return true;
        }
        return false;
    }

    [[nodiscard]]
    auto evict() -> std::optional<entry>
    {
        using std::end;

        if (is_empty())
            return std::nullopt;

        // Adjust the separator. If there are no elements in the "warm" queue, we have
        // to evict the LRU element from the "hot" queue. Remember, the cache is not
        // empty (see guard above).
        auto target = prev(end(m_list));

        if (m_sep == target)
            m_sep = next(target);

        auto entry = std::move(*target);
        m_list.erase(target);
        m_map.erase(entry.key);
        return entry;
    }

    [[nodiscard]]
    auto begin() -> iterator
    {
        using std::begin;
        return begin(m_list);
    }

    [[nodiscard]]
    auto begin() const -> const_iterator
    {
        using std::begin;
        return begin(m_list);
    }

    [[nodiscard]]
    auto end() -> iterator
    {
        using std::end;
        return end(m_list);
    }

    [[nodiscard]]
    auto end() const -> const_iterator
    {
        using std::end;
        return end(m_list);
    }

    [[nodiscard]]
    auto rbegin() -> reverse_iterator
    {
        using std::rbegin;
        return rbegin(m_list);
    }

    [[nodiscard]]
    auto rbegin() const -> const_reverse_iterator
    {
        using std::rbegin;
        return rbegin(m_list);
    }

    [[nodiscard]]
    auto rend() -> reverse_iterator
    {
        using std::rend;
        return rend(m_list);
    }

    [[nodiscard]]
    auto rend() const -> const_reverse_iterator
    {
        using std::rend;
        return rend(m_list);
    }

    // Need custom copy contructor/copy-assignment operator to figure out where the new
    // iterator should point. I think this can only be done in linear time (std::list's
    // iterator is not random-access), so we'll just disable copying for now.
    cache(const cache &) = delete;
    auto operator=(const cache &) -> cache & = delete;

private:
    using map_t = std::unordered_map<key_t, iterator, hash_t>;
    using list_t = std::list<entry>;

    // Make an element the most-important element.
    auto promote(typename map_t::iterator itr) -> iterator
    {
        using std::begin;
        auto &[key, value, hot] = *itr->second;

        // If the entry is not hot, then make it hot. Also, if the entry was already hot, then it must
        // not be equal to the separator.
        if (!hot) {
            if (m_sep == itr->second)
                m_sep = next(m_sep);
            hot = true;
        }
        // Elements always get promoted to the front of the hot queue.
        m_list.splice(begin(m_list), m_list, itr->second);
        itr->second = begin(m_list);
        return itr->second;
    }

    map_t m_map;
    list_t m_list;
    iterator m_sep {std::end(m_list)};
};

} // namespace calico

#endif // CALICO_UTILS_CACHE_H

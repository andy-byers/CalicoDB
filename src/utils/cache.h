#ifndef CALICO_UTILS_CACHE_H
#define CALICO_UTILS_CACHE_H

#include "calico/common.h"
#include "utils/expect.h"
#include <functional>
#include <list>
#include <optional>
#include <unordered_map>

namespace calico {

namespace impl {

    template<class Key, class Value, class Hash = std::hash<Key>>
    class UniqueCache {
    public:
        using Iterator = typename std::list<std::pair<Key, Value>>::iterator;
        using ConstIterator = typename std::list<std::pair<Key, Value>>::const_iterator;

        UniqueCache() = default;
        virtual ~UniqueCache() = default;

        [[nodiscard]]
        virtual auto is_empty() const -> Size
        {
            return m_map.size() == 0;
        }

        [[nodiscard]]
        virtual auto size() const -> Size
        {
            return m_map.size();
        }

        [[nodiscard]]
        virtual auto contains(const Key &key) const -> bool
        {
            using std::end;
            return m_map.find(key) != end(m_map);
        }

        [[nodiscard]]
        virtual auto get(const Key &key) -> Iterator
        {
            using std::end;
            if (auto itr = m_map.find(key); itr != end(m_map))
                return itr->second;
            return end(m_list);
        }

        [[nodiscard]]
        virtual auto extract(const Key &key) -> std::optional<Value>
        {
            if (auto node = m_map.extract(key)) {
                auto value = std::move(node.mapped()->second);
                m_list.erase(node.mapped());
                return value;
            }
            return std::nullopt;
        }

        virtual auto put(const Key &key, Value &&value) -> void
        {
            // We don't handle duplicate keys. We only need to cache unique identifiers.
            CALICO_EXPECT_FALSE(contains(key));
            m_list.emplace_back(key, std::forward<Value>(value));
            m_map.emplace(key, prev(end()));
        }

        [[nodiscard]]
        virtual auto evict() -> std::optional<Value>
        {
            if (is_empty())
                return std::nullopt;
            auto [key, value] = std::move(m_list.front());
            m_list.pop_front();
            m_map.erase(key);
            return std::move(value);
        }

        [[nodiscard]]
        virtual auto begin() -> Iterator
        {
            using std::begin;
            return begin(m_list);
        }

        [[nodiscard]]
        virtual auto begin() const -> ConstIterator
        {
            using std::cbegin;
            return cbegin(m_list);
        }

        [[nodiscard]]
        virtual auto end() -> Iterator
        {
            using std::end;
            return end(m_list);
        }

        [[nodiscard]]
        virtual auto end() const -> ConstIterator
        {
            using std::cend;
            return cend(m_list);
        }

    protected:
        using List = std::list<std::pair<Key, Value>>;
        using Map = std::unordered_map<Key, typename List::iterator, Hash>;

        List m_list;
        Map m_map;
    };

} // namespace impl

template<class Key, class Value, class Hash = std::hash<Key>>
class UniqueFifoCache final: public impl::UniqueCache<Key, Value, Hash> {
public:
    using typename impl::UniqueCache<Key, Value, Hash>::Iterator;
    using typename impl::UniqueCache<Key, Value, Hash>::ConstIterator;

    UniqueFifoCache() = default;
    ~UniqueFifoCache() override = default;
};

template<class Key, class Value, class Hash = std::hash<Key>>
class UniqueLruCache final: public impl::UniqueCache<Key, Value, Hash> {
public:
    using typename impl::UniqueCache<Key, Value, Hash>::Iterator;
    using typename impl::UniqueCache<Key, Value, Hash>::ConstIterator;

    UniqueLruCache() = default;
    ~UniqueLruCache() override = default;

    [[nodiscard]]
    auto get(const Key &key) -> Iterator override
    {
        using Base = impl::UniqueCache<Key, Value, Hash>;
        if (auto itr = Base::m_map.find(key); itr != end(Base::m_map)) {
            Base::m_list.splice(end(Base::m_list), Base::m_list, itr->second);
            return itr->second;
        }
        return end(Base::m_list);
    }
};

} // namespace cco

#endif // CALICO_UTILS_CACHE_H

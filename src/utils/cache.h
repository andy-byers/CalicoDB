#ifndef CCO_UTILS_CACHE_H
#define CCO_UTILS_CACHE_H

#include "calico/common.h"
#include "utils/expect.h"
#include <functional>
#include <list>
#include <optional>
#include <unordered_map>

namespace cco {

template<class Key, class Value, class Hash = std::hash<Key>>
class FifoCache {
public:
    using Reference = std::reference_wrapper<Value>;
    using Iterator = typename std::list<std::pair<Key, Value>>::iterator;

    FifoCache() = default;
    ~FifoCache() = default;

    [[nodiscard]] auto is_empty() const -> Size
    {
        return m_map.size() == 0;
    }

    [[nodiscard]] auto size() const -> Size
    {
        return m_map.size();
    }

    [[nodiscard]] auto contains(const Key &key) const -> bool
    {
        return m_map.find(key) != std::end(m_map);
    }

    [[nodiscard]] auto get(const Key &key) -> std::optional<Reference>
    {
        if (auto itr = m_map.find(key); itr != std::end(m_map))
            return std::ref(itr->second->second);
        return std::nullopt;
    }

    [[nodiscard]] auto extract(const Key &key) -> std::optional<Value>
    {
        if (auto node = m_map.extract(key)) {
            auto value = std::move(node.mapped()->second);
            m_list.erase(node.mapped());
            return value;
        }
        return std::nullopt;
    }

    auto put(const Key &key, Value &&value) -> std::optional<Value>
    {
        // Currently, we don't handle duplicate keys. We only cache page IDs, which are unique.
        CCO_EXPECT_FALSE(contains(key));
        m_list.emplace_back(key, std::forward<Value>(value));
        m_map.emplace(key, prev(end()));
        return std::nullopt;
    }

    [[nodiscard]] auto evict() -> std::optional<Value>
    {
        if (is_empty())
            return std::nullopt;
        auto [key, value] = std::move(m_list.front());
        m_list.pop_front();
        m_map.erase(key);
        return std::move(value);
    }

    [[nodiscard]] auto begin() -> Iterator
    {
        return std::begin(m_list);
    }

    [[nodiscard]] auto end() -> Iterator
    {
        return std::end(m_list);
    }

protected:
    using List = std::list<std::pair<Key, Value>>;
    using Map = std::unordered_map<Key, typename List::iterator, Hash>;

    List m_list;
    Map m_map;
};

template<class Key, class Value, class Hash = std::hash<Key>>
class LruCache final : public FifoCache<Key, Value, Hash> {
public:
    using Base = FifoCache<Key, Value, Hash>;
    using typename Base::Reference;

    LruCache() = default;
    ~LruCache() = default;

    [[nodiscard]] auto get(const Key &key) -> std::optional<Reference>
    {
        if (auto itr = Base::m_map.find(key); itr != end(Base::m_map)) {
            Base::m_list.splice(end(Base::m_list), Base::m_list, itr->second);
            return std::ref(itr->second->second);
        }
        return std::nullopt;
    }
};

} // namespace cco

#endif // CCO_UTILS_CACHE_H

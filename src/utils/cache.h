#ifndef CALICO_UTILS_CACHE_H
#define CALICO_UTILS_CACHE_H

#include <functional>
#include <list>
#include <unordered_map>
#include "calico/common.h"
#include "utils/expect.h"

namespace calico {

template<class Key, class Value, class Hash = std::hash<Key>>
class FifoCache {
public:
    using Reference = std::reference_wrapper<Value>;

    FifoCache() = default;
    virtual ~FifoCache() = default;

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

    virtual auto put(const Key &key, Value &&value) -> std::optional<Value>
    {
        // Currently, we don't handle duplicate keys. We only cache page IDs, which are unique.
        CALICO_EXPECT_FALSE(contains(key));
        m_list.emplace_back(key, std::forward<Value>(value));
        m_map.emplace(key, std::prev(std::end(m_list)));
        return std::nullopt;
    }

    virtual auto get(const Key &key) -> std::optional<Reference>
    {
        if (auto itr = m_map.find(key); itr != std::end(m_map))
            return std::ref(itr->second->second);
        return std::nullopt;
    }

    virtual auto extract(const Key &key) -> std::optional<Value>
    {
        if (auto node = m_map.extract(key)) {
            auto value = std::move(node.mapped()->second);
            m_list.erase(node.mapped());
            return value;
        }
        return std::nullopt;
    }

    virtual auto evict() -> std::optional<Value>
    {
        if (is_empty())
            return std::nullopt;
        auto [key, value] = std::move(m_list.front());
        m_list.pop_front();
        m_map.erase(key);
        return std::move(value);
    }

protected:
    using List = std::list<std::pair<Key, Value>>;
    using Map = std::unordered_map<Key, typename List::iterator, Hash>;

    List m_list;
    Map m_map;
};

template<class Key, class Value, class Hash = std::hash<Key>>
class LruCache final: public FifoCache<Key, Value, Hash> {
public:
    using Base = FifoCache<Key, Value, Hash>;
    using typename Base::Reference;

    LruCache() = default;
    ~LruCache() override = default;

    auto get(const Key &key) -> std::optional<Reference> override
    {
        auto &m = Base::m_map;
        auto &L = Base::m_list;
        if (auto itr = m.find(key); itr != std::end(m)) {
            L.splice(std::end(L), L, itr->second);
            return std::ref(itr->second->second);
        }
        return std::nullopt;
    }
};

} // calico

#endif // CALICO_UTILS_CACHE_H

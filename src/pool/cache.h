#ifndef CALICO_POOL_PAGE_CACHE_H
#define CALICO_POOL_PAGE_CACHE_H

#include <list>
#include <unordered_map>
#include "utils/identifier.h"

namespace calico {

class Frame;

template<class Key, class Value, class Hash = std::hash<Key>>
class LruCache final {
public:
    using Reference = std::reference_wrapper<Value>;
    using ConstReference = std::reference_wrapper<const Value>;

    LruCache() = default;
    ~LruCache() = default;

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

    [[nodiscard]] auto hit_ratio() const -> double
    {
        if (const auto total = static_cast<double>(m_hit_count + m_miss_count); total != 0.0)
            return static_cast<double>(m_hit_count) / total;
        return 0.0;
    }

    auto put(const Key &key, Value &&value) -> std::optional<Value>
    {
        if (auto itr = m_map.find(key); itr != std::end(m_map)) {
            m_map.erase(itr);
            m_list.erase(itr->second);
        }

        m_list.emplace_back(key, std::forward<Value>(value));
        m_map.emplace(key, std::prev(std::end(m_list)));
        return std::nullopt;
    }

    auto get(const Key &key) -> std::optional<Reference>
    {
        if (auto itr = m_map.find(key); itr != std::end(m_map)) {
            m_list.splice(std::end(m_list), m_list, itr->second);
            return std::ref(itr->second);
        }
        return std::nullopt;
    }

    auto get(const Key &key) const -> std::optional<ConstReference>
    {
        if (auto itr = m_map.find(key); itr != std::end(m_map)) {
            m_list.splice(std::end(m_list), m_list, itr->second);
            return std::cref(itr->second);
        }
        return std::nullopt;
    }

    auto extract(const Key &key) -> std::optional<Value>
    {
        if (auto node = m_map.extract(key)) {
            auto value = std::move(node.mapped()->second);
            m_list.erase(node.mapped());
            m_hit_count++;
            return value;
        }
        m_miss_count++;
        return std::nullopt;
    }

    auto evict() -> std::optional<Value>
    {
        if (is_empty())
            return std::nullopt;
        auto [key, value] = std::move(m_list.front());
        m_list.pop_front();
        m_map.erase(key);
        return std::move(value);
    }

private:
    std::list<std::pair<Key, Value>> m_list;
    std::unordered_map<Key, typename decltype(m_list)::iterator, Hash> m_map;
    Size m_hit_count {};
    Size m_miss_count {};
};

class PageCache final {
public:
    using Reference = std::reference_wrapper<Frame>;
    using ConstReference = std::reference_wrapper<const Frame>;

    PageCache() = default;
    ~PageCache() = default;


    [[nodiscard]] auto is_empty() const -> Size
    {
        return m_entry.is_empty() && m_hot.is_empty();
    }

    [[nodiscard]] auto size() const -> Size
    {
        return m_entry.size() + m_hot.size();
    }

    [[nodiscard]] auto contains(PID id) const -> bool
    {
        return m_entry.contains(id) || m_hot.contains(id);
    }

    [[nodiscard]] auto hit_ratio() const -> double
    {
        return 0.0;
    }

    auto put(PID, Frame) -> std::optional<Frame>;
    auto get(PID) -> std::optional<Reference>;
    auto get(PID) const -> std::optional<ConstReference>;
    auto extract(PID) -> std::optional<Frame>;
    auto evict() -> std::optional<Frame>;

private:
    LruCache<PID, Frame, PID::Hasher> m_entry;
    LruCache<PID, Frame, PID::Hasher> m_hot;
};

//using PageCache = LruCache<PID, Frame, PID::Hasher>;

} // calico

#endif // CALICO_POOL_PAGE_CACHE_H

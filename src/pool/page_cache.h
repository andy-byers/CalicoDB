/*
 * Very loosely based off of https://medium.com/@koushikmohan/an-analysis-of-2q-cache-replacement-algorithms-21acceae672a
 */

#ifndef CCO_POOL_PAGE_CACHE_H
#define CCO_POOL_PAGE_CACHE_H

#include <list>
#include <unordered_map>
#include "frame.h"
#include "utils/cache.h"
#include "utils/identifier.h"

namespace cco {

class Frame;

class PageCache final {
public:
    using Reference = std::reference_wrapper<Frame>;
    using ConstReference = std::reference_wrapper<const Frame>;

    PageCache() = default;
    ~PageCache() = default;

    [[nodiscard]] auto is_empty() const -> Size
    {
        return m_cold.is_empty() && m_hot.is_empty();
    }

    [[nodiscard]] auto size() const -> Size
    {
        return m_cold.size() + m_hot.size();
    }

    [[nodiscard]] auto contains(PID id) const -> bool
    {
        return m_cold.contains(id) || m_hot.contains(id);
    }

    [[nodiscard]] auto hit_ratio() const -> double
    {
        if (const auto total = static_cast<double>(m_hits + m_misses); total != 0.0)
            return static_cast<double>(m_hits) / total;
        return 0.0;
    }

    auto put(PID, Frame) -> void;
    auto get(PID) -> std::optional<Reference>;
    auto extract(PID) -> std::optional<Frame>;
    auto evict() -> std::optional<Frame>;

private:
    utils::FifoCache<PID, Frame, PID::Hasher> m_cold;
    utils::LruCache<PID, Frame, PID::Hasher> m_hot;
    Size m_hits {};
    Size m_misses {};
};

} // calico

#endif // CCO_POOL_PAGE_CACHE_H

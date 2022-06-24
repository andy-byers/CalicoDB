#ifndef CALICO_POOL_PAGE_CACHE_H
#define CALICO_POOL_PAGE_CACHE_H

#include <list>
#include <unordered_map>
#include "utils/cache.h"
#include "utils/identifier.h"

namespace calico {

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
    FifoCache<PID, Frame, PID::Hasher> m_cold;
    LruCache<PID, Frame, PID::Hasher> m_hot;
    Size m_hits {};
    Size m_misses {};
};

} // calico

#endif // CALICO_POOL_PAGE_CACHE_H

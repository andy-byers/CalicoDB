#ifndef CUB_POOL_PAGE_CACHE_H
#define CUB_POOL_PAGE_CACHE_H

#include <list>
#include <unordered_map>
#include "utils/identifier.h"

namespace cub {

class Frame;

class PageCache final {
public:
    PageCache() = default;
    ~PageCache() = default;

    [[nodiscard]] auto is_empty() const -> Size
    {
        return size() == 0;
    }

    [[nodiscard]] auto size() const -> Size
    {
        return m_list.size();
    }

    [[nodiscard]] auto contains(PID id) const -> bool
    {
        return m_map.find(id) != m_map.end();
    }

    [[nodiscard]] auto hit_ratio() const -> double
    {
        if (const auto total = static_cast<double>(m_hit_count + m_miss_count); total != 0.0)
            return static_cast<double>(m_hit_count) / total;
        return 0.0;
    }

    auto put(Frame) -> void;
    auto extract(PID) -> std::optional<Frame>;
    auto evict(LSN) -> std::optional<Frame>;
    auto purge() -> void;

private:
    std::unordered_map<PID, std::list<Frame>::iterator, PID::Hasher> m_map;
    std::list<Frame> m_list;
    Size m_hit_count{};
    Size m_miss_count{};
};

} // cub

#endif // CUB_POOL_PAGE_CACHE_H

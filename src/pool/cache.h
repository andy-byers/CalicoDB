#ifndef CUB_POOL_PAGE_CACHE_H
#define CUB_POOL_PAGE_CACHE_H

#include <list>
#include <unordered_map>
#include "common.h"
#include "utils/identifier.h"

namespace cub {

class Frame;

class PageCache {
public:
    PageCache() = default;
    ~PageCache() = default;
    [[nodiscard]] auto hit_ratio() const -> double;
    [[nodiscard]] auto is_empty() const -> Size;
    [[nodiscard]] auto size() const -> Size;
    [[nodiscard]] auto contains(PID id) const -> bool;
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

#ifndef CUB_UTILS_UTILS_H
#define CUB_UTILS_UTILS_H

#include <filesystem>
#include <string>
#include "common.h"

namespace cub {

static constexpr Size PAGE_ID_SIZE {sizeof(uint32_t)};
static constexpr Size CELL_POINTER_SIZE {sizeof(uint16_t)};
static constexpr Index NULL_ID_VALUE {0};
static constexpr Index ROOT_ID_VALUE {1};

enum class PageType: uint16_t {
    NULL_PAGE     = 0x0000,
    INTERNAL_NODE = 0x494E, // "IN"
    EXTERNAL_NODE = 0x4558, // "EX"
    OVERFLOW_LINK = 0x4F56, // "OV"
    FREELIST_LINK = 0x4652, // "FR"
};

inline auto is_page_type_valid(PageType type) -> bool
{
    return type == PageType::INTERNAL_NODE ||
           type == PageType::EXTERNAL_NODE ||
           type == PageType::OVERFLOW_LINK ||
           type == PageType::FREELIST_LINK;
}

// Source: http://graphics.stanford.edu/~seander/bithacks.html#DetermineIfPowerOf2
template<class T> auto is_power_of_two(T v) noexcept -> bool
{
    return v && !(v & (v-1));
}

inline auto get_wal_path(const std::string &path) -> std::string
{
    std::filesystem::path full {path};
    return full.parent_path() / std::filesystem::path {"." + full.filename().string() + ".wal"};
}

} // cub

#endif // CUB_UTILS_UTILS_H

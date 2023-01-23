#ifndef CALICO_UTILS_LAYOUT_H
#define CALICO_UTILS_LAYOUT_H

#include "header.h"
#include "types.h"
#include "utils.h"

namespace Calico {

class PageLayout {
public:
    static constexpr Size LSN_OFFSET {0};
    static constexpr Size TYPE_OFFSET {8};
    static constexpr Size HEADER_SIZE {10};

    static constexpr auto header_offset(Id page_id) noexcept -> Size
    {
        return page_id.is_root() * sizeof(FileHeader);
    }

    static constexpr auto content_offset(Id page_id) noexcept -> Size
    {
        return header_offset(page_id) + HEADER_SIZE;
    }
};

class NodeLayout {
public:
    static constexpr Size PARENT_ID_OFFSET {0};
    static constexpr Size RIGHTMOST_CHILD_ID_OFFSET {8};
    static constexpr Size RIGHT_SIBLING_ID_OFFSET {8};
    static constexpr Size RESERVED_OFFSET {16};
    static constexpr Size LEFT_SIBLING_ID_OFFSET {16};
    static constexpr Size CELL_COUNT_OFFSET {24};
    static constexpr Size CELL_START_OFFSET {26};
    static constexpr Size FREE_START_OFFSET {28};
    static constexpr Size FRAG_TOTAL_OFFSET {30};
    static constexpr Size FREE_TOTAL_OFFSET {32};
    static constexpr Size HEADER_SIZE {34};

    static constexpr auto header_offset(Id page_id) noexcept -> Size
    {
        return PageLayout::content_offset(page_id);
    }

    static constexpr auto content_offset(Id page_id) noexcept -> Size
    {
        return header_offset(page_id) + HEADER_SIZE;
    }
};

class LinkLayout {
public:
    static constexpr Size NEXT_ID_OFFSET {0};
    static constexpr Size HEADER_SIZE {8};

    static constexpr auto header_offset() noexcept -> Size
    {
        // The root page can never become a link page, so this value is the same for
        // all pages.
        const Id non_root {Id::root().value + 1};
        return PageLayout::content_offset(non_root);
    }

    static constexpr auto content_offset() noexcept -> Size
    {
        return header_offset() + HEADER_SIZE;
    }
};

inline constexpr auto get_min_local(Size page_size) -> Size
{
    CALICO_EXPECT_TRUE(is_power_of_two(page_size));
    // NOTE: This computation was adapted from a similar one in SQLite3.
    return (page_size - PageLayout::HEADER_SIZE - NodeLayout::HEADER_SIZE) * 32 / 256 -
           MAX_CELL_HEADER_SIZE - CELL_POINTER_SIZE;
}

inline constexpr auto get_max_local(Size page_size) -> Size
{
    CALICO_EXPECT_TRUE(is_power_of_two(page_size));
    // NOTE: This computation was adapted from a similar one in SQLite3.
    return (page_size - PageLayout::HEADER_SIZE - NodeLayout::HEADER_SIZE) * 64 / 256 -
           MAX_CELL_HEADER_SIZE - CELL_POINTER_SIZE;
}

// TODO: Get rid of this one.
inline constexpr auto get_local_value_size(Size key_size, Size value_size, Size page_size) -> Size
{
    CALICO_EXPECT_GT(key_size, 0);
    CALICO_EXPECT_TRUE(is_power_of_two(page_size));

    if (const auto total = key_size + value_size; total > get_max_local(page_size)) {
        const auto nonlocal_value_size = total - std::max(key_size, get_min_local(page_size));
        return value_size - nonlocal_value_size;
    }
    return value_size;
}

inline constexpr auto get_local_value_size(Size key_size, Size value_size, Size min_local, Size max_local) -> Size
{
    CALICO_EXPECT_GT(key_size, 0);

    if (const auto total = key_size + value_size; total > max_local) {
        const auto nonlocal_value_size = total - std::max(key_size, min_local);
        return value_size - nonlocal_value_size;
    }
    return value_size;
}

} // namespace Calico

#endif // CALICO_UTILS_LAYOUT_H

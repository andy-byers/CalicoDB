#ifndef CCO_UTILS_LAYOUT_H
#define CCO_UTILS_LAYOUT_H

#include "identifier.h"
#include "utils.h"

namespace cco {

class FileLayout {
public:
    static constexpr Size MAGIC_CODE_OFFSET {0};
    static constexpr Size HEADER_CRC_OFFSET {4};
    static constexpr Size PAGE_COUNT_OFFSET {8};
    static constexpr Size FREE_START_OFFSET {12};
    static constexpr Size PAGE_SIZE_OFFSET {16};
    static constexpr Size KEY_COUNT_OFFSET {20};
    static constexpr Size FLUSHED_LSN_OFFSET {24};
    static constexpr Size HEADER_SIZE {48};

    static constexpr auto header_offset() noexcept -> Index
    {
        return 0;
    }

    static constexpr auto content_offset() noexcept -> Index
    {
        return header_offset() + HEADER_SIZE;
    }

    static constexpr auto page_offset(PageId page_id, Size page_size) noexcept -> Index
    {
        return page_id.as_index() * page_size;
    }
};

class PageLayout {
public:
    static constexpr Size LSN_OFFSET {0};
    static constexpr Size TYPE_OFFSET {4};
    static constexpr Size HEADER_SIZE {6};

    static constexpr auto header_offset(PageId page_id) noexcept -> Size
    {
        return page_id.is_base() * FileLayout::HEADER_SIZE;
    }

    static constexpr auto content_offset(PageId page_id) noexcept -> Size
    {
        return header_offset(page_id) + HEADER_SIZE;
    }
};

class NodeLayout {
public:
    static constexpr Size PARENT_ID_OFFSET {0};
    static constexpr Size RIGHTMOST_CHILD_ID_OFFSET {4};
    static constexpr Size RIGHT_SIBLING_ID_OFFSET {4};
    static constexpr Size RESERVED_OFFSET {8};
    static constexpr Size LEFT_SIBLING_ID_OFFSET {8};
    static constexpr Size CELL_COUNT_OFFSET {12};
    static constexpr Size CELL_START_OFFSET {14};
    static constexpr Size FREE_START_OFFSET {16};
    static constexpr Size FRAG_TOTAL_OFFSET {18};
    static constexpr Size FREE_TOTAL_OFFSET {20};
    static constexpr Size HEADER_SIZE {22};

    static constexpr auto header_offset(PageId page_id) noexcept -> Size
    {
        return PageLayout::content_offset(page_id);
    }

    static constexpr auto content_offset(PageId page_id) noexcept -> Size
    {
        return header_offset(page_id) + HEADER_SIZE;
    }
};

class LinkLayout {
public:
    static constexpr Size NEXT_ID_OFFSET {0};
    static constexpr Size HEADER_SIZE {4};

    static constexpr auto header_offset() noexcept -> Size
    {
        // The root page can never become a link page, so this value is the same for
        // all pages.
        const PageId non_root {ROOT_ID_VALUE + 1};
        CCO_EXPECT_FALSE(non_root.is_base());
        return PageLayout::content_offset(non_root);
    }

    static constexpr auto content_offset() noexcept -> Size
    {
        return header_offset() + HEADER_SIZE;
    }
};

inline constexpr auto get_min_local(Size page_size)
{
    CCO_EXPECT_TRUE(is_power_of_two(page_size));
    // NOTE: This computation was adapted from a similar one in SQLite3.
    return (page_size - PageLayout::HEADER_SIZE - NodeLayout::HEADER_SIZE) * 32 / 256 -
           MAX_CELL_HEADER_SIZE - CELL_POINTER_SIZE;
}

inline constexpr auto get_max_local(Size page_size)
{
    CCO_EXPECT_TRUE(is_power_of_two(page_size));
    // NOTE: This computation was adapted from a similar one in SQLite3.
    return (page_size - PageLayout::HEADER_SIZE - NodeLayout::HEADER_SIZE) * 64 / 256 -
           MAX_CELL_HEADER_SIZE - CELL_POINTER_SIZE;
}

inline constexpr auto get_local_value_size(Size key_size, Size value_size, Size page_size) -> Size
{
    CCO_EXPECT_GT(key_size, 0);
    CCO_EXPECT_TRUE(is_power_of_two(page_size));

    /* Cases:
         *              Byte 0     min_local(...)  get_max_local(...)
         *                   |                  |               |
         *                   |                  |               |
         *                   v                  v               v
         *     (1)  ::H::::: ::K::::::: ::V::::::::::::::::::::::
         *     (2)  ::H::::: ::K::::::::::::::::::::::: ::V::::::
         *     (3)  ::H::::: ::K::::::: ::V::::::**************************
         *     (4)  ::H::::: ::K::::::::::::::::::::::::::::::::: **V******
         *     (5)  ::H::::: ::K::::::::::::::::::::::: **V****************
         *
         * Everything shown as a '*' is stored on an overflow page.
         *
         * In (1) and (2), the entire value is stored in the cell. In (3), (4), and (5), part of V is
         * written to an overflow page. In (3), V is truncated such that the local payload is min_local(...)
         * in length. In (4) and (5), we try to truncate the local payload to get_min_local(...), but we never
         * remove any of the key.
        */
    if (const auto total = key_size + value_size; total > get_max_local(page_size)) {
        const auto nonlocal_value_size = total - std::max(key_size, get_min_local(page_size));
        return value_size - nonlocal_value_size;
    }
    return value_size;
}

} // namespace cco

#endif // CCO_UTILS_LAYOUT_H

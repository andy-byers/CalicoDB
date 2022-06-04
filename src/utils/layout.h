#ifndef CUB_UTILS_LAYOUT_H
#define CUB_UTILS_LAYOUT_H

#include "assert.h"
#include "common.h"
#include "identifier.h"
#include "utils.h"

namespace cub {

/*
 * File Header Layout:
 *     .--------------------.-------.--------.
 *     | Field Name         | Size  | Offset |
 *     :--------------------:-------:--------:
 *     | magic_code         | 4     | 0      |
 *     | header_crc         | 4     | 4      |
 *     | page_count         | 4     | 8      |
 *     | node_count         | 4     | 12     |
 *     | free_count         | 4     | 16     |
 *     | free_start         | 4     | 20     |
 *     | page_size          | 2     | 24     |
 *     | block_size         | 2     | 26     |
 *     | key_count          | 4     | 28     |
 *     | flushed_lsn        | 4     | 32     |
 *     '--------------------'-------'--------'
 */

class FileLayout {
public:
    static const Size MAGIC_CODE_OFFSET = 0;
    static const Size HEADER_CRC_OFFSET = 4;
    static const Size PAGE_COUNT_OFFSET = 8;
    static const Size NODE_COUNT_OFFSET = 12;
    static const Size FREE_COUNT_OFFSET = 16;
    static const Size FREE_START_OFFSET = 20;
    static const Size PAGE_SIZE_OFFSET = 24;
    static const Size BLOCK_SIZE_OFFSET = 26;
    static const Size KEY_COUNT_OFFSET = 28;
    static const Size FLUSHED_LSN_OFFSET = 32;
    static const Size HEADER_SIZE = 36;

    static auto header_offset() noexcept -> Index
    {
        return 0;
    }

    static auto content_offset() noexcept -> Index
    {
        return header_offset() + HEADER_SIZE;
    }

    static auto page_offset(PID page_id, Size page_size) noexcept -> Index
    {
        return page_id.as_index() * page_size;
    }
};

/*
 * Root Page Headers Layout:
 *          .-------------------------------------------------.
 *          | 00 01 02 03 04 05 06 07 08 09 0A 0B 0C 0D 0E 0F |
 *     .----:-------------------------------------------------:
 *     | 00 | File Header ----------------------------------> |
 *     | 10 | ----------------------------------------------X |
 *     | 20 | Page Header ----------X Node Header ----------> |
 *     | 30 | ----------------------------------X .. .. .. .. |
 *     '----'-------------------------------------------------'
 *
 * Non-Root Page Headers Layout:
 *          .-------------------------------------------------.
 *          | 00 01 02 03 04 05 06 07 08 09 0A 0B 0C 0D 0E 0F |
 *     .----:-------------------------------------------------:
 *     | 00 | Page Header ----------X Node Header ----------> |
 *     | 10 | ----------------------------------X .. .. .. .. |
 *     '----'-------------------------------------------------'
 */

/*
 * Page Header Layout:
 *     .--------------------.-------.--------.
 *     | Field Name         | Size  | Offset |
 *     :--------------------:-------:--------:
 *     | lsn                | 4     | 0      |
 *     | type               | 2     | 4      |
 *     '--------------------'-------'--------'
 */

class PageLayout {
public:
    static const Size LSN_OFFSET = 0;
    static const Size TYPE_OFFSET = 4;
    static const Size HEADER_SIZE = 6;

    static auto header_offset(PID page_id) noexcept -> Size
    {
        return page_id.is_root() * FileLayout::HEADER_SIZE;
    }

    static auto content_offset(PID page_id) noexcept -> Size
    {
        return header_offset(page_id) + HEADER_SIZE;
    }
};

/*
 * Node Header Layout:
 *     .--------------------.-------.--------.
 *     | Field Name         | Size  | Offset |
 *     :--------------------:-------:--------:
 *     | header_crc         | 4     | 0      |
 *     | parent_id          | 4     | 4      |
 *     | rightmost_child_id | 4     | 8      |
 *     | right_sibling_id¹  | 4     | 8      |
 *     | cell_count (n)     | 2     | 12     |
 *     | free_count         | 2     | 14     |
 *     | cell_start         | 2     | 16     |
 *     | free_start         | 2     | 18     |
 *     | frag_count         | 2     | 20     |
 *     :--------------------:-------:--------:
 *     | cell_pointers      | n * 2 | 22     |
 *     '--------------------'-------'--------'
 *
 *     ¹ rightmost_child_id and right_sibling_id refer to the same data location. One should access rightmost_child_id
 *       in internal nodes and right_sibling_id in external nodes.
 */

class NodeLayout {
public:
    static const Size HEADER_CRC_OFFSET = 0;
    static const Size PARENT_ID_OFFSET = 4;
    static const Size RIGHTMOST_CHILD_ID_OFFSET = 8;
    static const Size RIGHT_SIBLING_ID_OFFSET = 8;
    static const Size CELL_COUNT_OFFSET = 12;
    static const Size FREE_COUNT_OFFSET = 14;
    static const Size CELL_START_OFFSET = 16;
    static const Size FREE_START_OFFSET = 18;
    static const Size FRAG_COUNT_OFFSET = 20;
    static const Size HEADER_SIZE = 22;

    static auto header_offset(PID page_id) noexcept -> Size
    {
        return PageLayout::content_offset(page_id);
    }

    static auto content_offset(PID page_id) noexcept -> Size
    {
        return header_offset(page_id) + HEADER_SIZE;
    }
};

/*
 * Link Header Layout:
 *     .--------------------.-------.--------.
 *     | Field Name         | Size  | Offset |
 *     :--------------------:-------:--------:
 *     | next_id            | 4     | 0      |
 *     '--------------------'-------'--------'
 */

class LinkLayout {
public:
    static const Size NEXT_ID_OFFSET = 0;
    static const Size HEADER_SIZE = 4;

    static auto header_offset() noexcept -> Size
    {
        // The root page can never become a link page, so this value is the same for
        // all pages.
        const PID non_root{2};
        CUB_EXPECT_FALSE(non_root.is_root());
        return PageLayout::content_offset(non_root);
    }

    static auto content_offset() noexcept -> Size
    {
        return header_offset() + HEADER_SIZE;
    }
};


inline constexpr auto get_min_local(Size page_size)
{
    CUB_EXPECT_TRUE(is_power_of_two(page_size));
    // NOTE: This computation was adapted from a similar one in SQLite3.
    return (page_size - PageLayout::HEADER_SIZE - NodeLayout::HEADER_SIZE) * 32 / 256 -
           MAX_CELL_HEADER_SIZE - CELL_POINTER_SIZE;
}

inline constexpr auto get_max_local(Size page_size)
{
    CUB_EXPECT_TRUE(is_power_of_two(page_size));
    // NOTE: This computation was adapted from a similar one in SQLite3.
    return (page_size - PageLayout::HEADER_SIZE - NodeLayout::HEADER_SIZE) * 64 / 256 -
           MAX_CELL_HEADER_SIZE - CELL_POINTER_SIZE;
}

inline auto get_local_value_size(Size key_size, Size value_size, Size page_size) -> Size
{
    CUB_EXPECT_GT(key_size, 0);
    CUB_EXPECT_TRUE(is_power_of_two(page_size));

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

} // cub

#endif // CUB_UTILS_LAYOUT_H

#ifndef CUB_UTILS_LAYOUT_H
#define CUB_UTILS_LAYOUT_H

#include "assert.h"
#include "common.h"
#include "utils.h"

namespace cub {

static constexpr Size PAGE_ID_SIZE {sizeof(uint32_t)};
static constexpr Size CELL_POINTER_SIZE {sizeof(uint16_t)};
static constexpr Index NULL_ID_VALUE {0};
static constexpr Index ROOT_ID_VALUE {1};

struct IdentifierHash;

struct Identifier {
    using Hasher = IdentifierHash;

    Identifier() noexcept = default;

    template<class Id> explicit Identifier(Id id) noexcept
        : value{static_cast<uint32_t>(id)}
    {
        using Unsigned = std::make_unsigned_t<Id>;
        CUB_EXPECT_BOUNDED_BY(uint32_t, static_cast<Unsigned>(id));
    }

    auto operator==(const Identifier &rhs) const noexcept -> bool
    {
        return value == rhs.value;
    }

    auto operator!=(const Identifier &rhs) const noexcept -> bool
    {
        return value != rhs.value;
    }

    [[nodiscard]] auto is_null() const noexcept -> bool
    {
        return value == 0;
    }

    [[nodiscard]] auto is_root() const noexcept -> bool
    {
        return value == ROOT_ID_VALUE;
    }

    [[nodiscard]] auto as_index() const noexcept -> Index
    {
        CUB_EXPECT_GE(value, ROOT_ID_VALUE);
        return value - ROOT_ID_VALUE;
    }

    uint32_t value{};
};

struct IdentifierHash {
    auto operator()(const Identifier &id) const -> size_t
    {
        return std::hash<uint32_t>{}(id.value);
    }
};

struct PID final: public Identifier {
    PID() noexcept = default;

    template<class T> explicit PID(T id) noexcept
        : Identifier{id} {}

    static auto null() noexcept -> PID
    {
        return PID{NULL_ID_VALUE};
    }

    static auto root() noexcept -> PID
    {
        return PID{ROOT_ID_VALUE};
    }
};

struct LSN final: public Identifier {
    LSN() noexcept = default;

    template<class T> explicit LSN(T id) noexcept
        : Identifier{id} {}

    static auto null() noexcept -> LSN
    {
        return LSN{NULL_ID_VALUE};
    }

    static auto base() noexcept -> LSN
    {
        return LSN{ROOT_ID_VALUE};
    }
};


/*
 * File Header Layout:
 *     .--------------------.-------.--------.
 *     | Field Name         | Size  | Offset |
 *     :--------------------:-------:--------:
 *     | magic_code         | 4     | 0      |
 *     | page_count         | 4     | 4      |
 *     | node_count         | 4     | 8      |
 *     | free_count         | 4     | 12     |
 *     | free_start         | 4     | 16     |
 *     | cub_page_size          | 2     | 20     |
 *     | wal_page_size         | 2     | 22     |
 *     | key_count          | 4     | 24     |
 *     | flushed_lsn        | 4     | 28     |
 *     '--------------------'-------'--------'
 */

class FileLayout {
public:
    static const Size MAGIC_CODE_OFFSET = 0;
    static const Size PAGE_COUNT_OFFSET = 4;
    static const Size NODE_COUNT_OFFSET = 8;
    static const Size FREE_COUNT_OFFSET = 12;
    static const Size FREE_START_OFFSET = 16;
    static const Size PAGE_SIZE_OFFSET = 20;
    static const Size BLOCK_SIZE_OFFSET = 22;
    static const Size KEY_COUNT_OFFSET = 24;
    static const Size FLUSHED_LSN_OFFSET = 28;
    static const Size HEADER_SIZE = 32;

    static auto header_offset() noexcept -> Index
    {
        return 0UL;
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
 * Node Header Layout:
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
 *     | parent_id          | 4     | 0      |
 *     | rightmost_child_id | 4     | 4      |
 *     | right_sibling_id¹  | 4     | 4      |
 *     | cell_count (n)     | 2     | 8      |
 *     | free_count         | 2     | 10     |
 *     | cell_start         | 2     | 12     |
 *     | free_start         | 2     | 14     |
 *     | frag_count         | 2     | 16     |
 *     :--------------------:-------:--------:
 *     | cell_pointers      | n * 2 | 20     |
 *     '--------------------'-------'--------'
 *
 *     ¹ rightmost_child_id and right_sibling_id refer to the same data location. One should access rightmost_child_id
 *       in internal nodes and right_sibling_id in external nodes.
 */

class NodeLayout {
public:
    static const Size PARENT_ID_OFFSET = 0;
    static const Size RIGHTMOST_CHILD_ID_OFFSET = 4;
    static const Size RIGHT_SIBLING_ID_OFFSET = 4;
    static const Size CELL_COUNT_OFFSET = 8;
    static const Size FREE_COUNT_OFFSET = 10;
    static const Size CELL_START_OFFSET = 12;
    static const Size FREE_START_OFFSET = 14;
    static const Size FRAG_COUNT_OFFSET = 16;
    static const Size HEADER_SIZE = 18;

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


} // db

#endif // CUB_UTILS_LAYOUT_H

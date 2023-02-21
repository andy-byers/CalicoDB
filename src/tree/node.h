#ifndef CALICO_TREE_NODE_H
#define CALICO_TREE_NODE_H

#include "header.h"
#include "pager/page.h"
#include "utils/types.h"
#include <optional>

namespace Calico {

struct Node;

static constexpr Size MAX_CELL_HEADER_SIZE =
    sizeof(std::uint64_t) + // Value size  (varint)
    sizeof(std::uint64_t) + // Key size    (varint)
    sizeof(Id);             // Overflow ID (8 B)

inline constexpr auto compute_min_local(Size page_size) -> Size
{
    CALICO_EXPECT_TRUE(is_power_of_two(page_size));
    // NOTE: This computation was adapted from a similar one in SQLite3.
    return (page_size - NodeHeader::SIZE) * 32 / 256 -
           MAX_CELL_HEADER_SIZE - sizeof(PageSize);
}

inline constexpr auto compute_max_local(Size page_size) -> Size
{
    CALICO_EXPECT_TRUE(is_power_of_two(page_size));
    // NOTE: This computation was adapted from a similar one in SQLite3.
    return (page_size - NodeHeader::SIZE) * 64 / 256 -
           MAX_CELL_HEADER_SIZE - sizeof(PageSize);
}

inline constexpr auto compute_local_size(Size key_size, Size value_size, Size min_local, Size max_local) -> Size
{
    if (key_size + value_size <= max_local) {
        return key_size + value_size;
    } else if (key_size > max_local) {
        return max_local;
    }
    return std::max(min_local, key_size);
}

/* Internal Cell Format:
 *     Size    Name
 *     8       child_id
 *     varint  key_size
 *     n       key
 *     8       [overflow_id]
 *
 * External Cell Format:
 *     Size    Name
 *     varint  value_size
 *     varint  key_size
 *     n       key
 *     m       value
 *     8       [overflow_id]
 */
struct Cell {
    Byte *ptr {};
    Byte *key {};
    Size local_size {};
    Size key_size {};
    Size size {};
    bool is_free {};
    bool has_remote {};
};

struct NodeMeta {
    using CellSize = Size (*)(const NodeMeta &, const Byte *);
    using ParseCell = Cell (*)(const NodeMeta &, Byte *);

    CellSize cell_size {};
    ParseCell parse_cell {};
    Size min_local {};
    Size max_local {};
};

auto internal_cell_size(const NodeMeta &meta, const Byte *data) -> Size;
auto external_cell_size(const NodeMeta &meta, const Byte *data) -> Size;
auto parse_internal_cell(const NodeMeta &meta, Byte *data) -> Cell;
auto parse_external_cell(const NodeMeta &meta, Byte *data) -> Cell;

struct Node {
    explicit Node(Page inner, Byte *defragmentation_space);
    [[nodiscard]] auto take() && -> Page;

    Node(Node &&rhs) noexcept = default;
    auto operator=(Node &&) noexcept -> Node & = default;

    [[nodiscard]] auto get_slot(Size index) const -> Size;
    auto set_slot(Size index, Size pointer) -> void;
    auto insert_slot(Size index, Size pointer) -> void;
    auto remove_slot(Size index) -> void;

    auto TEST_validate() -> void;

    Page page;
    Byte *scratch {};
    const NodeMeta *meta {};
    NodeHeader header;
    std::optional<Cell> overflow;
    PageSize overflow_index {};
    PageSize slots_offset {};
    PageSize gap_size {};
};

/*
 * Determine the amount of usable space remaining in the node.
 */
[[nodiscard]] auto usable_space(const Node &node) -> Size;

/*
 * Read a cell from the node at the specified index or offset. The node must remain alive for as long as the cell.
 */
[[nodiscard]] auto read_cell_at(Node &node, Size offset) -> Cell;
[[nodiscard]] auto read_cell(Node &node, Size index) -> Cell;

/*
 * Write a cell to the node at the specified index. May defragment the node. The cell must be of the same
 * type as the node, or if the node is internal, promote_cell() must have been called on the cell.
 */
auto write_cell(Node &node, Size index, const Cell &cell) -> Size;

/*
 * Erase a cell from the node at the specified index.
 */
auto erase_cell(Node &node, Size index) -> void;
auto erase_cell(Node &node, Size index, Size size_hint) -> void;

/*
 * Manually defragment the node. Collects all cells at the end of the page with no room in-between (adds the
 * intra-node freelist and fragments back to the "gap").
 */
auto manual_defragment(Node &node) -> void;

/*
 * Helpers for constructing a cell in an external node. emplace_cell() will write an overflow ID if one is provided. It is
 * up to the caller to determine if one is needed, allocate it, and provide its value here.
 */
[[nodiscard]] auto allocate_block(Node &node, std::uint16_t index, std::uint16_t size) -> Size;
auto emplace_cell(Byte *out, Size key_size, Size value_size, const Slice &local_key, const Slice &local_value, Id overflow_id = Id::null()) -> Byte *;

/*
 * Write the cell into backing memory and update its pointers.
 */
auto detach_cell(Cell &cell, Byte *backing) -> void;

[[nodiscard]] auto read_child_id_at(const Node &node, Size offset) -> Id;
auto write_child_id_at(Node &node, Size offset, Id child_id) -> void;

[[nodiscard]] auto read_child_id(const Node &node, Size index) -> Id;
[[nodiscard]] auto read_child_id(const Cell &cell) -> Id;
[[nodiscard]] auto read_overflow_id(const Cell &cell) -> Id;
auto write_child_id(Node &node, Size index, Id child_id) -> void;
auto write_child_id(Cell &cell, Id child_id) -> void;
auto write_overflow_id(Cell &cell, Id overflow_id) -> void;

auto merge_root(Node &root, Node &child) -> void;

} // namespace Calico

#endif // CALICO_TREE_NODE_H

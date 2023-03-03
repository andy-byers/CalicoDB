#ifndef CALICODB_NODE_H
#define CALICODB_NODE_H

#include "header.h"
#include "page.h"
#include <optional>

namespace calicodb
{

struct Node;

static constexpr std::size_t kMaxCellHeaderSize =
    sizeof(std::uint64_t) + // Value size  (varint)
    sizeof(std::uint64_t) + // Key size    (varint)
    sizeof(Id);             // Overflow ID (8 B)

inline constexpr auto compute_min_local(std::size_t page_size) -> std::size_t
{
    CDB_EXPECT_TRUE(is_power_of_two(page_size));
    // NOTE: This computation was adapted from a similar one in SQLite3.
    return (page_size - NodeHeader::kSize) * 32 / 256 -
           kMaxCellHeaderSize - sizeof(PageSize);
}

inline constexpr auto compute_max_local(std::size_t page_size) -> std::size_t
{
    CDB_EXPECT_TRUE(is_power_of_two(page_size));
    // NOTE: This computation was adapted from a similar one in SQLite3.
    return (page_size - NodeHeader::kSize) * 64 / 256 -
           kMaxCellHeaderSize - sizeof(PageSize);
}

inline constexpr auto compute_local_size(std::size_t key_size, std::size_t value_size, std::size_t min_local, std::size_t max_local) -> std::size_t
{
    if (key_size + value_size <= max_local) {
        return key_size + value_size;
    } else if (key_size > max_local) {
        return max_local;
    }
    // Try to prevent the key from being split.
    return std::max(min_local, key_size);
}

/* Internal Cell Format:
 *     std::size_t    Name
 *    -----------------------
 *     8       child_id
 *     varint  key_size
 *     n       key
 *     8       [overflow_id]
 *
 * External Cell Format:
 *     std::size_t    Name
 *    -----------------------
 *     varint  value_size
 *     varint  key_size
 *     n       key
 *     m       value
 *     8       [overflow_id]
 */
struct Cell {
    char *ptr {};
    char *key {};
    std::size_t local_size {};
    std::size_t key_size {};
    std::size_t size {};
    bool is_free {};
    bool has_remote {};
};

struct NodeMeta {
    using CellSize = std::size_t (*)(const NodeMeta &, const char *);
    using ParseCell = Cell (*)(const NodeMeta &, char *);

    CellSize cell_size {};
    ParseCell parse_cell {};
    std::size_t min_local {};
    std::size_t max_local {};
};

auto internal_cell_size(const NodeMeta &meta, const char *data) -> std::size_t;
auto external_cell_size(const NodeMeta &meta, const char *data) -> std::size_t;
auto parse_internal_cell(const NodeMeta &meta, char *data) -> Cell;
auto parse_external_cell(const NodeMeta &meta, char *data) -> Cell;

struct Node {
    Node() = default;
    auto initialize() -> void;
    [[nodiscard]] auto take() && -> Page;

    Node(Node &&rhs) noexcept = default;
    auto operator=(Node &&) noexcept -> Node & = default;

    [[nodiscard]] auto get_slot(std::size_t index) const -> std::size_t;
    auto set_slot(std::size_t index, std::size_t pointer) -> void;
    auto insert_slot(std::size_t index, std::size_t pointer) -> void;
    auto remove_slot(std::size_t index) -> void;

    auto TEST_validate() -> void;

    Page page;
    char *scratch {};
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
[[nodiscard]] auto usable_space(const Node &node) -> std::size_t;

/*
 * Read a cell from the node at the specified index or offset. The node must remain alive for as long as the cell.
 */
[[nodiscard]] auto read_cell_at(Node &node, std::size_t offset) -> Cell;
[[nodiscard]] auto read_cell(Node &node, std::size_t index) -> Cell;

/*
 * Write a cell to the node at the specified index. May defragment the node. The cell must be of the same
 * type as the node, or if the node is internal, promote_cell() must have been called on the cell.
 */
auto write_cell(Node &node, std::size_t index, const Cell &cell) -> std::size_t;

/*
 * Erase a cell from the node at the specified index.
 */
auto erase_cell(Node &node, std::size_t index) -> void;
auto erase_cell(Node &node, std::size_t index, std::size_t size_hint) -> void;

/*
 * Manually defragment the node. Collects all cells at the end of the page with no room in-between (adds the
 * intra-node freelist and fragments back to the "gap").
 */
auto manual_defragment(Node &node) -> void;

/*
 * Helpers for constructing a cell in an external node. emplace_cell() will write an overflow ID if one is provided. It is
 * up to the caller to determine if one is needed, allocate it, and provide its value here.
 */
[[nodiscard]] auto allocate_block(Node &node, std::uint16_t index, std::uint16_t size) -> std::size_t;
auto emplace_cell(char *out, std::size_t key_size, std::size_t value_size, const Slice &local_key, const Slice &local_value, Id overflow_id = Id::null()) -> char *;

/*
 * Write the cell into backing memory and update its pointers.
 */
auto detach_cell(Cell &cell, char *backing) -> void;

[[nodiscard]] auto read_child_id_at(const Node &node, std::size_t offset) -> Id;
auto write_child_id_at(Node &node, std::size_t offset, Id child_id) -> void;

[[nodiscard]] auto read_child_id(const Node &node, std::size_t index) -> Id;
[[nodiscard]] auto read_child_id(const Cell &cell) -> Id;
[[nodiscard]] auto read_overflow_id(const Cell &cell) -> Id;
auto write_child_id(Node &node, std::size_t index, Id child_id) -> void;
auto write_child_id(Cell &cell, Id child_id) -> void;
auto write_overflow_id(Cell &cell, Id overflow_id) -> void;

auto merge_root(Node &root, Node &child) -> void;

} // namespace calicodb

#endif // CALICODB_NODE_H

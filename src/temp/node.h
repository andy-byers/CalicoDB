#ifndef CALICO_TREE_NODE_H
#define CALICO_TREE_NODE_H

#include <optional>
#include "header.h"
#include "page.h"
#include "utils/types.h"

namespace Calico {

struct Node_;

inline constexpr auto compute_min_local(Size page_size) -> Size
{
    CALICO_EXPECT_TRUE(is_power_of_two(page_size));
    // NOTE: This computation was adapted from a similar one in SQLite3.
    return (page_size - NodeHeader_::SIZE) * 32 / 256 -
           MAX_CELL_HEADER_SIZE - CELL_POINTER_SIZE;
}

inline constexpr auto compute_max_local(Size page_size) -> Size
{
    CALICO_EXPECT_TRUE(is_power_of_two(page_size));
    // NOTE: This computation was adapted from a similar one in SQLite3.
    return (page_size - NodeHeader_::SIZE) * 64 / 256 -
           MAX_CELL_HEADER_SIZE - CELL_POINTER_SIZE;
}

/* Internal Cell Format:
 *     Offset  Size  Name
 *     0       8     child_id
 *     8       2     key_size (n)
 *     10      n     key
 *
 * External Cell Format:
 *     Offset  Size  Name
 *     0       4     value_size (m)
 *     4       2     key_size (n)
 *     6       n     key
 *     6+n     m     value
 *     6+n+m   8     [overflow_id]
 */
struct Cell_ {
    Byte *ptr {};
    Byte *key {};
    Size total_ps {};
    Size local_ps {};
    Size key_size {};
    Size size {};
    bool is_free {};
};

struct NodeMeta {
    using ReadKey = Slice (*)(const Byte *);
    using CellSize = Size (*)(const NodeMeta &, const Byte *);
    using ParseCell = Cell_ (*)(const NodeMeta &, Byte *);

    ReadKey read_key {};
    CellSize cell_size {};
    ParseCell parse_cell {};
    Size min_local {};
    Size max_local {};
};

auto internal_cell_size(const NodeMeta &meta, const Byte *data) -> Size;
auto external_cell_size(const NodeMeta &meta, const Byte *data) -> Size;
auto parse_internal_cell(const NodeMeta &meta, Byte *data) -> Cell_;
auto parse_external_cell(const NodeMeta &meta, Byte *data) -> Cell_;
auto read_internal_key(const Byte *data) -> Slice;
auto read_external_key(const Byte *data) -> Slice;

struct Node_ {
    explicit Node_(Page inner, Byte *defragmentation_space);
    [[nodiscard]] auto take() && -> Page;

    /*
     * Construct for searching for a key within a node.
     */
    class Iterator {
        Node_ *m_node {};
        unsigned m_index {};

    public:
        explicit Iterator(Node_ &node);
        [[nodiscard]] auto is_valid() const -> bool;
        [[nodiscard]] auto index() const -> Size;
        [[nodiscard]] auto key() const -> Slice;
        [[nodiscard]] auto data() -> Byte *;
        [[nodiscard]] auto data() const -> const Byte *;
        [[nodiscard]] auto seek(const Slice &key) -> bool;
        auto next() -> void;
    };

    Node_(Node_ &&rhs) noexcept = default;
    auto operator=(Node_ &&) noexcept -> Node_ & = default;

    Page page;

    Byte *scratch {};
    const NodeMeta *meta {};
    NodeHeader_ header;

    std::optional<Cell_> overflow;
    std::uint16_t overflow_index {};
    std::uint16_t slots_offset {};
    std::uint16_t gap_size {};

    [[nodiscard]] auto get_slot(Size index) const -> Size;
    auto set_slot(Size index, Size pointer) -> void;
    auto insert_slot(Size index, Size pointer) -> void;
    auto remove_slot(Size index) -> void;

    [[nodiscard]]
    auto cell_size(Size offset) const -> Size
    {
        return meta->cell_size(*meta, page.data() + offset);
    }

    [[nodiscard]]
    auto parse_cell(Size offset) -> Cell_
    {
        return meta->parse_cell(*meta, page.data() + offset);
    }

    [[nodiscard]]
    auto read_key(Size offset) const -> Slice
    {
        return meta->read_key(page.data() + offset);
    }

    [[nodiscard]]
    auto read_key(const Cell_ &cell) const -> Slice
    {
        return {cell.key, cell.key_size};
    }
};

/*
 * Determine the amount of usable space remaining in the node.
 */
[[nodiscard]] auto usable_space(const Node_ &node) -> bool;

/*
 * Read a cell from the node at the specified index. The node must remain alive for as long as the cell.
 */
[[nodiscard]] auto read_cell(Node_ &node, Size index) -> Cell_;

/*
 * Write a cell to the node at the specified index. May defragment the node. The cell must be of the same
 * type as the node, or if the node is internal, promote_cell() must have been called on the cell.
 */
auto write_cell(Node_ &node, Size index, const Cell_ &cell) -> void;

/*
 * Erase a cell from the node at the specified index.
 */
auto erase_cell(Node_ &node, Size index) -> void;
auto erase_cell(Node_ &node, Size index, Size size_hint) -> void;

/*
 * Manually defragment the node. Collects all cells at the end of the page with no room in-between (adds the
 * intra-node freelist and fragments back to the "gap").
 */
auto manual_defragment(Node_ &node) -> void;

/*
 * Helpers for constructing a cell in an external node.
 */
[[nodiscard]] auto allocate_block(Node_ &node, std::uint16_t index, std::uint16_t size) -> Size;
[[nodiscard]] auto determine_cell_size(Size key_size, Size &value_size, const NodeMeta &meta) -> Size;
auto emplace_cell(Byte *out, Size value_size, const Slice &key, const Slice &local_value, Id overflow_id = Id::null()) -> void;

/*
 * If an external cell that requires promotion is written into scratch memory, it should be written at an offset
 * of this many bytes from the start.
 */
static constexpr Size EXTERNAL_SHIFT {4};

/*
 * Prepare a cell embedded in an external node for transfer into an internal node.
 */
auto promote_cell(Cell_ &cell) -> void;

/*
 * Write the cell into backing memory and update its pointers.
 */
auto detach_cell(Cell_ &cell, Byte *backing) -> void;

[[nodiscard]] auto read_child_id(const Node_ &node, Size index) -> Id;
[[nodiscard]] auto read_child_id(const Cell_ &cell) -> Id;
[[nodiscard]] auto read_overflow_id(const Cell_ &cell) -> Id;
auto write_child_id(Node_ &node, Size index, Id child_id) -> void;
auto write_child_id(Cell_ &cell, Id child_id) -> void;

} // namespace Calico

#endif // CALICO_TREE_NODE_H

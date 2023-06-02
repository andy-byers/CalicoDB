// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_TREE_H
#define CALICODB_TREE_H

#include "calicodb/cursor.h"
#include "header.h"
#include "page.h"
#include "pager.h"
#include <optional>

namespace calicodb
{

class CursorImpl;
class Schema;
class Tree;
struct Node;

// Maintains a list of available memory regions within a node, called the free
// block list, as well as the "gap" space between the cell with the lowest offset
// and the end of the cell pointer array.
//
// Free blocks take the form (offset, size), where "offset" and "size" are 16-bit
// unsigned integers. The entries are kept sorted by the "offset" field, and
// adjacent regions are merged if possible. Free blocks must be at least 4 bytes.
// If a region smaller than 4 bytes is released, it is considered a fragment, and
// its size is added to the "frag_count" node header field. Luckily, it is possible
// to clean up some of these fragments as regions are merged (see release()).
class BlockAllocator
{
public:
    static auto accumulate_free_bytes(const Node &node) -> std::size_t;

    // Allocate "needed_size" bytes of contiguous memory in "node" and return the
    // offset of the first byte, relative to the start of the page.
    static auto allocate(Node &node, std::size_t needed_size) -> std::size_t;

    // Free "block_size" bytes of contiguous memory in "node" starting at
    // "block_start".
    static auto release(Node &node, std::size_t block_start, std::size_t block_size) -> void;

    // Merge all free blocks and fragment bytes into the gap space.
    static auto defragment(Node &node, int skip = -1) -> void;
};

// Internal Cell Format:
//     Size    Name
//    -----------------------
//     4       child_id
//     varint  key_size
//     n       key
//     4       [overflow_id]
//
// External Cell Format:
//     Size    Name
//    -----------------------
//     varint  value_size
//     varint  key_size
//     n       key
//     m       value
//     4       [overflow_id]
//
struct Cell {
    char *ptr = nullptr;
    char *key = nullptr;
    std::size_t local_size = 0;
    std::size_t total_size = 0;
    std::size_t key_size = 0;
    std::size_t size = 0;
    bool is_free = false;
    bool has_remote = false;
};

struct NodeMeta {
    using ParseCell = Cell (*)(char *, const char *);

    ParseCell parse_cell = nullptr;
};

struct Node {
    Node() = default;
    [[nodiscard]] auto take() && -> Page;

    Node(Node &&rhs) = default;
    auto operator=(Node &&) -> Node & = default;

    [[nodiscard]] auto get_slot(std::size_t index) const -> std::size_t;
    auto set_slot(std::size_t index, std::size_t pointer) -> void;
    auto insert_slot(std::size_t index, std::size_t pointer) -> void;
    auto remove_slot(std::size_t index) -> void;

    auto TEST_validate() -> void;

    Page page;
    char *scratch = nullptr;
    const NodeMeta *meta = nullptr;
    NodeHeader header;
    std::optional<Cell> overflow;
    unsigned overflow_index = 0;
    unsigned slots_offset = 0;
    unsigned gap_size = 0;
};

// Read a cell from the node at the specified index or offset. The node must remain alive for as long as the cell.
[[nodiscard]] auto read_cell(Node &node, std::size_t index) -> Cell;

// Write a cell to the node at the specified index. May defragment the node. The cell must be of the same
// type as the node, or if the node is internal, promote_cell() must have been called on the cell.
auto write_cell(Node &node, std::size_t index, const Cell &cell) -> std::size_t;

// Erase a cell from the node at the specified index.
auto erase_cell(Node &node, std::size_t index) -> void;

struct PayloadManager {
    [[nodiscard]] static auto promote(Pager &pager, char *scratch, Cell &cell, Id parent_id) -> Status;
    [[nodiscard]] static auto access(Pager &pager, const Cell &cell, std::size_t offset,
                                     std::size_t length, const char *in_buf, char *out_buf) -> Status;
};

class InternalCursor
{
    friend class Tree;

    mutable Status m_status;
    std::string m_buffer;
    Tree *m_tree;

    // TODO: Cache overflow page IDs so Tree::read_value() doesn't have to read pages that have already
    //       been read by Tree::read_key() (if the key overflowed).
    // std::vector<Id> m_ovfl_cache;

    Node m_node;
    bool m_write = false;

public:
    static constexpr std::size_t kMaxDepth = 20;

    struct Location {
        Id page_id;
        unsigned index = 0;
    } history[kMaxDepth];
    int level = 0;

    explicit InternalCursor(Tree &tree)
        : m_status(Status::not_found()),
          m_tree(&tree)
    {
    }

    ~InternalCursor();

    [[nodiscard]] auto is_valid() const -> bool
    {
        return m_status.is_ok();
    }

    [[nodiscard]] auto status() const -> Status
    {
        return m_status;
    }

    [[nodiscard]] auto index() const -> std::size_t
    {
        CALICODB_EXPECT_TRUE(is_valid());
        return history[level].index;
    }

    [[nodiscard]] auto node() -> Node &
    {
        CALICODB_EXPECT_TRUE(is_valid());
        return m_node;
    }

    [[nodiscard]] auto take() -> Node
    {
        CALICODB_EXPECT_TRUE(is_valid());
        m_status = Status::not_found();
        return std::move(m_node);
    }

    auto move_to(Node node, int diff) -> void
    {
        clear();
        history[level += diff] = {node.page.id(), 0};
        m_node = std::move(node);
        m_status = Status::ok();
    }

    auto clear() -> void;
    auto seek_root(bool write) -> void;
    auto seek(const Slice &key) -> bool;
    auto move_down(Id child_id) -> void;
};

class CursorImpl : public Cursor
{
    mutable Status m_status;
    mutable Node m_node;
    mutable bool m_is_valid = false;
    Tree *m_tree;
    std::string m_key;
    std::string m_value;
    std::size_t m_key_size = 0;
    std::size_t m_value_size = 0;
    std::size_t m_index = 0;

protected:
    auto seek_to(Node node, std::size_t index) -> void;
    auto fetch_payload(Node &node, std::size_t index) -> Status;

public:
    friend class Tree;
    friend class SchemaCursor;

    explicit CursorImpl(Tree &tree)
        : m_tree(&tree)
    {
    }

    ~CursorImpl() override;

    [[nodiscard]] auto is_valid() const -> bool override
    {
        return m_is_valid && m_status.is_ok();
    }

    [[nodiscard]] auto status() const -> Status override
    {
        return m_status;
    }

    [[nodiscard]] auto key() const -> Slice override;
    [[nodiscard]] auto value() const -> Slice override;
    auto seek(const Slice &key) -> void override;
    auto seek_first() -> void override;
    auto seek_last() -> void override;
    auto next() -> void override;
    auto previous() -> void override;

    auto clear(Status s = Status::ok()) -> void;
};

class Tree final
{
public:
    explicit Tree(Pager &pager, const Id *root_id);
    [[nodiscard]] static auto create(Pager &pager, bool is_root, Id *out) -> Status;
    [[nodiscard]] static auto destroy(Tree &tree) -> Status;
    [[nodiscard]] auto put(const Slice &key, const Slice &value) -> Status;
    [[nodiscard]] auto get(const Slice &key, std::string *value) const -> Status;
    [[nodiscard]] auto erase(const Slice &key) -> Status;
    [[nodiscard]] auto vacuum(Schema &schema) -> Status;
    [[nodiscard]] auto allocate(bool is_external, Node &out) -> Status;
    [[nodiscard]] auto acquire(Id page_id, bool upgrade, Node &out) const -> Status;
    [[nodiscard]] auto free(Node node) -> Status;
    auto upgrade(Node &node) const -> void;
    auto release(Node node) const -> void;

    [[nodiscard]] auto TEST_to_string() const -> std::string;
    auto TEST_validate() const -> void;

    [[nodiscard]] auto root() const -> Id
    {
        return m_root_id ? *m_root_id : Id::root();
    }

    auto close_internal_cursor() -> void
    {
        return m_cursor.clear();
    }

    enum StatType {
        kStatRead,
        kStatWrite,
        kStatSMOCount,
        kStatTypeCount
    };

    using Stats = StatCounters<kStatTypeCount>;

    [[nodiscard]] auto statistics() const -> const Stats &
    {
        return m_stats;
    }

private:
    friend class CursorImpl;
    friend class InternalCursor;
    friend class SchemaCursor;
    friend class DBImpl;
    friend class Schema;
    friend class TreeValidator;

    [[nodiscard]] auto free_overflow(Id head_id) -> Status;
    [[nodiscard]] auto read_key(Node &node, std::size_t index, std::string &scratch, Slice *key_out, std::size_t limit = 0) const -> Status;
    [[nodiscard]] auto read_value(Node &node, std::size_t index, std::string &scratch, Slice *value_out) const -> Status;
    [[nodiscard]] auto write_key(Node &node, std::size_t index, const Slice &key) -> Status;
    [[nodiscard]] auto write_value(Node &node, std::size_t index, const Slice &value) -> Status;
    [[nodiscard]] auto emplace(Node &node, const Slice &key, const Slice &value, std::size_t index, bool &overflow) -> Status;
    [[nodiscard]] auto destroy_impl(Node node) -> Status;
    [[nodiscard]] auto vacuum_step(Page &free, PointerMap::Entry entry, Schema &schema, Id last_id) -> Status;
    [[nodiscard]] auto resolve_overflow() -> Status;
    [[nodiscard]] auto resolve_underflow() -> Status;
    [[nodiscard]] auto split_root() -> Status;
    [[nodiscard]] auto split_nonroot() -> Status;
    [[nodiscard]] auto split_nonroot_fast(Node parent, Node right, const Cell &overflow) -> Status;
    [[nodiscard]] auto fix_root() -> Status;
    [[nodiscard]] auto fix_nonroot(Node parent, std::size_t index) -> Status;
    [[nodiscard]] auto merge_left(Node &left, Node right, Node &parent, std::size_t index) -> Status;
    [[nodiscard]] auto merge_right(Node &left, Node right, Node &parent, std::size_t index) -> Status;
    [[nodiscard]] auto rotate_left(Node &parent, Node &left, Node &right, std::size_t index) -> Status;
    [[nodiscard]] auto rotate_right(Node &parent, Node &left, Node &right, std::size_t index) -> Status;
    [[nodiscard]] auto transfer_left(Node &left, Node &right) -> Status;

    [[nodiscard]] auto find_highest(Node &node) const -> Status;
    [[nodiscard]] auto find_lowest(Node &node) const -> Status;
    [[nodiscard]] auto find_external(const Slice &key, bool write, bool &exact) const -> Status;
    [[nodiscard]] auto insert_cell(Node &node, std::size_t index, const Cell &cell) -> Status;
    [[nodiscard]] auto remove_cell(Node &node, std::size_t index) -> Status;
    [[nodiscard]] auto find_parent_id(Id page_id, Id &out) const -> Status;
    [[nodiscard]] auto fix_parent_id(Id page_id, Id parent_id, PointerMap::Type type) -> Status;
    [[nodiscard]] auto maybe_fix_overflow_chain(const Cell &cell, Id parent_id) -> Status;
    [[nodiscard]] auto fix_links(Node &node, Id parent_id = Id::null()) -> Status;
    [[nodiscard]] auto cell_scratch() -> char *;

    auto report_stats(StatType type, std::size_t increment) const -> void;

    mutable Stats m_stats;
    mutable std::string m_node_scratch;
    mutable std::string m_cell_scratch;
    Pager *m_pager = nullptr;
    mutable InternalCursor m_cursor;
    const Id *m_root_id = nullptr;
};

} // namespace calicodb

#endif // CALICODB_TREE_H

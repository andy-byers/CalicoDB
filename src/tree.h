// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_TREE_H
#define CALICODB_TREE_H

#include "calicodb/cursor.h"
#include "header.h"
#include "pager.h"
#include <optional>

namespace calicodb
{

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

    // Merge all free blocks and fragment bytes into the gap space
    // Returns 0 on success, -1 on failure. This routine might fail if the page has been corrupted.
    static auto defragment(Node &node, int skip = -1) -> int;
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
    bool has_remote = false;
};

// Simple construct representing a tree node
struct Node {
    PageRef *ref = nullptr;
    char *scratch = nullptr;

    // If ref is not nullptr, then the following fields can be accessed.
    NodeHdr hdr;
    int (*parser)(char *, const char *, Cell *) = nullptr;
    std::optional<Cell> overflow;
    unsigned overflow_index = 0;
    unsigned slots_offset = 0;
    unsigned gap_size = 0;

    auto TEST_validate() -> void;
};

class CursorImpl : public Cursor
{
    mutable Status m_status;
    mutable Node m_node;
    mutable bool m_is_valid = false;
    Tree *m_tree;
    std::string m_key_buf;
    std::string m_val_buf;
    Slice m_key;
    Slice m_val;
    std::size_t m_index = 0;

protected:
    auto seek_to(Node &node, std::size_t index) -> void;
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

    [[nodiscard]] auto key() const -> Slice override
    {
        CALICODB_EXPECT_TRUE(is_valid());
        return m_key;
    }

    [[nodiscard]] auto value() const -> Slice override
    {
        CALICODB_EXPECT_TRUE(is_valid());
        return m_val;
    }

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
    ~Tree()
    {
        // Ensure that all page references are released back to the pager.
        finish_operation();
    }

    explicit Tree(Pager &pager, char *scratch, const Id *root_id);
    static auto create(Pager &pager, Id *out) -> Status;
    static auto destroy(Tree &tree) -> Status;

    auto put(const Slice &key, const Slice &value) -> Status;
    auto get(const Slice &key, std::string *value) const -> Status;
    auto erase(const Slice &key) -> Status;
    auto vacuum(Schema &schema) -> Status;

    auto allocate(bool is_external, Node &node) -> Status;
    auto acquire(Id page_id, bool write, Node &node) const -> Status;
    auto free(Node &node) -> Status;
    auto upgrade(Node &node) const -> void;
    auto release(Node &node) const -> void;

    auto wset_allocate(bool is_external, Node *&node_out) -> Status;
    auto wset_acquire(Id page_id, bool upgrade, Node *&node_out) const -> Status;
    auto advance_cursor(Node &node, int diff) const -> void;
    auto clear_working_set() const -> void;
    auto finish_operation() const -> void;

    [[nodiscard]] auto TEST_to_string() const -> std::string;
    auto TEST_validate() const -> void;

    [[nodiscard]] auto root() const -> Id
    {
        return m_root_id ? *m_root_id : Id::root();
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
    class InternalCursor
    {
        mutable Status m_status;
        std::string m_buffer;
        Tree *m_tree;
        Node *m_node;

    public:
        static constexpr std::size_t kMaxDepth = 20;

        struct Location {
            Id page_id;
            unsigned index = 0;
        } history[kMaxDepth];
        int level = 0;

        explicit InternalCursor(Tree &tree);
        ~InternalCursor();

        [[nodiscard]] auto is_valid() const -> bool
        {
            return m_status.is_ok() && m_node->ref != nullptr;
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
            return *m_node;
        }

        auto move_to(Node &node, int diff) -> void
        {
            clear();
            history[level += diff] = {node.ref->page_id, 0};
            m_status = Status::ok();
            *m_node = node;
            node.ref = nullptr;
        }

        auto clear() -> void;
        auto seek_root() -> void;
        auto seek(const Slice &key) -> bool;
        auto move_down(Id child_id) -> void;
    };

    friend class CursorImpl;
    friend class SchemaCursor;
    friend class DBImpl;
    friend class Schema;
    friend class TreeValidator;

    // Find the external node containing the lowest/highest key in the tree
    auto find_lowest(Node &node_out) const -> Status;
    auto find_highest(Node &node_out) const -> Status;

    // Move the internal cursor to the external node containing the given `key`
    auto find_external(const Slice &key, bool &exact) const -> Status;

    auto resolve_overflow() -> Status;
    auto split_root() -> Status;
    auto split_nonroot() -> Status;
    auto split_nonroot_fast(Node &parent, Node &right, const Cell &overflow) -> Status;

    auto resolve_underflow() -> Status;
    auto fix_root() -> Status;
    auto fix_nonroot(Node &parent, std::size_t index) -> Status;
    auto merge_left(Node &left, Node &right, Node &parent, std::size_t index) -> Status;
    auto merge_right(Node &left, Node &right, Node &parent, std::size_t index) -> Status;
    auto rotate_left(Node &parent, Node &left, Node &right, std::size_t index) -> Status;
    auto rotate_right(Node &parent, Node &left, Node &right, std::size_t index) -> Status;

    auto read_key(const Cell &cell, std::string &scratch, Slice *key_out, std::size_t limit = 0) const -> Status;
    auto read_value(const Cell &cell, std::string &scratch, Slice *value_out) const -> Status;
    auto read_key(Node &node, std::size_t index, std::string &scratch, Slice *key_out, std::size_t limit = 0) const -> Status;
    auto read_value(Node &node, std::size_t index, std::string &scratch, Slice *value_out) const -> Status;
    auto write_key(Node &node, std::size_t index, const Slice &key) -> Status;
    auto write_value(Node &node, std::size_t index, const Slice &value) -> Status;

    auto emplace(Node &node, const Slice &key, const Slice &value, std::size_t index, bool &overflow) -> Status;
    auto free_overflow(Id head_id) -> Status;
    auto destroy_impl(Node &node) -> Status;
    auto vacuum_step(PageRef &free, PointerMap::Entry entry, Schema &schema, Id last_id) -> Status;
    auto transfer_left(Node &left, Node &right) -> Status;

    auto insert_cell(Node &node, std::size_t index, const Cell &cell) -> Status;
    auto remove_cell(Node &node, std::size_t index) -> Status;
    auto find_parent_id(Id page_id, Id &out) const -> Status;
    auto fix_parent_id(Id page_id, Id parent_id, PointerMap::Type type) -> Status;
    auto maybe_fix_overflow_chain(const Cell &cell, Id parent_id) -> Status;
    auto fix_links(Node &node, Id parent_id = Id::null()) -> Status;
    [[nodiscard]] auto cell_scratch() -> char *;

    [[nodiscard]] auto wset_node(std::size_t idx) const -> Node &
    {
        return m_wset[idx + 1];
    }

    // Storage for the current working set of nodes, that is, nodes that are being accessed by the current
    // tree operation. The tree algorithms are designed so that they never need more than kMaxWorkingSetSize
    // nodes at once. The internal cursor uses slot 0.
    static constexpr std::size_t kMaxWorkingSetSize = 5;
    mutable Node m_wset[kMaxWorkingSetSize + 1];
    mutable std::size_t m_wset_idx = 0;

    // Various tree operation counts are tracked in this variable.
    mutable Stats m_stats;

    // Internal cursor used to traverse the tree structure.
    mutable InternalCursor m_c;

    // Scratch memory for defragmenting nodes and storing an overflow cell.
    char *const m_node_scratch;
    char *const m_cell_scratch;

    Pager *const m_pager;
    const Id *const m_root_id;
};

} // namespace calicodb

#endif // CALICODB_TREE_H

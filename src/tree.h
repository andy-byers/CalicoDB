// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_TREE_H
#define CALICODB_TREE_H

#include "calicodb/cursor.h"
#include "header.h"
#include "node.h"
#include "pager.h"

namespace calicodb
{

class Schema;
class Tree;
class TreeCursor;

[[nodiscard]] inline auto truncate_suffix(const Slice &lhs, const Slice &rhs) -> Slice
{
    CALICODB_EXPECT_FALSE(rhs.is_empty());
    // If this is true, then 1 of these 2 things must be true:
    //   1. There is some index at which the byte in `rhs` compares greater than the
    //      corresponding byte in `lhs`.
    //   2. `rhs` is longer than `lhs`, but they are otherwise identical.
    CALICODB_EXPECT_LT(lhs, rhs);

    const auto end = std::min(
        lhs.size(), rhs.size());

    std::size_t n = 0;
    for (; n < end; ++n) {
        if (lhs[n] != rhs[n]) {
            break;
        }
    }
    // `lhs` < result <= `rhs`
    return rhs.range(0, n + 1);
}

class Tree final
{
public:
    ~Tree();
    auto release_nodes() const -> void;

    explicit Tree(Pager &pager, Stat &stat, char *scratch, const Id *root_id);
    static auto create(Pager &pager, Id *out) -> Status;
    static auto destroy(Tree &tree) -> Status;
    static auto get_tree(Cursor &c) -> Tree *;

    auto new_cursor() -> Cursor *;
    auto put(const Slice &key, const Slice &value) -> Status;
    auto get(const Slice &key, std::string *value) const -> Status;
    auto erase(const Slice &key) -> Status;
    auto vacuum(Schema &schema) -> Status;

    auto put(Cursor &c, const Slice &key, const Slice &value) -> Status;
    auto erase(Cursor &c) -> Status;

    auto allocate(bool is_external, Node &node_out) -> Status
    {
        PageRef *ref;
        auto s = m_pager->allocate(ref);
        if (s.is_ok()) {
            CALICODB_EXPECT_FALSE(PointerMap::is_map(ref->page_id));
            node_out = Node::from_new_page(*ref, is_external);
        }
        return s;
    }

    auto acquire(Id page_id, Node &node_out, bool write = false) const -> Status
    {
        CALICODB_EXPECT_FALSE(PointerMap::is_map(page_id));

        PageRef *ref;
        auto s = m_pager->acquire(page_id, ref);
        if (s.is_ok()) {
            if (Node::from_existing_page(*ref, node_out)) {
                m_pager->release(ref);
                return corrupted_page(page_id);
            }
            if (write) {
                upgrade(node_out);
            }
        }
        return s;
    }

    auto free(Node node) -> Status
    {
        return m_pager->destroy(node.ref);
    }

    auto upgrade(Node &node) const -> void
    {
        m_pager->mark_dirty(*node.ref);
    }

    auto release(Node node) const -> void
    {
        if (node.ref != nullptr && m_pager->mode() == Pager::kDirty) {
            // If the pager is in kWrite mode and a page is marked dirty, it immediately
            // transitions to kDirty mode. So, if this node is dirty, then the pager must
            // be in kDirty mode (unless there was an error).
            if (0x80 < NodeHdr::get_frag_count(node.hdr())) {
                // Fragment count is too large. Defragment the node to get rid of all fragments.
                if (node.defrag(m_node_scratch)) {
                    // Sets the pager error status.
                    (void)corrupted_page(node.ref->page_id);
                }
            }
        }
        m_pager->release(node.ref);
    }

    [[nodiscard]] auto root() const -> Id
    {
        return m_root_id ? *m_root_id : Id::root();
    }

    [[nodiscard]] auto to_string() const -> std::string;
    auto TEST_validate() const -> void;

private:
    friend class DBImpl;
    friend class InorderTraversal;
    friend class Schema;
    friend class SchemaCursor;
    friend class TreeCursor;
    friend class TreeValidator;
    friend class UserCursor;

    auto put(TreeCursor &c, const Slice &key, const Slice &value) -> Status;
    auto erase(TreeCursor &c) -> Status;

    auto corrupted_page(Id page_id) const -> Status;

    auto redistribute_cells(Node &left, Node &right, Node &parent, U32 pivot_idx) -> Status;
    auto resolve_overflow(TreeCursor &c) -> Status;
    auto split_root(TreeCursor &c) -> Status;
    auto split_nonroot(TreeCursor &c) -> Status;
    auto split_nonroot_fast(TreeCursor &c, Node &parent, Node right) -> Status;
    auto resolve_underflow(TreeCursor &c) -> Status;
    auto fix_root(TreeCursor &c) -> Status;
    auto fix_nonroot(TreeCursor &c, Node &parent, U32 index) -> Status;

    auto read_key(const Cell &cell, std::string &scratch, Slice *key_out, U32 limit = 0) const -> Status;
    auto read_value(const Cell &cell, std::string &scratch, Slice *value_out) const -> Status;
    auto read_key(Node &node, U32 index, std::string &scratch, Slice *key_out, U32 limit = 0) const -> Status;
    auto read_value(Node &node, U32 index, std::string &scratch, Slice *value_out) const -> Status;
    auto write_key(Node &node, U32 index, const Slice &key) -> Status;
    auto write_value(Node &node, U32 index, const Slice &value) -> Status;

    auto emplace(Node &node, const Slice &key, const Slice &value, U32 index, bool &overflow) -> Status;
    auto free_overflow(Id head_id) -> Status;
    auto vacuum_step(PageRef &free, PointerMap::Entry entry, Schema &schema, Id last_id) -> Status;

    struct PivotOptions {
        const Cell *cells[2];
        char *scratch;
        Id parent_id;
    };
    auto make_pivot(const PivotOptions &opt, Cell &pivot_out) -> Status;
    auto post_pivot(Node &node, U32 idx, Cell &cell, Id child_id) -> Status;
    auto insert_cell(Node &node, U32 idx, const Cell &cell) -> Status;
    auto remove_cell(Node &node, U32 idx) -> Status;
    auto find_parent_id(Id page_id, Id &out) const -> Status;
    auto fix_parent_id(Id page_id, Id parent_id, PointerMap::Type type) -> Status;
    auto maybe_fix_overflow_chain(const Cell &cell, Id parent_id) -> Status;
    auto fix_links(Node &node, Id parent_id = Id::null()) -> Status;

    // Internal cursor used to traverse the tree structure
    TreeCursor *const m_cursor;

    auto use_cursor(Cursor *c) const -> void;
    mutable Cursor *m_last_c = nullptr;

    // Various tree operation counts are tracked in this variable.
    Stat *m_stat;

    // When the node pointed at by m_c overflows, store the cell that couldn't fit on the page here. The
    // location that the overflow cell should be placed is copied into the pid and idx fields from m_c.
    // The overflow cell is usually backed by one of the cell scratch buffers.
    struct {
        // Return true if an overflow cell exists, false otherwise
        [[nodiscard]] auto exists() const -> bool
        {
            return !pid.is_null();
        }

        // Discard the overflow cell
        auto clear() -> void
        {
            pid = Id::null();
        }

        Cell cell;
        Id pid;
        U32 idx;
    } m_ovfl;

    // Scratch memory for defragmenting nodes.
    char *const m_node_scratch;

    // Scratch memory for cells that aren't embedded in nodes. Use m_cell_scratch[n] to get a pointer to
    // the start of cell scratch buffer n, where n < kNumCellBuffers.
    static constexpr std::size_t kNumCellBuffers = 4;
    static constexpr auto kCellBufferLen = kPageSize / kNumCellBuffers;
    char *const m_cell_scratch[kNumCellBuffers];

    Pager *const m_pager;
    const Id *const m_root_id;
};

} // namespace calicodb

#endif // CALICODB_TREE_H

// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_TREE_H
#define CALICODB_TREE_H

#include "freelist.h"
#include "header.h"
#include "node.h"
#include "pager.h"
#include "ptr.h"

namespace calicodb
{

class Schema;
class Tree;
class CursorImpl;

[[nodiscard]] inline auto truncate_suffix(const Slice &lhs, const Slice &rhs, Slice &prefix_out) -> int
{
    const auto end = std::min(
        lhs.size(), rhs.size());

    size_t n = 0;
    for (; n < end; ++n) {
        const auto u = static_cast<uint8_t>(lhs[n]);
        const auto v = static_cast<uint8_t>(rhs[n]);
        if (u < v) {
            break;
        } else if (u > v) {
            return -1;
        }
    }
    if (n >= rhs.size()) {
        return -1;
    }
    // `lhs` < result <= `rhs`
    prefix_out = rhs.range(0, n + 1);
    return 0;
}

class Tree final
{
public:
    struct ListEntry {
        Slice name;
        Tree *const tree;
        ListEntry *prev_entry;
        ListEntry *next_entry;
    } list_entry;

    ~Tree();
    auto save_all_cursors() const -> void;

    explicit Tree(Pager &pager, Stat &stat, char *scratch, Id root_id, UniqueBuffer name);
    static auto get_tree(CursorImpl &c) -> Tree *;

    struct Reroot {
        Id before;
        Id after;
    };
    // Must be called on the schema tree.
    auto create(Id *out) -> Status;
    auto destroy(Tree &tree, Reroot &rr) -> Status;

    auto put(CursorImpl &c, const Slice &key, const Slice &value) -> Status;
    auto erase(CursorImpl &c) -> Status;
    auto erase(CursorImpl &c, const Slice &key) -> Status;
    auto vacuum() -> Status;

    enum AllocationType {
        kAllocateAny = Freelist::kRemoveAny,
        kAllocateExact = Freelist::kRemoveExact,
    };
    auto allocate(AllocationType type, Id nearby, PageRef *&page_out) -> Status;

    auto allocate_(bool is_external, Id nearby, Node &node_out) -> Status
    {
        PageRef *ref;
        auto s = allocate(kAllocateAny, nearby, ref);
        if (s.is_ok()) {
            if (ref->refs == 1) {
                CALICODB_EXPECT_FALSE(PointerMap::is_map(ref->page_id));
                node_out = Node::from_new_page(*ref, m_node_scratch, is_external);
            } else {
                m_pager->release(ref);
                s = corrupted_node(ref->page_id);
            }
        }
        return s;
    }

    auto acquire(Id page_id, Node &node_out, bool write = false) const -> Status
    {
        PageRef *ref;
        auto s = m_pager->acquire(page_id, ref);
        if (s.is_ok()) {
            if (Node::from_existing_page(*ref, m_node_scratch, node_out)) {
                m_pager->release(ref);
                return corrupted_node(page_id);
            }
            if (write) {
                upgrade(node_out);
            }
        }
        return s;
    }

    auto upgrade(Node &node) const -> void
    {
        m_pager->mark_dirty(*node.ref);
    }

    auto release(Node node, Pager::ReleaseAction action = Pager::kKeep) const -> void
    {
        m_pager->release(node.ref, action);
    }

    [[nodiscard]] auto root() const -> Id
    {
        return m_root_id;
    }

    auto print_structure(std::string &repr_out) -> Status;
    auto print_nodes(std::string &repr_out) -> Status;
    auto TEST_validate() -> void;

private:
    friend class DBImpl;
    friend class InorderTraversal;
    friend class Schema;
    friend class SchemaCursor;
    friend class CursorImpl;
    friend class TreePrinter;
    friend class TreeValidator;

    auto relocate_page(Node &node, PointerMap::Type type, Id id) -> int;

    auto corrupted_node(Id page_id) const -> Status;

    auto redistribute_cells(Node &left, Node &right, Node &parent, uint32_t pivot_idx) -> Status;
    auto resolve_overflow(CursorImpl &c) -> Status;
    auto split_root(CursorImpl &c) -> Status;
    auto split_nonroot(CursorImpl &c) -> Status;
    auto split_nonroot_fast(CursorImpl &c, Node &parent, Node right) -> Status;
    auto resolve_underflow(CursorImpl &c) -> Status;
    auto fix_root(CursorImpl &c) -> Status;
    auto fix_nonroot(CursorImpl &c, Node &parent, uint32_t index) -> Status;

    struct KeyScratch {
        char *buf;
        size_t len;
    };

    auto extract_key(Node &node, uint32_t index, KeyScratch &scratch, Slice &key_out, uint32_t limit = 0) const -> Status;
    auto extract_key(const Cell &cell, KeyScratch &scratch, Slice &key_out, uint32_t limit = 0) const -> Status;
    auto read_key(const Cell &cell, char *scratch, Slice *key_out, uint32_t limit = 0) const -> Status;
    auto read_value(const Cell &cell, char *scratch, Slice *value_out) const -> Status;
    auto read_value(Node &node, uint32_t index, char *scratch, Slice *value_out) const -> Status;
    auto overwrite_value(const Cell &cell, const Slice &value) -> Status;
    auto emplace(Node &node, const Slice &key, const Slice &value, uint32_t index, bool &overflow) -> Status;
    auto free_overflow(Id head_id) -> Status;

    auto relocate_page(PageRef *&free, PointerMap::Entry entry, Id last_id) -> Status;

    struct PivotOptions {
        const Cell *cells[2];
        char *scratch;
        Id parent_id;
    };
    auto make_pivot(const PivotOptions &opt, Cell &pivot_out) -> Status;
    auto post_pivot(Node &node, uint32_t idx, Cell &cell, Id child_id) -> Status;
    auto insert_cell(Node &node, uint32_t idx, const Cell &cell) -> Status;
    auto remove_cell(Node &node, uint32_t idx) -> Status;
    auto find_parent_id(Id page_id, Id &out) const -> Status;
    auto fix_parent_id(Id page_id, Id parent_id, PointerMap::Type type, Status &s) -> void;
    auto maybe_fix_overflow_chain(const Cell &cell, Id parent_id, Status &s) -> void;
    auto fix_links(Node &node, Id parent_id = Id::null()) -> Status;

    struct CursorEntry {
        CursorImpl *cursor;
        CursorEntry *prev_entry;
        CursorEntry *next_entry;
    };

    mutable CursorEntry m_active_list;
    mutable CursorEntry m_inactive_list;

    enum CursorAction {
        kInitNormal,
        kInitSaveCursors,
    };

    auto manage_cursors(Cursor *c, CursorAction type) const -> void;

    // Various tree operation counts are tracked in this variable.
    Stat *const m_stat;

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
        uint32_t idx;
    } m_ovfl;

    // Scratch buffers for holding overflowing keys. See extract_key().
    KeyScratch m_key_scratch[2] = {};

    // Scratch memory for manipulating nodes.
    char *const m_node_scratch;

    // Scratch memory for cells that aren't embedded in nodes. Use m_cell_scratch[n] to get a pointer to
    // the start of cell scratch buffer n, where n < kNumCellBuffers.
    static constexpr size_t kNumCellBuffers = 4;
    static constexpr auto kCellBufferLen = kPageSize / kNumCellBuffers;
    char *const m_cell_scratch[kNumCellBuffers];

    UniqueBuffer m_name;
    Pager *const m_pager;
    Id m_root_id;
    const bool m_writable;
};

} // namespace calicodb

#endif // CALICODB_TREE_H

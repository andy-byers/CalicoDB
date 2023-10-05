// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_TREE_H
#define CALICODB_TREE_H

#include "freelist.h"
#include "header.h"
#include "node.h"
#include "pager.h"
#include "pointer_map.h"
#include "unique_ptr.h"

namespace calicodb
{

class Schema;
class Tree;
class TreeCursor;

[[nodiscard]] inline auto truncate_suffix(const Slice &lhs, const Slice &rhs, Slice &prefix_out) -> int
{
    const auto end = minval(
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
    n = minval(n + 1, rhs.size());
    // `lhs` <= result <= `rhs`
    prefix_out = rhs.range(0, n);
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

    explicit Tree(Pager &pager, Stats &stat, char *scratch, Id root_id, String name, bool unique);
    static auto get_tree(TreeCursor &c) -> Tree *;

    auto activate_cursor(TreeCursor &target, bool requires_position) const -> void;
    auto deactivate_cursors(TreeCursor *exclude) const -> void;

    struct Reroot {
        Id before;
        Id after;
    };
    // Must be called on the schema tree.
    auto create(Id &root_id_out) -> Status;
    auto destroy(Tree &tree, Reroot &rr) -> Status;

    auto put(TreeCursor &c, const Slice &key, const Slice &value) -> Status;
    auto erase(TreeCursor &c) -> Status;
    auto erase(TreeCursor &c, const Slice &key) -> Status;
    auto vacuum() -> Status;

    enum AllocationType {
        kAllocateAny = Freelist::kRemoveAny,
        kAllocateExact = Freelist::kRemoveExact,
    };
    auto allocate(AllocationType type, Id nearby, PageRef *&page_out) -> Status;

    auto acquire(Id page_id, Node &node_out, bool write = false) const -> Status
    {
        PageRef *ref;
        auto s = m_pager->acquire(page_id, ref);
        if (s.is_ok()) {
            if (Node::from_existing_page(*ref, m_page_size, m_node_scratch, node_out)) {
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

    auto check_integrity() -> Status;

    auto print_structure(String &repr_out) -> Status;
    auto print_nodes(String &repr_out) -> Status;
    auto TEST_validate() -> void;

private:
    friend class DBImpl;
    friend class Schema;
    friend class SchemaCursor;
    friend class TreeCursor;
    friend class TreePrinter;
    friend class TreeValidator;

    auto corrupted_node(Id page_id) const -> Status;

    auto redistribute_cells(Node &left, Node &right, Node &parent, uint32_t pivot_idx) -> Status;
    auto resolve_overflow(TreeCursor &c) -> Status;
    auto split_root(TreeCursor &c) -> Status;
    auto split_nonroot(TreeCursor &c) -> Status;
    auto split_nonroot_fast(TreeCursor &c, Node &parent, Node right) -> Status;
    auto resolve_underflow(TreeCursor &c) -> Status;
    auto fix_root(TreeCursor &c) -> Status;
    auto fix_nonroot(TreeCursor &c, Node &parent, uint32_t index) -> Status;

    struct KeyScratch {
        char *buf;
        size_t len;
    };

    auto extract_key(Node &node, uint32_t index, KeyScratch &scratch, Slice &key_out, uint32_t limit = 0) const -> Status;
    auto extract_key(const Cell &cell, KeyScratch &scratch, Slice &key_out, uint32_t limit = 0) const -> Status;
    auto read_key(const Cell &cell, char *scratch, Slice *key_out, uint32_t limit = 0) const -> Status;
    auto read_value(const Cell &cell, char *scratch, Slice *value_out) const -> Status;
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
        TreeCursor *cursor;
        CursorEntry *prev_entry;
        CursorEntry *next_entry;
    };

    mutable CursorEntry m_active_list;
    mutable CursorEntry m_inactive_list;

    // Various tree operation counts are tracked in this variable.
    Stats *const m_stat;

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
    char *const m_cell_scratch[kNumCellBuffers];

    String m_name;
    Pager *const m_pager;
    Id m_root_id;
    const uint32_t m_page_size;
    const bool m_writable;
    const bool m_unique;
};

class TreeCursor
{
    friend class CursorImpl;
    friend class InorderTraversal;
    friend class Tree;
    friend class TreeValidator;

    // Intrusive list of cursors open on m_tree.
    Tree::CursorEntry m_list_entry = {};

    Tree *const m_tree;
    Status m_status;

    Node m_node;
    Cell m_cell;
    uint32_t m_idx = 0;

    // *_path members are used to track the path taken from the tree's root to the current
    // position.
    static constexpr size_t kMaxDepth = 17 + 1;
    Node m_node_path[kMaxDepth - 1];
    uint32_t m_idx_path[kMaxDepth - 1];
    int m_level = 0;

    Buffer<char> m_key_buf;
    Buffer<char> m_value_buf;
    Slice m_key;
    Slice m_value;

    enum State {
        kInvalidState,
        kSavedState,
        kValidState,
    } m_state = kInvalidState;

    auto save_position() -> void
    {
        if (m_state == kValidState && on_record()) {
            m_state = kSavedState;
        }
        release_nodes(kAllLevels);
    }

    auto activate(bool write) -> void
    {
        m_tree->activate_cursor(*this, write);
    }

    // Seek back to the saved position.
    auto ensure_position_loaded() -> void;

    // When the cursor is being used to read records, this routine is used to move
    // cursors that are one-past-the-end in a leaf node to the first position in
    // the right sibling node.
    auto ensure_correct_leaf() -> void;

    auto seek_to_root() -> void;
    auto search_branch(const Slice &key) -> void;
    auto search_leaf(const Slice &key) -> bool;

    auto start_write() -> void;
    auto start_write(const Slice &key, bool is_erase) -> bool;
    auto finish_write(Status &s) -> void;

    auto read_user_key() -> Status;
    auto read_user_value() -> Status;

    auto perform_comparison(uint32_t index, const Slice &lhs, int &cmp_out) -> int;

public:
    explicit TreeCursor(Tree &tree);
    ~TreeCursor();

    auto reset(const Status &s = Status::ok()) -> void;

    auto handle_split_root(Node child) -> void
    {
        static constexpr auto kNumSlots = ARRAY_SIZE(m_node_path);
        CALICODB_EXPECT_EQ(m_node.ref->page_id, m_tree->root());
        CALICODB_EXPECT_EQ(m_node_path[0].ref, nullptr);             // m_node goes here
        CALICODB_EXPECT_EQ(m_node_path[kNumSlots - 1].ref, nullptr); // Overwrite this slot
        CALICODB_EXPECT_EQ(m_level, 0);
        // Shift the node and index paths to make room for the new level. Nodes are not trivially
        // copiable.
        for (size_t i = 1; i < kNumSlots; ++i) {
            m_node_path[kNumSlots - i] = move(m_node_path[kNumSlots - i - 1]);
        }
        std::memmove(m_idx_path + 1, m_idx_path,
                     (kNumSlots - 1) * sizeof *m_idx_path);
        assign_child(move(child));
        // Root is empty until split_nonroot() is called
        m_idx_path[0] = 0;
    }

    auto handle_merge_root() -> void
    {
        // m_node is the root node. It always belongs at m_node_path[0]. The node in
        // m_node_path[1] was destroyed.
        CALICODB_EXPECT_EQ(m_node.ref->page_id, m_tree->root());
        CALICODB_EXPECT_EQ(m_node_path[0].ref, nullptr); // m_node goes here
        CALICODB_EXPECT_EQ(m_node_path[1].ref, nullptr); // Overwrite this slot
        CALICODB_EXPECT_EQ(m_level, 0);
        static constexpr auto kNumSlots = ARRAY_SIZE(m_node_path);
        for (size_t i = 0; i < kNumSlots - 1; ++i) {
            m_node_path[i] = move(m_node_path[i + 1]);
        }
        std::memmove(m_idx_path, m_idx_path + 1,
                     (kNumSlots - 1) * sizeof *m_idx_path);
        m_idx = m_idx_path[0];
    }

    auto seek_to_leaf(const Slice &key, bool read_payload) -> bool;
    auto seek_to_last_leaf() -> void;
    auto move_right() -> void;
    auto move_left() -> void;
    auto move_to_parent(bool preserve_path) -> void;
    auto move_to_child(Id child_id) -> void;
    auto assign_child(Node child) -> void;

    auto fetch_record_if_valid() -> void;
    auto fetch_user_payload() -> Status;
    [[nodiscard]] auto on_last_node() const -> bool;

    // Return true if the cursor is positioned on a valid node, false otherwise
    [[nodiscard]] auto on_node() const -> bool
    {
        return m_status.is_ok() && m_node.ref != nullptr;
    }

    // Return true if the cursor is positioned on a valid key, false otherwise
    [[nodiscard]] auto on_record() const -> bool
    {
        return on_node() && m_idx < NodeHdr::get_cell_count(m_node.hdr());
    }

    [[nodiscard]] auto page_id() const -> Id
    {
        CALICODB_EXPECT_TRUE(on_node());
        return m_node.ref->page_id;
    }

    enum ReleaseType {
        kCurrentLevel,
        kAllLevels,
    };

    auto release_nodes(ReleaseType type) -> void;
    auto key() const -> Slice;
    auto value() const -> Slice;

    [[nodiscard]] auto handle() -> void *
    {
        return this;
    }

    [[nodiscard]] auto is_valid() const -> bool
    {
        return on_record() || m_state == kSavedState;
    }

    auto status() const -> Status
    {
        return m_status;
    }

    auto assert_state() const -> bool;

    auto TEST_tree() -> Tree &
    {
        return *m_tree;
    }
};

} // namespace calicodb

#endif // CALICODB_TREE_H

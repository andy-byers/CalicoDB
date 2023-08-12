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
    static constexpr size_t kRequiredBufferSize = 3 * kPageSize;

    ~Tree();
    auto save_all_cursors() const -> void;

    explicit Tree(Pager &pager, Stat &stat, char *scratch, const Id *root_id, bool writable);
    static auto create(Pager &pager, Id *out) -> Status;
    static auto destroy(Tree &tree) -> Status;
    static auto get_tree(CursorImpl &c) -> Tree *;

    auto new_cursor() -> Cursor *;
    auto get(CursorImpl &c, const Slice &key, std::string *value) const -> Status;
    auto put(CursorImpl &c, const Slice &key, const Slice &value) -> Status;
    auto erase(CursorImpl &c) -> Status;
    auto erase(CursorImpl &c, const Slice &key) -> Status;
    auto vacuum(Schema &schema) -> Status;

    auto allocate(bool is_external, Node &node_out) -> Status
    {
        PageRef *ref;
        auto s = m_pager->allocate(ref);
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

    auto free(Node node) -> Status
    {
        return m_pager->destroy(node.ref);
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
        return m_root_id ? *m_root_id : Id::root();
    }

    auto print_structure(std::string &repr_out) const -> Status;
    auto print_nodes(std::string &repr_out) const -> Status;

    auto TEST_validate() const -> void;

private:
    friend class DBImpl;
    friend class InorderTraversal;
    friend class Schema;
    friend class SchemaCursor;
    friend class CursorImpl;
    friend class TreePrinter;
    friend class TreeValidator;

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
    auto read_value(Node &node, uint32_t index, std::string &scratch, Slice *value_out) const -> Status;
    auto overwrite_value(const Cell &cell, const Slice &value) -> Status;
    auto emplace(Node &node, const Slice &key, const Slice &value, uint32_t index, bool &overflow) -> Status;
    auto free_overflow(Id head_id) -> Status;

    auto vacuum_step(PageRef *&free, PointerMap::Entry entry, Schema &schema, Id last_id) -> Status;

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

    Pager *const m_pager;
    const Id *const m_root_id;
    const bool m_writable;
};

class CursorImpl : public Cursor
{
    friend class InorderTraversal;
    friend class Tree;
    friend class TreeValidator;

    Tree::CursorEntry m_list_entry = {};

    Tree *const m_tree;
    Status m_status;

    Node m_node;
    uint32_t m_idx = 0;

    // *_path members are used to track the path taken from the tree's root to the current
    // position. At any given time, the elements with indices less than the current level
    // are valid.
    static constexpr size_t kMaxDepth = 17 + 1;
    Node m_node_path[kMaxDepth - 1];
    uint32_t m_idx_path[kMaxDepth - 1];
    int m_level = 0;

    // A heap-allocated buffer used to store the current record. m_key and m_value are
    // slices into this buffer. The buffer length is not stored explicitly, but it is
    // always greater than or equal to the sum of the lengths of m_key and m_value.
    char *m_user_payload = nullptr;
    Slice m_key;
    Slice m_value;
    bool m_saved = false;

    auto save_position() -> void
    {
        CALICODB_EXPECT_TRUE(has_key());
        release_nodes(kAllLevels);
        m_saved = m_status.is_ok();
    }

    // Seek back to the saved position. Does not read the payload.
    auto ensure_position_loaded() -> void
    {
        if (m_saved) {
            seek_to_leaf(m_key, kSeekNormal);
        }
    }

    // When the cursor is being used to read records, this routine is used to move
    // cursors that are one-past-the-end in a leaf node to the first position in
    // the right sibling node.
    auto ensure_correct_leaf(bool read_payload) -> void;

    auto prepare(Tree::CursorAction type) -> void
    {
        m_tree->manage_cursors(this, type);
    }

    auto seek_to_first_leaf() -> void
    {
        seek_to_leaf("", kSeekReader);
    }

    auto seek_to_last_leaf() -> void;

    auto fetch_user_payload() -> Status;
    auto move_to_right_sibling() -> void;
    auto move_to_left_sibling() -> void;
    auto search_node(const Slice &key) -> bool;
    explicit CursorImpl(Tree &tree);

public:
    ~CursorImpl() override;
    auto move_to_parent() -> void;
    auto assign_child(Node child) -> void;
    auto move_to_child(Id child_id) -> void;
    [[nodiscard]] auto on_last_node() const -> bool;

    // Return true if the cursor is positioned on a valid node, false otherwise
    [[nodiscard]] auto has_node() const -> bool
    {
        return m_status.is_ok() && m_node.ref != nullptr;
    }

    // Return true if the cursor is positioned on a valid key, false otherwise
    [[nodiscard]] auto has_key() const -> bool
    {
        return has_node() && m_idx < NodeHdr::get_cell_count(m_node.hdr());
    }

    [[nodiscard]] auto page_id() const -> Id
    {
        CALICODB_EXPECT_TRUE(has_node());
        return m_node.ref->page_id;
    }

    auto reset(const Status &s = Status::ok()) -> void
    {
        release_nodes(kAllLevels);
        m_status = s;
        m_level = 0;
    }

    enum SeekType {
        kSeekWriter,
        kSeekNormal,
        kSeekReader,
    };

    auto seek_to_leaf(const Slice &key, SeekType type) -> bool;

    enum ReleaseType {
        kCurrentLevel,
        kAllLevels,
    };

    auto release_nodes(ReleaseType type) -> void;
    auto seek_first() -> void override;
    auto seek_last() -> void override;
    auto next() -> void override;
    auto previous() -> void override;
    auto seek(const Slice &key) -> void override;

    [[nodiscard]] auto handle() -> void * override
    {
        return this;
    }

    auto key() const -> Slice override
    {
        CALICODB_EXPECT_TRUE(is_valid());
        return m_key;
    }

    auto value() const -> Slice override
    {
        CALICODB_EXPECT_TRUE(is_valid());
        return m_value;
    }

    [[nodiscard]] auto is_valid() const -> bool override
    {
        return has_key() || m_saved;
    }

    auto status() const -> Status override
    {
        return m_status;
    }

    auto TEST_tree() const -> Tree &
    {
        return *m_tree;
    }
};

} // namespace calicodb

#endif // CALICODB_TREE_H

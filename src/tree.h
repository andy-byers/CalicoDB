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

class CursorImpl : public Cursor
{
    Status m_status;
    Tree *m_tree;
    U32 *m_count_ptr = nullptr;

    Node m_node;
    U32 m_index = 0;

    // Buffers for storing keys and/or values that wouldn't fit on a single page.
    std::string m_key_buf;
    std::string m_val_buf;

    // References to the current key and value, which may be located in one of the auxiliary buffers, or
    // directly on a database page from the cache (in m_node).
    Slice m_key;
    Slice m_val;

protected:
    auto seek_to(Node node, U32 index) -> void;
    auto fetch_payload(Node &node, U32 index) -> Status;

public:
    friend class Tree;
    friend class SchemaCursor;

    explicit CursorImpl(Tree &tree, U32 *count_ptr)
        : m_tree(&tree),
          m_count_ptr(count_ptr)
    {
    }

    ~CursorImpl() override;

    [[nodiscard]] auto is_valid() const -> bool override
    {
        return m_node.ref && m_status.is_ok();
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

[[nodiscard]] inline auto determine_prefix(const Slice &lhs, const Slice &rhs) -> Slice
{
    CALICODB_EXPECT_FALSE(lhs.is_empty());
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
    // Include the first mismatch in `rhs`.
    return rhs.range(0, n + 1);
}

class Tree final
{
public:
    ~Tree()
    {
        // Ensure that all page references are released back to the pager.
        finish_operation();
    }

    explicit Tree(Pager &pager, Stat &stat, char *scratch, const Id *root_id);
    static auto create(Pager &pager, Id *out) -> Status;
    static auto destroy(Tree &tree) -> Status;

    auto put(const Slice &key, const Slice &value) -> Status;
    auto get(const Slice &key, std::string *value) const -> Status;
    auto erase(const Slice &key) -> Status;
    auto vacuum(Schema &schema) -> Status;

    auto allocate(bool is_external, Node &node) -> Status
    {
        PageRef *ref;
        auto s = m_pager->allocate(ref);
        if (s.is_ok()) {
            CALICODB_EXPECT_FALSE(PointerMap::is_map(ref->page_id));
            node = Node::from_new_page(*ref, is_external);
        }
        return s;
    }

    auto acquire(Id page_id, bool write, Node &node) const -> Status
    {
        CALICODB_EXPECT_FALSE(PointerMap::is_map(page_id));

        PageRef *ref;
        auto s = m_pager->acquire(page_id, ref);
        if (s.is_ok()) {
            if (Node::from_existing_page(*ref, node)) {
                m_pager->release(ref);
                return corrupted_page(page_id);
            }
            if (write) {
                upgrade(node);
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
        if (node.ref && m_pager->mode() == Pager::kDirty) {
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
        // Pager::release() NULLs out the page reference.
        m_pager->release(node.ref);
    }

    auto advance_cursor(Node node, int diff) const -> void
    {
        // InternalCursor::move_to() takes ownership of the page reference in `node`. When the working set
        // is cleared below, this reference is not released.
        m_c.move_to(std::move(node), diff);
    }

    auto finish_operation() const -> void
    {
        m_c.clear();
    }

    [[nodiscard]] auto root() const -> Id
    {
        return m_root_id ? *m_root_id : Id::root();
    }

    [[nodiscard]] auto TEST_to_string() const -> std::string;
    auto TEST_validate() const -> void;

private:
    friend class CursorImpl;
    friend class SchemaCursor;
    friend class DBImpl;
    friend class Schema;
    friend class TreeValidator;

    auto corrupted_page(Id page_id) const -> Status;

    // Find the external node containing the lowest/highest key in the tree
    auto find_lowest(Node &node_out) const -> Status;
    auto find_highest(Node &node_out) const -> Status;

    // Move the internal cursor to the external node containing the given `key`
    auto find_external(const Slice &key, bool &exact) const -> Status;

    auto redistribute_cells(Node &left, Node &right, Node &parent, U32 pivot_idx) -> Status;

    auto resolve_overflow() -> Status;
    auto split_root() -> Status;
    auto split_nonroot() -> Status;
    auto split_nonroot_fast(Node parent, Node right) -> Status;

    auto resolve_underflow() -> Status;
    auto fix_root() -> Status;
    auto fix_nonroot(Node parent, U32 index) -> Status;

    auto read_key(const Cell &cell, std::string &scratch, Slice *key_out, U32 limit = 0) const -> Status;
    auto read_value(const Cell &cell, std::string &scratch, Slice *value_out) const -> Status;
    auto read_key(Node &node, U32 index, std::string &scratch, Slice *key_out, U32 limit = 0) const -> Status;
    auto read_value(Node &node, U32 index, std::string &scratch, Slice *value_out) const -> Status;
    auto write_key(Node &node, U32 index, const Slice &key) -> Status;
    auto write_value(Node &node, U32 index, const Slice &value) -> Status;

    auto emplace(Node &node, const Slice &key, const Slice &value, U32 index, bool &overflow) -> Status;
    auto free_overflow(Id head_id) -> Status;
    auto destroy_impl(Node node) -> Status;
    auto vacuum_step(PageRef &free, PointerMap::Entry entry, Schema &schema, Id last_id) -> Status;

    struct PivotOptions {
        const Cell *left;
        const Cell *right;
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
    mutable class InternalCursor
    {
        mutable Status m_status;
        std::string m_buffer;
        Tree *m_tree;
        Node m_node;

    public:
        static constexpr std::size_t kMaxDepth = 20;
        struct Location {
            Id page_id;
            U32 index = 0;
        } history[kMaxDepth];
        int level = 0;

        explicit InternalCursor(Tree &tree);
        ~InternalCursor();

        [[nodiscard]] auto is_valid() const -> bool
        {
            return m_node.ref && m_status.is_ok();
        }

        [[nodiscard]] auto status() const -> Status
        {
            return m_status;
        }

        [[nodiscard]] auto index() const -> U32
        {
            CALICODB_EXPECT_TRUE(is_valid());
            return history[level].index;
        }

        [[nodiscard]] auto node() -> Node &
        {
            CALICODB_EXPECT_TRUE(is_valid());
            return m_node;
        }

        auto move_to(Node node, int diff) -> void
        {
            clear();
            level += diff;
            history[level].page_id = node.ref->page_id;
            m_node = std::move(node);
        }

        auto clear() -> void;
        auto seek_root() -> void;
        auto seek(const Slice &key) -> bool;
        auto move_down(Id child_id) -> void;
    } m_c;

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

// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_TREE_H
#define CALICODB_TREE_H

#include "calicodb/cursor.h"
#include "header.h"
#include "node.h"
#include "pager.h"
#include <optional>

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
    auto seek_to(Node node, std::size_t index) -> void;
    auto fetch_payload(Node &node, std::size_t index) -> Status;

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
    auto free(Node node) -> Status;
    auto upgrade(Node &node) const -> void;
    auto release(Node node) const -> void;

    auto advance_cursor(Node node, int diff) const -> void;
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

    auto redistribute_cells(Node &left, Node &right, Node &parent) -> Status;

    auto resolve_overflow() -> Status;
    auto split_root() -> Status;
    auto split_nonroot() -> Status;
    auto split_nonroot_fast(Node parent, Node right) -> Status;

    auto resolve_underflow() -> Status;
    auto fix_root() -> Status;
    auto fix_nonroot(Node parent, std::size_t index) -> Status;

    auto read_key(const Cell &cell, std::string &scratch, Slice *key_out, std::size_t limit = 0) const -> Status;
    auto read_value(const Cell &cell, std::string &scratch, Slice *value_out) const -> Status;
    auto read_key(Node &node, std::size_t index, std::string &scratch, Slice *key_out, std::size_t limit = 0) const -> Status;
    auto read_value(Node &node, std::size_t index, std::string &scratch, Slice *value_out) const -> Status;
    auto write_key(Node &node, std::size_t index, const Slice &key) -> Status;
    auto write_value(Node &node, std::size_t index, const Slice &value) -> Status;

    auto emplace(Node &node, const Slice &key, const Slice &value, std::size_t index, bool &overflow) -> Status;
    auto free_overflow(Id head_id) -> Status;
    auto destroy_impl(Node node) -> Status;
    auto vacuum_step(PageRef &free, PointerMap::Entry entry, Schema &schema, Id last_id) -> Status;
    auto transfer_left(Node &left, Node &right) -> Status;

    auto insert_cell(Node &node, std::size_t index, const Cell &cell) -> Status;
    auto remove_cell(Node &node, std::size_t index) -> Status;
    auto find_parent_id(Id page_id, Id &out) const -> Status;
    auto fix_parent_id(Id page_id, Id parent_id, PointerMap::Type type) -> Status;
    auto maybe_fix_overflow_chain(const Cell &cell, Id parent_id) -> Status;
    auto fix_links(Node &node, Id parent_id = Id::null()) -> Status;
    [[nodiscard]] auto cell_scratch(std::size_t n = 0) -> char *;

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

    // When the node pointed at by m_c overflows, store the cell that couldn't fit on the page here. The
    // overflow index (the index that the cell would have had if it fit on the page) is m_c.index().
    Cell m_ovfl_cell;
    bool m_has_ovfl = false;

    // Various tree operation counts are tracked in this variable.
    mutable Stats m_stats;

    // Scratch memory for defragmenting nodes and storing cells that aren't embedded in nodes.
    char *const m_node_scratch;
    char *const m_cell_scratch[3];

    Pager *const m_pager;
    const Id *const m_root_id;
};

} // namespace calicodb

#endif // CALICODB_TREE_H

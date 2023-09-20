// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_CURSOR_IMPL_H
#define CALICODB_CURSOR_IMPL_H

#include "buffer.h"
#include "calicodb/cursor.h"
#include "tree.h"

namespace calicodb
{

class Schema;

class CursorImpl
    : public Cursor,
      public HeapObject
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
    // position.
    static constexpr size_t kMaxDepth = 17 + 1;
    Node m_node_path[kMaxDepth - 1];
    uint32_t m_idx_path[kMaxDepth - 1];
    int m_level = 0;

    // Heap-allocated buffers used to store the current record. m_key and m_value are
    // slices into these buffers.
    Buffer<char> m_key_buf;
    Buffer<char> m_value_buf;
    size_t m_key_len = 0;
    size_t m_value_len = 0;
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
            seek_to_leaf(key(), kSeekNormal);
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

    auto move_to_right_sibling() -> void;
    auto move_to_left_sibling() -> void;
    auto search_node(const Slice &key) -> bool;

public:
    explicit CursorImpl(Tree &tree);
    ~CursorImpl() override;

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

    auto move_to_parent(bool preserve_path) -> void;
    auto assign_child(Node child) -> void;
    auto move_to_child(Id child_id) -> void;
    auto fetch_user_payload() -> Status;
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

    auto finish_write(const Status &s) -> void
    {
        if (!s.is_ok()) {
            reset(s);
            return;
        }
        m_idx_path[m_level] = m_idx;
        m_node_path[m_level] = move(m_node);
        while (!m_node_path[m_level].is_leaf()) {
            ++m_level;
        }
        m_node = move(m_node_path[m_level]);
        m_idx = m_idx_path[m_level];
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
    auto find(const Slice &key) -> void override;
    auto seek(const Slice &key) -> void override;

    [[nodiscard]] auto handle() -> void * override
    {
        return this;
    }

    auto key() const -> Slice override
    {
        CALICODB_EXPECT_TRUE(is_valid());
        CALICODB_EXPECT_LE(m_key_len, m_key_buf.len());
        return m_key_len ? Slice(m_key_buf.ptr(), m_key_len)
                         : Slice();
    }

    auto value() const -> Slice override
    {
        CALICODB_EXPECT_TRUE(is_valid());
        CALICODB_EXPECT_LE(m_value_len, m_value_buf.len());
        return m_value_len ? Slice(m_value_buf.ptr(), m_value_len)
                           : Slice();
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

    auto assert_state() -> bool;
};

} // namespace calicodb

#endif // CALICODB_CURSOR_IMPL_H

// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "cursor_impl.h"
#include "pager.h"
#include "schema.h"
#include "utils.h"

namespace calicodb
{

auto CursorImpl::fetch_user_payload() -> Status
{
    CALICODB_EXPECT_TRUE(has_key());
    const auto buffer_len = m_key.size() + m_value.size();

    m_key.clear();
    m_value.clear();

    Cell cell;
    if (m_node.read(m_idx, cell)) {
        return m_tree->corrupted_node(page_id());
    }
    Status s;
    if (cell.total_pl_size > buffer_len) {
        auto *ptr = Alloc::realloc_string(
            m_user_payload, cell.total_pl_size);
        if (ptr == nullptr) {
            s = Status::no_memory();
        } else {
            m_user_payload = ptr;
        }
    }
    if (cell.total_pl_size > 0) {
        if (s.is_ok()) {
            s = m_tree->read_key(cell, m_user_payload, &m_key);
        }
        if (s.is_ok()) {
            s = m_tree->read_value(cell, m_user_payload + m_key.size(), &m_value);
        }
        if (!s.is_ok()) {
            m_key.clear();
            m_value.clear();
        }
    }
    return s;
}

auto CursorImpl::ensure_correct_leaf(bool read_payload) -> void
{
    if (has_node()) {
        CALICODB_EXPECT_TRUE(m_node.is_leaf());
        if (m_idx == NodeHdr::get_cell_count(m_node.hdr())) {
            move_to_right_sibling();
        }
    }
    if (read_payload && has_key()) {
        m_status = fetch_user_payload();
    }
}

auto CursorImpl::move_to_right_sibling() -> void
{
    CALICODB_EXPECT_TRUE(has_node());
    CALICODB_EXPECT_TRUE(m_node.is_leaf());
    const auto leaf_level = m_level;
    for (uint32_t adjust = 0;; adjust = 1) {
        const auto ncells = NodeHdr::get_cell_count(m_node.hdr());
        if (++m_idx < ncells + adjust) {
            break;
        } else if (m_level == 0) {
            reset();
            return;
        }
        move_to_parent();
    }
    while (!m_node.is_leaf()) {
        move_to_child(m_node.read_child_id(m_idx));
        if (!m_status.is_ok()) {
            return;
        }
        m_idx = 0;
    }
    if (m_level != leaf_level) {
        m_status = Status::corruption();
    }
}

auto CursorImpl::move_to_left_sibling() -> void
{
    CALICODB_EXPECT_TRUE(has_node());
    CALICODB_EXPECT_TRUE(m_node.is_leaf());
    for (;;) {
        if (m_idx > 0) {
            --m_idx;
            break;
        } else if (m_level == 0) {
            reset();
            return;
        }
        move_to_parent();
    }
    while (!m_node.is_leaf()) {
        move_to_child(m_node.read_child_id(m_idx));
        if (!m_status.is_ok()) {
            return;
        }
        m_idx = NodeHdr::get_cell_count(m_node.hdr()) - m_node.is_leaf();
    }
}

auto CursorImpl::seek_to_last_leaf() -> void
{
    reset();
    m_status = m_tree->acquire(m_tree->root(), m_node);
    while (m_status.is_ok()) {
        m_idx = NodeHdr::get_cell_count(m_node.hdr());
        if (m_node.is_leaf()) {
            m_idx -= m_idx > 0;
            break;
        }
        move_to_child(NodeHdr::get_next_id(m_node.hdr()));
    }
}

auto CursorImpl::search_node(const Slice &key) -> bool
{
    CALICODB_EXPECT_TRUE(m_status.is_ok());
    CALICODB_EXPECT_NE(m_node.ref, nullptr);

    auto exact = false;
    auto upper = NodeHdr::get_cell_count(m_node.hdr());
    uint32_t lower = 0;

    while (lower < upper) {
        Slice rhs;
        const auto mid = (lower + upper) / 2;
        // This call to Tree::extract_key() may return a partial key, if the whole key wasn't
        // needed for the comparison. We read at most 1 byte more than is present in `key` so
        // that we still have the necessary length information to break ties. This lets us avoid
        // reading overflow chains if it isn't really necessary.
        m_status = m_tree->extract_key(m_node, mid, m_tree->m_key_scratch[0], rhs,
                                       static_cast<uint32_t>(key.size() + 1));
        if (!m_status.is_ok()) {
            break;
        }
        const auto cmp = key.compare(rhs);
        if (cmp <= 0) {
            exact = cmp == 0;
            upper = mid;
        } else {
            lower = mid + 1;
        }
    }

    m_idx = lower + exact * !m_node.is_leaf();
    return exact;
}

CursorImpl::CursorImpl(Tree &tree)
    : m_list_entry{this, nullptr, nullptr},
      m_tree(&tree),
      m_status(tree.m_pager->status())
{
    IntrusiveList::add_head(m_list_entry, tree.m_inactive_list);
}

CursorImpl::~CursorImpl()
{
    if (!IntrusiveList::is_empty(m_list_entry)) {
        IntrusiveList::remove(m_list_entry);
        reset();
    }
    Alloc::free(m_user_payload);
}

auto CursorImpl::move_to_parent() -> void
{
    CALICODB_EXPECT_GT(m_level, 0);
    release_nodes(kCurrentLevel);
    --m_level;
    m_idx = m_idx_path[m_level];
    m_node = std::move(m_node_path[m_level]);
}

auto CursorImpl::assign_child(Node child) -> void
{
    CALICODB_EXPECT_TRUE(has_node());
    m_idx_path[m_level] = m_idx;
    m_node_path[m_level] = std::move(m_node);
    m_node = std::move(child);
    ++m_level;
}

auto CursorImpl::move_to_child(Id child_id) -> void
{
    CALICODB_EXPECT_TRUE(has_node());
    if (m_level < static_cast<int>(kMaxDepth - 1)) {
        Node child;
        m_status = m_tree->acquire(child_id, child);
        if (m_status.is_ok()) {
            assign_child(std::move(child));
        }
    } else {
        m_status = m_tree->corrupted_node(child_id);
    }
}

auto CursorImpl::on_last_node() const -> bool
{
    CALICODB_EXPECT_TRUE(has_node());
    for (int i = 0; i < m_level; ++i) {
        const auto &node = m_node_path[i];
        if (m_idx_path[i] < NodeHdr::get_cell_count(node.hdr())) {
            return false;
        }
    }
    return true;
}

auto CursorImpl::seek_to_leaf(const Slice &key, SeekType type) -> bool
{
    if (m_status.is_corruption()) {
        // Don't recover from corruption. The user needs to restart the whole transaction.
        return false;
    }
    auto on_correct_node = false;
    if (has_key() && on_last_node()) {
        CALICODB_EXPECT_TRUE(m_node.is_leaf());

        // This block handles cases where the cursor is already positioned on the target node. This
        // means that (a) this tree is the most-recently-accessed tree in the database, and (b) the
        // last operation didn't cause an overflow or underflow.
        Slice boundary;
        m_status = m_tree->extract_key(m_node, 0, m_tree->m_key_scratch[0], boundary);
        if (!m_status.is_ok()) {
            return false;
        }
        on_correct_node = boundary <= key;
    }
    if (!on_correct_node) {
        reset();
        m_status = m_tree->acquire(m_tree->root(), m_node);
    }
    while (m_status.is_ok()) {
        const auto found_exact_key = search_node(key);
        if (m_status.is_ok()) {
            if (m_node.is_leaf()) {
                if (type >= kSeekNormal) {
                    ensure_correct_leaf(type != kSeekNormal);
                }
                return found_exact_key;
            }
            move_to_child(m_node.read_child_id(m_idx));
        }
    }
    return false;
}

auto CursorImpl::release_nodes(ReleaseType type) -> void
{
    m_tree->release(std::move(m_node));
    if (type < kAllLevels) {
        return;
    }
    for (int i = 0; i < m_level; ++i) {
        m_tree->release(std::move(m_node_path[i]));
    }
}

auto CursorImpl::seek_first() -> void
{
    prepare(Tree::kInitNormal);
    seek_to_first_leaf();
    if (has_key()) {
        m_status = fetch_user_payload();
    }
}

auto CursorImpl::seek_last() -> void
{
    prepare(Tree::kInitNormal);
    seek_to_last_leaf();
    if (has_key()) {
        m_status = fetch_user_payload();
    }
}

auto CursorImpl::next() -> void
{
    CALICODB_EXPECT_TRUE(is_valid());
    // NOTE: Loading the cursor position involves seeking back to the saved key. If the saved key
    //       was erased, then this will place the cursor on the first record with a key that
    //       compares greater than it.
    prepare(Tree::kInitNormal);

    if (!has_key()) {
        return;
    }
    move_to_right_sibling();
    if (has_key()) {
        m_status = fetch_user_payload();
    }
}

auto CursorImpl::previous() -> void
{
    CALICODB_EXPECT_TRUE(is_valid());
    prepare(Tree::kInitNormal);

    if (!has_key()) {
        return;
    }
    move_to_left_sibling();
    if (has_key()) {
        m_status = fetch_user_payload();
    }
}

auto CursorImpl::seek(const Slice &key) -> void
{
    // The cursor position is not reset prior to the call to seek_to_leaf(). seek_to_leaf() may
    // try to avoid performing a full root-to-leaf traversal.
    prepare(Tree::kInitNormal);
    seek_to_leaf(key, kSeekReader);
}

auto CursorImpl::find(const Slice &key) -> void
{
    prepare(Tree::kInitNormal);
    if (seek_to_leaf(key, kSeekReader)) {
        // Found a record with the given `key`.
    } else if (m_status.is_ok()) {
        reset(Status::not_found());
    } else {
        release_nodes(kAllLevels);
    }
}

} // namespace calicodb

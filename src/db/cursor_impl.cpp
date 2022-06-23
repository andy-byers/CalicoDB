#include "cursor_impl.h"
#include <algorithm>
#include "calico/exception.h"
#include "tree/tree.h"

namespace calico {

Cursor::Impl::Impl(ITree *tree)
    : m_tree {tree}
{
    reset();
}

auto Cursor::Impl::has_record() const -> bool
{
    return has_node() && m_index < m_node->cell_count();
}

auto Cursor::Impl::is_minimum() const -> bool
{
    return has_record() && !can_decrement();
}

auto Cursor::Impl::is_maximum() const -> bool
{
    return has_record() && !can_increment();
}

/**
 * Determine if the cursor is on the first entry in the tree.
 *
 * @returns Whether the cursor on the leftmost entry of the leftmost node
 */
auto Cursor::Impl::can_decrement() const -> bool
{
    CALICO_EXPECT_TRUE(has_node());
    if (!m_index && m_node->is_external()) {
        // The tree is empty.
        if (!m_node->cell_count() && m_traversal.empty())
            return false;
        return std::any_of(std::begin(m_traversal), std::end(m_traversal), [](Index index) {
            return index > 0;
        });
    }
    return true;
}

/**
 * Determine if the cursor is on the last entry in the tree.
 *
 * @returns Whether the cursor on the rightmost entry of the rightmost node
 */
auto Cursor::Impl::can_increment() const -> bool
{
    CALICO_EXPECT_TRUE(has_node());
    if (is_end_of_tree())
        return false;
    return !m_node->is_external() ||
           m_index < m_node->cell_count() - 1 ||
           !m_node->right_sibling_id().is_null();
}

/**
 * Determine if the cursor is at the end of the tree.
 *
 * @returns Whether the cursor one past the rightmost entry of the rightmost node
 */
auto Cursor::Impl::is_end_of_tree() const -> bool
{
    CALICO_EXPECT_TRUE(has_node());
    return is_end_of_node() &&
           m_node->is_external() &&
           m_node->right_sibling_id().is_null();
}

auto Cursor::Impl::is_end_of_node() const -> bool
{
    CALICO_EXPECT_TRUE(has_node());
    return m_index == m_node->cell_count();
}

auto Cursor::Impl::reset() -> void
{
    m_index = 0;
    m_traversal.clear();
    move_cursor(PID::root());
}

auto Cursor::Impl::find(BytesView key) -> bool
{
    CALICO_EXPECT_FALSE(key.is_empty());
    reset();

    if (!find_aux(key)) {
        if (is_end_of_node() && !is_end_of_tree())
            increment();
        if (is_end_of_tree())
            decrement();
        return false;
    }
    return true;
}

auto Cursor::Impl::find_minimum() -> void
{
    reset();
    find_local_min();
}

auto Cursor::Impl::find_local_min() -> void
{
    CALICO_EXPECT_TRUE(has_node());

    if (has_record()) {
        while (true) {
            m_index = 0;
            if (m_node->is_external())
                break;
            goto_child(m_index);
        }
    }
}

auto Cursor::Impl::find_maximum() -> void
{
    reset();
    find_local_max();
}

auto Cursor::Impl::find_local_max() -> void
{
    CALICO_EXPECT_TRUE(has_node());

    if (has_record()) {
        while (true) {
            m_index = m_node->cell_count() - 1;
            if (m_node->is_external())
                break;
            goto_child(m_index + 1);
        }
    }
}

auto Cursor::Impl::find_aux(BytesView key) -> bool
{
    CALICO_EXPECT_FALSE(key.is_empty());
    while (true) {
        const auto [index, found_eq] = m_node->find_ge(key);
        m_index = index;
        if (found_eq)
            return true;
        if (m_node->is_external())
            return false;
        goto_child(m_index);
    }
}

auto Cursor::Impl::increment() -> bool
{
    CALICO_EXPECT_TRUE(has_node());
    if (can_increment()) {
        if (m_node->is_external()) {
            increment_external();
        } else {
            increment_internal();
        }
        return true;
    }
    return false;
}

auto Cursor::Impl::increment_external() -> void
{
    CALICO_EXPECT_TRUE(has_node());
    CALICO_EXPECT_EQ(m_node->type(), PageType::EXTERNAL_NODE);

    if (m_index < m_node->cell_count())
        m_index++;
    if (!is_end_of_tree()) {
        while (is_end_of_node())
            goto_parent();
    }
}

auto Cursor::Impl::increment_internal() -> void
{
    CALICO_EXPECT_TRUE(has_node());
    CALICO_EXPECT_EQ(m_node->type(), PageType::INTERNAL_NODE);

    // m_index should never equal the cell count here. We handle this case when we traverse toward the root
    // from an external node.
    if (!is_end_of_node())
        goto_inorder_successor();
}

auto Cursor::Impl::decrement() -> bool
{
    CALICO_EXPECT_TRUE(has_node());
    if (can_decrement()) {
        if (m_node->is_external()) {
            decrement_external();
        } else {
            decrement_internal();
        }
        return true;
    }
    return false;
}

auto Cursor::Impl::decrement_internal() -> void
{
    CALICO_EXPECT_TRUE(has_node());
    CALICO_EXPECT_EQ(m_node->type(), PageType::INTERNAL_NODE);
    goto_inorder_predecessor();
}

auto Cursor::Impl::decrement_external() -> void
{
    CALICO_EXPECT_TRUE(has_node());
    CALICO_EXPECT_EQ(m_node->type(), PageType::EXTERNAL_NODE);

    if (m_index) {
        m_index--;

        // This method should leave us on the last cell if we were one past.
        CALICO_EXPECT_FALSE(is_end_of_tree());
        return;
    }
    while (!m_node->parent_id().is_null()) {
        goto_parent();
        if (m_index) {
            m_index--;
            break;
        }
    }
}

auto Cursor::Impl::goto_inorder_successor() -> void
{
    goto_child(m_index + 1);
    m_index = 0;
    while (!m_node->is_external()) {
        goto_child(m_index);
        m_index = 0;
    }
}

auto Cursor::Impl::goto_inorder_predecessor() -> void
{
    goto_child(m_index);
    m_index = m_node->cell_count();
    while (!m_node->is_external()) {
        goto_child(m_index);
        m_index = m_node->cell_count();
    }
    m_index--;
}

/**
 * Note that after calling this method, the value of m_index becomes meaningless. The caller should
 * set it to either 0 or m_node_helper->cell_count() - 1 after traversing into the child, depending on the
 * direction of traversal.
 */
auto Cursor::Impl::goto_child(Index index) -> void
{
    CALICO_EXPECT_TRUE(has_node());
    CALICO_EXPECT_FALSE(m_node->is_external());
    CALICO_EXPECT_LE(index, m_node->cell_count());
    const auto child_id = m_node->child_id(index);
    move_cursor(child_id);
    m_traversal.push_back(index);
}

auto Cursor::Impl::goto_parent() -> void
{
    CALICO_EXPECT_TRUE(has_node());
    const auto parent_id = m_node->parent_id();
    CALICO_EXPECT_FALSE(parent_id.is_null());
    move_cursor(parent_id);
    m_index = m_traversal.back();
    m_traversal.pop_back();
}

auto Cursor::Impl::key() const -> BytesView
{
    CALICO_EXPECT_TRUE(has_record());
    return m_node->read_key(m_index);
}

auto Cursor::Impl::value() const -> std::string
{
    CALICO_EXPECT_TRUE(has_record());
    return m_tree.value->collect_value(*m_node, m_index);
}

auto Cursor::Impl::move_cursor(PID pid) -> void
{
    try {
        m_node.reset();
        m_node = m_tree.value->acquire_node(pid, false);
    } catch (...) {
        CALICO_EXPECT_EQ(m_node, std::nullopt);
        m_traversal.clear();
        m_index = 0;
        throw;
    }
}

} // calico

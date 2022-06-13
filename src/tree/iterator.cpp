#include "iterator.h"
#include <algorithm>
#include "cub/exception.h"
#include "tree/tree.h"

namespace cub {

Iterator::Iterator(ITree *source)
    : m_source {source}
{
    reset();
}

auto Iterator::has_record() const -> bool
{
    return has_node() && m_index < m_node->cell_count();
}

auto Iterator::is_minimum() const -> bool
{
    return has_record() && !can_decrement();
}

auto Iterator::is_maximum() const -> bool
{
    return has_record() && !can_increment();
}

/**
 * Determine if the cursor is on the first entry in the tree.
 *
 * @returns Whether the cursor on the leftmost entry of the leftmost node
 */
auto Iterator::can_decrement() const -> bool
{
    CUB_EXPECT_TRUE(has_node());
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
auto Iterator::can_increment() const -> bool
{
    CUB_EXPECT_TRUE(has_node());
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
auto Iterator::is_end_of_tree() const -> bool
{
    CUB_EXPECT_TRUE(has_node());
    return is_end_of_node() &&
           m_node->is_external() &&
           m_node->right_sibling_id().is_null();
}

auto Iterator::is_end_of_node() const -> bool
{
    CUB_EXPECT_TRUE(has_node());
    return m_index == m_node->cell_count();
}

auto Iterator::reset() -> void
{
    m_index = 0;
    m_traversal.clear();
    move_cursor(PID::root());
}

auto Iterator::find(BytesView key) -> bool
{
    CUB_EXPECT_FALSE(key.is_empty());
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

auto Iterator::find_minimum() -> void
{
    reset();
    find_local_min();
}

auto Iterator::find_local_min() -> void
{
    CUB_EXPECT_TRUE(has_node());

    if (has_record()) {
        while (true) {
            m_index = 0;
            if (m_node->is_external())
                break;
            goto_child(m_index);
        }
    }
}

auto Iterator::find_maximum() -> void
{
    reset();
    find_local_max();
}

auto Iterator::find_local_max() -> void
{
    CUB_EXPECT_TRUE(has_node());

    if (has_record()) {
        while (true) {
            m_index = m_node->cell_count() - 1;
            if (m_node->is_external())
                break;
            goto_child(m_index + 1);
        }
    }
}

auto Iterator::find_aux(BytesView key) -> bool
{
    CUB_EXPECT_FALSE(key.is_empty());
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

auto Iterator::increment() -> bool
{
    CUB_EXPECT_TRUE(has_node());
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

auto Iterator::increment_external() -> void
{
    CUB_EXPECT_TRUE(has_node());
    CUB_EXPECT_EQ(m_node->type(), PageType::EXTERNAL_NODE);

    if (m_index < m_node->cell_count())
        m_index++;
    if (!is_end_of_tree()) {
        while (is_end_of_node())
            goto_parent();
    }
}

auto Iterator::increment_internal() -> void
{
    CUB_EXPECT_TRUE(has_node());
    CUB_EXPECT_EQ(m_node->type(), PageType::INTERNAL_NODE);

    // m_index should never equal the cell count here. We handle this case when we traverse toward the root
    // from an external node.
    if (!is_end_of_node())
        goto_inorder_successor();
}

auto Iterator::decrement() -> bool
{
    CUB_EXPECT_TRUE(has_node());
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

auto Iterator::decrement_internal() -> void
{
    CUB_EXPECT_TRUE(has_node());
    CUB_EXPECT_EQ(m_node->type(), PageType::INTERNAL_NODE);
    goto_inorder_predecessor();
}

auto Iterator::decrement_external() -> void
{
    CUB_EXPECT_TRUE(has_node());
    CUB_EXPECT_EQ(m_node->type(), PageType::EXTERNAL_NODE);

    if (m_index) {
        m_index--;

        // This method should leave us on the last cell if we were one past.
        CUB_EXPECT_FALSE(is_end_of_tree());
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

auto Iterator::goto_inorder_successor() -> void
{
    goto_child(m_index + 1);
    m_index = 0;
    while (!m_node->is_external()) {
        goto_child(m_index);
        m_index = 0;
    }
}

auto Iterator::goto_inorder_predecessor() -> void
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
auto Iterator::goto_child(Index index) -> void
{
    CUB_EXPECT_TRUE(has_node());
    CUB_EXPECT_FALSE(m_node->is_external());
    CUB_EXPECT_LE(index, m_node->cell_count());
    const auto child_id = m_node->child_id(index);
    move_cursor(child_id);
    m_traversal.push_back(index);
}

auto Iterator::goto_parent() -> void
{
    CUB_EXPECT_TRUE(has_node());
    const auto parent_id = m_node->parent_id();
    CUB_EXPECT_FALSE(parent_id.is_null());
    move_cursor(parent_id);
    m_index = m_traversal.back();
    m_traversal.pop_back();
}

auto Iterator::key() const -> BytesView
{
    CUB_EXPECT_TRUE(has_record());
    return m_node->read_key(m_index);
}

auto Iterator::value() const -> std::string
{
    CUB_EXPECT_TRUE(has_record());
    return m_source.value->collect_value(*m_node, m_index);
}

auto Iterator::move_cursor(PID pid) -> void
{
    try {
        m_node.reset();
        m_node = m_source.value->acquire_node(pid, false);
    } catch (...) {
        CUB_EXPECT_EQ(m_node, std::nullopt);
        m_traversal.clear(); // TODO: Catch and rethrow in the method that affects these two variables. It's weird to do it here where we don't already modify or read them (IMHO).
        m_index = 0;
        throw;
    }
}

} // cub
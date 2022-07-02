#include "internal.h"
#include "page/file_header.h"
#include "utils/layout.h"

namespace calico {

Internal::Internal(Parameters param)
    : m_scratch {get_max_local(param.pool->page_size()) * 2},
      m_pool {param.pool},
      m_cell_count {param.cell_count} {}

auto Internal::collect_value(const Node &node, Index index) const -> std::string
{
    auto cell = node.read_cell(index);
    const auto local = cell.local_value();
    std::string result(cell.value_size(), '\x00');
    auto out = stob(result);

    // Note that it is possible to have no value stored locally but have an overflow page. The happens when
    // the key is of maximal length (i.e. get_max_local(m_header->page_size())).
    if (!local.is_empty())
        mem_copy(out, local, local.size());

    if (!cell.overflow_id().is_null()) {
        CALICO_EXPECT_GT(cell.value_size(), cell.local_value().size());
        out.advance(local.size());
        m_pool->collect_overflow_chain(cell.overflow_id(), out);
    }
    return result;
}

auto Internal::find_root(bool is_writable) -> Node
{
    return m_pool->acquire(PID::root(), is_writable);
}

auto Internal::find_external(BytesView key, bool is_writable) -> Result
{
    auto node = find_root(is_writable);
    Node::SearchResult result;
    while (true) {
        result = node.find_ge(key);
        if (node.is_external())
            break;
        result.index += result.found_eq;
        node = m_pool->acquire(node.child_id(result.index), is_writable);
    }
    const auto [index, found_eq] = result;
    return {std::move(node), index, found_eq};
}


auto Internal::positioned_insert(Position position, BytesView key, BytesView value) -> void
{
    CALICO_EXPECT_LE(key.size(), get_max_local(m_pool->page_size()));
    auto [node, index] = std::move(position);
    auto cell = make_cell(key, value, true);
    node.insert_at(index, std::move(cell));
    m_cell_count++;

    if (node.is_overflowing())
        balance_after_overflow(std::move(node));
}

auto Internal::positioned_modify(Position position, BytesView value) -> void
{
    auto [node, index] = std::move(position);
    auto old_cell = node.read_cell(index);
    // Make a copy of the key. The data backing the old key slice may be written over when we call
    // remove_at() on the old cell.
    const auto key = btos(old_cell.key());
    auto new_cell = make_cell(stob(key), value, true);

    if (old_cell.overflow_size())
        m_pool->destroy_overflow_chain(old_cell.overflow_id(), old_cell.overflow_size());

    node.remove_at(index, old_cell.size());
    node.insert_at(index, std::move(new_cell));

    if (node.is_overflowing())
        balance_after_overflow(std::move(node));
}

auto Internal::positioned_remove(Position position) -> void
{
    auto [node, index] = std::move(position);
    CALICO_EXPECT_TRUE(node.is_external());
    CALICO_EXPECT_LT(index, node.cell_count());
    CALICO_EXPECT_GT(m_cell_count, 0);
    m_cell_count--;

    auto cell = node.read_cell(index);
    auto anchor = btos(cell.key());
    if (cell.overflow_size())
        m_pool->destroy_overflow_chain(cell.overflow_id(), cell.overflow_size());

    node.remove_at(index, node.read_cell(index).size());
    // TODO: Figure out how to avoid storing the anchor. It's easy to either use the call stack or an actual stack to backtrack,
    //       however, some rebalancing operations can cause ancestor nodes to split, which can invalidate the stack.
    balance_after_underflow(std::move(node), stob(anchor));
}

auto Internal::find_local_min(Node root) -> Position
{
    while (!root.is_external())
        root = m_pool->acquire(root.child_id(0), true);
    return {std::move(root), 0};
}

auto Internal::find_local_max(Node root) -> Position
{
    while (!root.is_external())
        root = m_pool->acquire(root.rightmost_child_id(), true);
    const auto index = root.cell_count() - 1;
    return {std::move(root), index};
}

auto Internal::balance_after_overflow(Node node) -> void
{
    CALICO_EXPECT(node.is_overflowing());
    while (node.is_overflowing()) {
        if (node.id().is_root()) {
            node = split_root(std::move(node));
        } else {
            node = split_non_root(std::move(node));
        }
    }
}

auto Internal::balance_after_underflow(Node node, BytesView anchor) -> void
{
    while (node.is_underflowing()) {
        if (node.id().is_root()) {
            if (!node.cell_count())
                fix_root(std::move(node));
            break;
        } else {
            auto parent = m_pool->acquire(node.parent_id(), true);
            // NOTE: Searching for the anchor key from the node we removed a cell from should
            //       always give us the correct cell ID due to the B-Tree ordering rules.
            const auto [index, found_eq] = parent.find_ge(anchor);
            if (!fix_non_root(std::move(node), parent, index + found_eq))
                return;
            node = std::move(parent);
        }
    }
}

/**
 * Balancing routine for fixing an over-full root node.
 */
auto Internal::split_root(Node root) -> Node
{
    CALICO_EXPECT_TRUE(root.id().is_root());
    CALICO_EXPECT_TRUE(root.is_overflowing());

    auto child = m_pool->allocate(root.type());
    ::calico::split_root(root, child);

    maybe_fix_child_parent_connections(child);
    CALICO_EXPECT_TRUE(child.is_overflowing());
    return child;
}

auto Internal::split_non_root(Node node) -> Node
{
    CALICO_EXPECT_FALSE(node.id().is_root());
    CALICO_EXPECT_FALSE(node.parent_id().is_null());
    CALICO_EXPECT(node.is_overflowing());

    auto parent = m_pool->acquire(node.parent_id(), true);
    auto sibling = m_pool->allocate(node.type());

    auto median = ::calico::split_non_root(node, sibling, m_scratch.get());
    auto [index, found_eq] = parent.find_ge(median.key());
    CALICO_EXPECT_FALSE(found_eq);

    if (node.is_external() && !sibling.right_sibling_id().is_null()) {
        auto right = m_pool->acquire(sibling.right_sibling_id(), true);
        right.set_left_sibling_id(sibling.id());
    }

    parent.insert_at(index, std::move(median));
    CALICO_EXPECT_FALSE(node.is_overflowing());
    CALICO_EXPECT_FALSE(sibling.is_overflowing());

    const auto offset = !parent.is_overflowing();
    parent.set_child_id(index + offset, sibling.id());
    maybe_fix_child_parent_connections(sibling);
    return parent;
}

auto Internal::maybe_fix_child_parent_connections(Node &node) -> void
{
    if (!node.is_external()) {
        const auto fix_connection = [&node, this](PID child_id) {
            auto child = m_pool->acquire(child_id, true);
            child.set_parent_id(node.id());
        };

        for (Index index {}; index <= node.cell_count(); ++index)
            fix_connection(node.child_id(index));

        if (node.is_overflowing())
            fix_connection(node.overflow_cell().left_child_id());
    }
}

/**
 * Note that the key and value must exist until the cell is safely embedded in the tree. If
 * the tree is balanced and there are no overflow cells then this is guaranteed to be true.
 */
auto Internal::make_cell(BytesView key, BytesView value, bool is_external) -> Cell
{
    if (is_external) {
        auto cell = ::calico::make_external_cell(key, value, m_pool->page_size());
        if (!cell.overflow_id().is_null()) {
            const auto overflow_value = value.range(cell.local_value().size());
            cell.set_overflow_id(m_pool->allocate_overflow_chain(overflow_value));
        }
        return cell;
    } else {
        return ::calico::make_internal_cell(key, m_pool->page_size());
    }
}

auto Internal::fix_non_root(Node node, Node &parent, Index index) -> bool
{
    CALICO_EXPECT_FALSE(node.id().is_root());
    CALICO_EXPECT_FALSE(node.is_overflowing());
    CALICO_EXPECT_FALSE(parent.is_overflowing());
    if (index > 0) {
        auto Lc = m_pool->acquire(parent.child_id(index - 1), true);
        if (can_merge_siblings(Lc, node, parent.read_cell(index - 1))) {
            merge_right(Lc, node, parent, index - 1);
            maybe_fix_child_parent_connections(Lc);
            if (node.is_external() && !node.right_sibling_id().is_null()) {
                auto rc = m_pool->acquire(node.right_sibling_id(), true);
                rc.set_left_sibling_id(Lc.id());
            }
            m_pool->destroy(std::move(node));
            return true;
        }
    }
    if (index < parent.cell_count()) {
        auto rc = m_pool->acquire(parent.child_id(index + 1), true);
        if (can_merge_siblings(node, rc, parent.read_cell(index))) {
            merge_left(node, rc, parent, index);
            maybe_fix_child_parent_connections(node);
            if (rc.is_external() && !rc.right_sibling_id().is_null()) {
                auto rrc = m_pool->acquire(rc.right_sibling_id(), true);
                rrc.set_left_sibling_id(node.id());
            }
            m_pool->destroy(std::move(rc));
            return true;
        }
    }
    // Skip the rotation but keep on rebalancing.
    if (!node.is_underflowing())
        return true;

    auto maybe_fix_parent = [&] {
        if (parent.is_overflowing()) {
            node.take();
            balance_after_overflow(std::move(parent));
            return false;
        }
        return true;
    };
    struct SiblingInfo {
        std::optional<Node> node;
        Size cell_count {};
    };
    SiblingInfo siblings[2] {};

    if (index > 0) {
        auto left_sibling = m_pool->acquire(parent.child_id(index - 1), true);
        const auto cell_count = left_sibling.cell_count();
        siblings[0] = {std::move(left_sibling), cell_count};
    }
    if (index < parent.cell_count()) {
        auto right_sibling = m_pool->acquire(parent.child_id(index + 1), true);
        const auto cell_count = right_sibling.cell_count();
        siblings[1] = {std::move(right_sibling), cell_count};
    }
    // For now, we'll skip rotation if it wouldn't yield us more balanced results with respect to
    // the cell counts. TODO: Maybe look into incorporating the usable space of each node, and maybe the size of the separators, to make a better-informed decision.
    const auto left_has_enough_cells = siblings[0].cell_count > node.cell_count() + 1;
    const auto right_has_enough_cells = siblings[1].cell_count > node.cell_count() + 1;
    if (!left_has_enough_cells && !right_has_enough_cells)
        return true;

    // Note that we are guaranteed at least one sibling (unless we are in the root, which is
    // handled by fix_root() anyway).
    if (siblings[0].cell_count > siblings[1].cell_count) {
        auto [left_sibling, cell_count] = std::move(siblings[0]);
        CALICO_EXPECT_NE(left_sibling, std::nullopt);
        siblings[1].node.reset();
        rotate_right(parent, *left_sibling, node, index - 1);
        CALICO_EXPECT_FALSE(node.is_overflowing());
        left_sibling.reset();
        return maybe_fix_parent();
    } else {
        auto [right_sibling, cell_count] = std::move(siblings[1]);
        CALICO_EXPECT_NE(right_sibling, std::nullopt);
        siblings[0].node.reset();
        rotate_left(parent, node, *right_sibling, index);
        CALICO_EXPECT_FALSE(node.is_overflowing());
        right_sibling.reset();
        return maybe_fix_parent();
    }
    return true;
}

auto Internal::fix_root(Node node) -> void
{
    CALICO_EXPECT_TRUE(node.id().is_root());
    CALICO_EXPECT_TRUE(node.is_underflowing());

    // If the root is external here, the whole tree must be empty.
    if (!node.is_external()) {
        auto child = m_pool->acquire(node.rightmost_child_id(), true);

        // We don't have enough room to transfer the child contents into the root, due to the file header. In
        // this case, we'll just split the child and let the median cell be inserted into the root. Note that
        // the child needs an overflow cell for the split routine to work. We'll just fake it by extracting an
        // arbitrary cell and making it the overflow cell.
        if (child.usable_space() < node.header_offset()) {
            child.set_overflow_cell(child.extract_cell(0, m_scratch.get()));
            node.take();
            split_non_root(std::move(child));
            node = find_root(true);
        } else {
            ::calico::merge_root(node, child);
            m_pool->destroy(std::move(child));
        }
        maybe_fix_child_parent_connections(node);
    }
}

auto Internal::save_header(FileHeader &header) const -> void
{
    m_pool->save_header(header);
    header.set_key_count(m_cell_count);
}

auto Internal::load_header(const FileHeader &header) -> void
{
    m_pool->load_header(header);
    m_cell_count = header.record_count();
}

auto Internal::rotate_left(Node &parent, Node &Lc, Node &rc, Index index) -> void
{
    if (Lc.is_external()) {
        external_rotate_left(parent, Lc, rc, index);
    } else {
        internal_rotate_left(parent, Lc, rc, index);
    }
}

auto Internal::rotate_right(Node &parent, Node &Lc, Node &rc, Index index) -> void
{
    if (Lc.is_external()) {
        external_rotate_right(parent, Lc, rc, index);
    } else {
        internal_rotate_right(parent, Lc, rc, index);
    }
}

auto Internal::external_rotate_left(Node &parent, Node &Lc, Node &rc, Index index) -> void
{
    CALICO_EXPECT_FALSE(parent.is_external());
    CALICO_EXPECT_GT(parent.cell_count(), 0);
    CALICO_EXPECT_GT(rc.cell_count(), 1);

    auto old_separator = parent.read_cell(index);
    auto lowest = rc.extract_cell(0, m_scratch.get());
    auto new_separator = make_cell(rc.read_key(0), {}, false);
    new_separator.set_left_child_id(Lc.id());

    // Parent might overflow.
    parent.remove_at(index, old_separator.size());
    parent.insert_at(index, std::move(new_separator));

    Lc.insert_at(Lc.cell_count(), std::move(lowest));
    CALICO_EXPECT_FALSE(Lc.is_overflowing());
}

auto Internal::external_rotate_right(Node &parent, Node &Lc, Node &rc, Index index) -> void
{
    CALICO_EXPECT_FALSE(parent.is_external());
    CALICO_EXPECT_GT(parent.cell_count(), 0);
    CALICO_EXPECT_GT(Lc.cell_count(), 1);

    auto separator = parent.read_cell(index);
    auto highest = Lc.extract_cell(Lc.cell_count() - 1, m_scratch.get());
    auto new_separator = make_cell(highest.key(), {}, false);
    new_separator.set_left_child_id(Lc.id());

    // Parent might overflow.
    parent.remove_at(index, separator.size());
    parent.insert_at(index, std::move(new_separator));

    rc.insert_at(0, std::move(highest));
    CALICO_EXPECT_FALSE(rc.is_overflowing());
}

auto Internal::internal_rotate_left(Node &parent, Node &Lc, Node &rc, Index index) -> void
{
    CALICO_EXPECT_FALSE(parent.is_external());
    CALICO_EXPECT_FALSE(Lc.is_external());
    CALICO_EXPECT_EQ(Lc.type(), rc.type());
    CALICO_EXPECT_GT(parent.cell_count(), 0);
    CALICO_EXPECT_GT(rc.cell_count(), 1);

    auto separator = parent.extract_cell(index, m_scratch.get());
    auto child = m_pool->acquire(rc.child_id(0), true);
    separator.set_left_child_id(Lc.rightmost_child_id());
    child.set_parent_id(Lc.id());
    Lc.set_rightmost_child_id(child.id());
    Lc.insert_at(Lc.cell_count(), std::move(separator));
    CALICO_EXPECT_FALSE(Lc.is_overflowing());

    auto lowest = rc.extract_cell(0, m_scratch.get());
    lowest.set_left_child_id(Lc.id());
    // Parent might overflow.
    parent.insert_at(index, std::move(lowest));
}

auto Internal::internal_rotate_right(Node &parent, Node &Lc, Node &rc, Index index) -> void
{
    CALICO_EXPECT_FALSE(parent.is_external());
    CALICO_EXPECT_FALSE(Lc.is_external());
    CALICO_EXPECT_EQ(Lc.type(), rc.type());
    CALICO_EXPECT_GT(parent.cell_count(), 0);
    CALICO_EXPECT_GT(Lc.cell_count(), 1);

    auto separator = parent.extract_cell(index, m_scratch.get());
    auto child = m_pool->acquire(Lc.rightmost_child_id(), true);
    separator.set_left_child_id(child.id());
    child.set_parent_id(rc.id());
    Lc.set_rightmost_child_id(Lc.child_id(Lc.cell_count() - 1));
    rc.insert_at(0, std::move(separator));
    CALICO_EXPECT_FALSE(rc.is_overflowing());

    auto highest = Lc.extract_cell(Lc.cell_count() - 1, m_scratch.get());
    highest.set_left_child_id(Lc.id());
    // The parent might overflow.
    parent.insert_at(index, std::move(highest));
}

} // calico
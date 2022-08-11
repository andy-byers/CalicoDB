#include "internal.h"
#include "utils/layout.h"

namespace cco {

Internal::Internal(Parameters param)
    : m_maximum_key_size {get_max_local(param.pool->page_size())},
      // Scratch memory needs to be able to hold a maximally-sized cell.
      m_scratch {m_maximum_key_size + MAX_CELL_HEADER_SIZE},
      m_pool {param.pool},
      m_cell_count {param.cell_count}
{}

auto Internal::collect_value(const Node &node, Index index) const -> Result<std::string>
{
    auto cell = node.read_cell(index);
    const auto local = cell.local_value();
    std::string result(cell.value_size(), '\x00');
    auto out = stob(result);

    // Note that it is possible to have no value stored locally but have an overflow page. The happens when
    // the key is of maximal length (i.e. m_maximum_key_size).
    if (!local.is_empty())
        mem_copy(out, local, local.size());

    if (!cell.overflow_id().is_null()) {
        CCO_EXPECT_GT(cell.value_size(), cell.local_value().size());
        out.advance(local.size());
        CCO_TRY(m_pool->collect_chain(cell.overflow_id(), out));
    }
    return result;
}

auto Internal::find_root(bool is_writable) -> Result<Node>
{
    return m_pool->acquire(PageId::base(), is_writable);
}

auto Internal::find_external(BytesView key) -> Result<SearchResult>
{
    if (m_cell_count == 0)
        return SearchResult {PageId::base(), 0, false};

    auto node = find_root(false);
    if (!node.has_value())
        return Err {node.error()};

    Node::FindGeResult result;
    for (;;) {
        result = node->find_ge(key);
        if (node->is_external())
            break;
        result.index += result.found_eq;
        const auto id = node->child_id(result.index);
        CCO_TRY(m_pool->release(std::move(*node)));
        node = m_pool->acquire(id, false);
        if (!node.has_value())
            return Err {node.error()};
    }
    const auto [index, found_eq] = result;
    return SearchResult {node->id(), index, found_eq};
}

auto Internal::find_minimum() -> Result<SearchResult>
{
    CCO_TRY_CREATE(node, m_pool->acquire(PageId::base(), false));
    auto id = node.id();

    while (!node.is_external()) {
        id = node.child_id(0);
        CCO_TRY(m_pool->release(std::move(node)));
        CCO_TRY_STORE(node, m_pool->acquire(id, false));
    }
    auto was_found = true;
    if (node.cell_count() == 0) {
        CCO_EXPECT_TRUE(id.is_base());
        was_found = false;
    }
    CCO_TRY(m_pool->release(std::move(node)));
    return SearchResult {id, 0, was_found};
}

auto Internal::find_maximum() -> Result<SearchResult>
{
    CCO_TRY_CREATE(node, m_pool->acquire(PageId::base(), false));
    auto id = node.id();

    while (!node.is_external()) {
        CCO_EXPECT_GT(node.cell_count(), 0);
        id = node.rightmost_child_id();
        CCO_TRY(m_pool->release(std::move(node)));
        CCO_TRY_STORE(node, m_pool->acquire(id, false));
    }
    auto was_found = true;
    Index index {};

    if (node.cell_count() == 0) {
        CCO_EXPECT_TRUE(id.is_base());
        was_found = false;
    } else {
        index = node.cell_count() - 1;
    }
    CCO_TRY(m_pool->release(std::move(node)));
    return SearchResult {id, index, was_found};
}

auto Internal::positioned_insert(Position position, BytesView key, BytesView value) -> Result<void>
{
    CCO_EXPECT_LE(key.size(), m_maximum_key_size);
    auto [node, index] = std::move(position);
    m_scratch.reset();

    CCO_TRY_CREATE(cell, make_cell(key, value, true));
    node.insert_at(index, cell);
    m_cell_count++;

    if (node.is_overflowing())
        return balance_after_overflow(std::move(node));
    return m_pool->release(std::move(node));
}

auto Internal::positioned_modify(Position position, BytesView value) -> Result<void>
{
    auto [node, index] = std::move(position);
    auto old_cell = node.read_cell(index);
    // Make a copy of the key. The data backing the old key slice may be written over when we call
    // remove_at() on the old cell.
    const auto key = btos(old_cell.key());
    m_scratch.reset();

    CCO_TRY_CREATE(new_cell, make_cell(stob(key), value, true));

    if (old_cell.overflow_size())
        CCO_TRY(m_pool->destroy_chain(old_cell.overflow_id(), old_cell.overflow_size()));

    node.remove_at(index, old_cell.size());
    node.insert_at(index, new_cell);

    if (node.is_overflowing())
        return balance_after_overflow(std::move(node));
    return m_pool->release(std::move(node));
}

auto Internal::positioned_remove(Position position) -> Result<void>
{
    auto [node, index] = std::move(position);
    CCO_EXPECT_TRUE(node.is_external());
    CCO_EXPECT_LT(index, node.cell_count());
    CCO_EXPECT_GT(m_cell_count, 0);
    m_scratch.reset();
    m_cell_count--;

    auto cell = node.read_cell(index);
    auto anchor = btos(cell.key());
    if (cell.overflow_size()) {
        auto was_destroyed = m_pool->destroy_chain(cell.overflow_id(), cell.overflow_size());
        if (!was_destroyed.has_value()) {
            CCO_TRY(m_pool->release(std::move(node)));
            return Err {was_destroyed.error()};
        }
    }

    node.remove_at(index, node.read_cell(index).size());
    // TODO: Figure out how to avoid storing the anchor. It's easy to either use the call stack or an actual stack to backtrack,
    //       however, some rebalancing operations can cause ancestor nodes to split, which can invalidate the stack.
    return balance_after_underflow(std::move(node), stob(anchor));
}

auto Internal::balance_after_overflow(Node node) -> Result<void>
{
    CCO_EXPECT_TRUE(node.is_overflowing());
    while (node.is_overflowing()) {
        if (node.overflow_cell().is_attached())
            m_scratch.reset();

        if (node.id().is_base()) {
            CCO_TRY_STORE(node, split_root(std::move(node)));
        } else {
            CCO_TRY_STORE(node, split_non_root(std::move(node)));
        }
    }
    return m_pool->release(std::move(node));
}

auto Internal::balance_after_underflow(Node node, BytesView anchor) -> Result<void>
{
    while (node.is_underflowing()) {
        m_scratch.reset();

        if (node.id().is_base()) {
            if (!node.cell_count())
                return fix_root(std::move(node));
            break;
        } else {
            CCO_TRY_CREATE(parent, m_pool->acquire(node.parent_id(), true));
            // NOTE: Searching for the anchor key from the node we took from should always
            //       give us the correct cell ID due to the B+-tree ordering rules.
            const auto [index, found_eq] = parent.find_ge(anchor);
            CCO_TRY_CREATE(was_fixed, fix_non_root(std::move(node), parent, index + found_eq));

            if (!was_fixed)
                return m_pool->release(std::move(parent));
            node = std::move(parent);
        }
    }
    return m_pool->release(std::move(node));
}

// TODO: Include the "adding of one to an index if the key was found in an internal node" to the internal node search behavior.

/**
 * Balancing routine for fixing an over-full root node.
 */
auto Internal::split_root(Node root) -> Result<Node>
{
    CCO_EXPECT_TRUE(root.id().is_base());
    CCO_EXPECT_TRUE(root.is_overflowing());

    CCO_TRY_CREATE(child, m_pool->allocate(root.type()));
    ::cco::split_root(root, child);

    CCO_TRY(maybe_fix_child_parent_connections(child));
    CCO_EXPECT_TRUE(child.is_overflowing());
    return child;
}

auto Internal::split_non_root(Node node) -> Result<Node>
{
    CCO_EXPECT_FALSE(node.id().is_base());
    CCO_EXPECT_FALSE(node.parent_id().is_null());
    CCO_EXPECT_TRUE(node.is_overflowing());

    CCO_TRY_CREATE(parent, m_pool->acquire(node.parent_id(), true));
    CCO_TRY_CREATE(sibling, m_pool->allocate(node.type()));

    auto separator = ::cco::split_non_root(node, sibling, m_scratch.get());
    auto [index, found_eq] = parent.find_ge(separator.key());
    CCO_EXPECT_FALSE(found_eq);

    if (node.is_external() && !sibling.right_sibling_id().is_null()) {
        CCO_TRY_CREATE(right, m_pool->acquire(sibling.right_sibling_id(), true));
        right.set_left_sibling_id(sibling.id());
        CCO_TRY(m_pool->release(std::move(right)));
    }

    parent.insert_at(index, separator);
    CCO_EXPECT_FALSE(node.is_overflowing());
    CCO_EXPECT_FALSE(sibling.is_overflowing());

    const auto offset = !parent.is_overflowing();
    parent.set_child_id(index + offset, sibling.id());
    CCO_TRY(maybe_fix_child_parent_connections(sibling));
    CCO_TRY(m_pool->release(std::move(sibling)));
    CCO_TRY(m_pool->release(std::move(node)));
    return parent;
}

auto Internal::maybe_fix_child_parent_connections(Node &node) -> Result<void>
{
    if (!node.is_external()) {
        const auto parent_id = node.id();

        const auto fix_connection = [parent_id, this](PageId child_id) -> Result<void> {
            CCO_TRY_CREATE(child, m_pool->acquire(child_id, true));
            child.set_parent_id(parent_id);
            return m_pool->release(std::move(child));
        };


        for (Index index {}; index <= node.cell_count(); ++index)
            CCO_TRY(fix_connection(node.child_id(index)));

        if (node.is_overflowing())
            CCO_TRY(fix_connection(node.overflow_cell().left_child_id()));
    }
    return {};
}

/**
 * Note that the key and value must exist until the cell is safely embedded in the tree. If
 * the tree is balanced and there are no overflow cells then this is guaranteed to be true.
 */
auto Internal::make_cell(BytesView key, BytesView value, bool is_external) -> Result<Cell>
{
    if (is_external) {
        auto cell = ::cco::make_external_cell(key, value, m_pool->page_size());
        if (!cell.overflow_id().is_null()) {
            const auto overflow_value = value.range(cell.local_value().size());
            CCO_TRY_CREATE(id, m_pool->allocate_chain(overflow_value));
            cell.set_overflow_id(id);
        }
        return cell;
    } else {
        return ::cco::make_internal_cell(key, m_pool->page_size());
    }
}

auto Internal::fix_non_root(Node node, Node &parent, Index index) -> Result<bool>
{
    CCO_EXPECT_FALSE(node.id().is_base());
    CCO_EXPECT_FALSE(node.is_overflowing());
    CCO_EXPECT_FALSE(parent.is_overflowing());
    if (index > 0) {
        CCO_TRY_CREATE(Lc, m_pool->acquire(parent.child_id(index - 1), true));
        if (can_merge_siblings(Lc, node, parent.read_cell(index - 1))) {
            merge_right(Lc, node, parent, index - 1);
            CCO_TRY(maybe_fix_child_parent_connections(Lc));
            if (node.is_external() && !node.right_sibling_id().is_null()) {
                CCO_TRY_CREATE(rc, m_pool->acquire(node.right_sibling_id(), true));
                rc.set_left_sibling_id(Lc.id());
            }
            CCO_TRY(m_pool->destroy(std::move(node)));
            return true;
        }
    }
    if (index < parent.cell_count()) {
        CCO_TRY_CREATE(rc, m_pool->acquire(parent.child_id(index + 1), true));
        if (can_merge_siblings(node, rc, parent.read_cell(index))) {
            merge_left(node, rc, parent, index);
            CCO_TRY(maybe_fix_child_parent_connections(node));
            if (rc.is_external() && !rc.right_sibling_id().is_null()) {
                CCO_TRY_CREATE(rrc, m_pool->acquire(rc.right_sibling_id(), true));
                rrc.set_left_sibling_id(node.id());
            }
            CCO_TRY(m_pool->destroy(std::move(rc)));
            return true;
        }
    }
    // Skip the rotation but keep on rebalancing.
    if (!node.is_underflowing())
        return true;

    auto maybe_fix_parent = [&]() -> Result<bool> {
        if (parent.is_overflowing()) {
            const auto id = parent.id();
            CCO_TRY(m_pool->release(std::move(node)));
            CCO_TRY(balance_after_overflow(std::move(parent)));
            CCO_TRY_STORE(parent, m_pool->acquire(id, true));
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
        CCO_TRY_CREATE(left_sibling, m_pool->acquire(parent.child_id(index - 1), true));
        const auto cell_count = left_sibling.cell_count();
        siblings[0] = {std::move(left_sibling), cell_count};
    }
    if (index < parent.cell_count()) {
        CCO_TRY_CREATE(right_sibling, m_pool->acquire(parent.child_id(index + 1), true));
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
        CCO_EXPECT_NE(left_sibling, std::nullopt);
        siblings[1].node.reset();
        CCO_TRY(rotate_right(parent, *left_sibling, node, index - 1));
        CCO_EXPECT_FALSE(node.is_overflowing());
        left_sibling.reset();
        return maybe_fix_parent();
    } else {
        auto [right_sibling, cell_count] = std::move(siblings[1]);
        CCO_EXPECT_NE(right_sibling, std::nullopt);
        siblings[0].node.reset();
        CCO_TRY(rotate_left(parent, node, *right_sibling, index));
        CCO_EXPECT_FALSE(node.is_overflowing());
        right_sibling.reset();
        return maybe_fix_parent();
    }
}

auto Internal::fix_root(Node node) -> Result<void>
{
    CCO_EXPECT_TRUE(node.id().is_base());
    CCO_EXPECT_TRUE(node.is_underflowing());

    // If the root is external here, the whole tree must be empty.
    if (!node.is_external()) {
        CCO_TRY_CREATE(child, m_pool->acquire(node.rightmost_child_id(), true));

        // We don't have enough room to transfer the child contents into the root, due to the storage header. In
        // this case, we'll just split the child and let the median cell be inserted into the root. Note that
        // the child needs an overflow cell for the split routine to work. We'll just fake it by extracting an
        // arbitrary cell and making it the overflow cell.
        if (child.usable_space() < node.header_offset()) {
            child.set_overflow_cell(child.extract_cell(0, m_scratch.get()));
            CCO_TRY(m_pool->release(std::move(node)));
            CCO_TRY(split_non_root(std::move(child)));
            CCO_TRY_STORE(node, find_root(true));
        } else {
            ::cco::merge_root(node, child);
            CCO_TRY(m_pool->destroy(std::move(child)));
        }
        auto result = maybe_fix_child_parent_connections(node);
        CCO_TRY(m_pool->release(std::move(node)));
        return result;
    }
    return m_pool->release(std::move(node));
}

auto Internal::save_state(FileHeader &header) const -> void
{
    m_pool->save_state(header);
    header.record_count = m_cell_count;
}

auto Internal::load_state(const FileHeader &header) -> void
{
    m_pool->load_state(header);
    m_cell_count = header.record_count;
}

auto Internal::rotate_left(Node &parent, Node &Lc, Node &rc, Index index) -> Result<void>
{
    if (Lc.is_external()) {
        return external_rotate_left(parent, Lc, rc, index);
    } else {
        return internal_rotate_left(parent, Lc, rc, index);
    }
}

auto Internal::rotate_right(Node &parent, Node &Lc, Node &rc, Index index) -> Result<void>
{
    if (Lc.is_external()) {
        return external_rotate_right(parent, Lc, rc, index);
    } else {
        return internal_rotate_right(parent, Lc, rc, index);
    }
}

auto Internal::external_rotate_left(Node &parent, Node &Lc, Node &rc, Index index) -> Result<void>
{
    CCO_EXPECT_FALSE(parent.is_external());
    CCO_EXPECT_GT(parent.cell_count(), 0);
    CCO_EXPECT_GT(rc.cell_count(), 1);

    auto old_separator = parent.read_cell(index);
    auto lowest = rc.extract_cell(0, m_scratch.get());
    CCO_TRY_CREATE(new_separator, make_cell(rc.read_key(0), {}, false));
    new_separator.set_left_child_id(Lc.id());
    new_separator.detach(m_scratch.get(), true);

    // Parent might overflow.
    parent.remove_at(index, old_separator.size());
    parent.insert_at(index, new_separator);

    Lc.insert_at(Lc.cell_count(), lowest);
    CCO_EXPECT_FALSE(Lc.is_overflowing());
    return {};
}

auto Internal::external_rotate_right(Node &parent, Node &Lc, Node &rc, Index index) -> Result<void>
{
    CCO_EXPECT_FALSE(parent.is_external());
    CCO_EXPECT_GT(parent.cell_count(), 0);
    CCO_EXPECT_GT(Lc.cell_count(), 1);

    auto separator = parent.read_cell(index);
    auto highest = Lc.extract_cell(Lc.cell_count() - 1, m_scratch.get());
    CCO_TRY_CREATE(new_separator, make_cell(highest.key(), {}, false));
    new_separator.set_left_child_id(Lc.id());
    new_separator.detach(m_scratch.get(), true);

    // Parent might overflow.
    parent.remove_at(index, separator.size());
    parent.insert_at(index, new_separator);

    rc.insert_at(0, highest);
    CCO_EXPECT_FALSE(rc.is_overflowing());
    return {};
}

auto Internal::internal_rotate_left(Node &parent, Node &Lc, Node &rc, Index index) -> Result<void>
{
    CCO_EXPECT_FALSE(parent.is_external());
    CCO_EXPECT_FALSE(Lc.is_external());
    CCO_EXPECT_EQ(Lc.type(), rc.type());
    CCO_EXPECT_GT(parent.cell_count(), 0);
    CCO_EXPECT_GT(rc.cell_count(), 1);

    auto separator = parent.extract_cell(index, m_scratch.get());
    CCO_TRY_CREATE(child, m_pool->acquire(rc.child_id(0), true));
    separator.set_left_child_id(Lc.rightmost_child_id());
    child.set_parent_id(Lc.id());
    Lc.set_rightmost_child_id(child.id());
    CCO_TRY(m_pool->release(std::move(child)));
    Lc.insert_at(Lc.cell_count(), separator);
    CCO_EXPECT_FALSE(Lc.is_overflowing());

    auto lowest = rc.extract_cell(0, m_scratch.get());
    lowest.set_left_child_id(Lc.id());
    // Parent might overflow.
    parent.insert_at(index, lowest);
    return {};
}

auto Internal::internal_rotate_right(Node &parent, Node &Lc, Node &rc, Index index) -> Result<void>
{
    CCO_EXPECT_FALSE(parent.is_external());
    CCO_EXPECT_FALSE(Lc.is_external());
    CCO_EXPECT_EQ(Lc.type(), rc.type());
    CCO_EXPECT_GT(parent.cell_count(), 0);
    CCO_EXPECT_GT(Lc.cell_count(), 1);

    auto separator = parent.extract_cell(index, m_scratch.get());
    CCO_TRY_CREATE(child, m_pool->acquire(Lc.rightmost_child_id(), true));
    separator.set_left_child_id(child.id());
    child.set_parent_id(rc.id());
    CCO_TRY(m_pool->release(std::move(child)));
    Lc.set_rightmost_child_id(Lc.child_id(Lc.cell_count() - 1));
    rc.insert_at(0, separator);
    CCO_EXPECT_FALSE(rc.is_overflowing());

    auto highest = Lc.extract_cell(Lc.cell_count() - 1, m_scratch.get());
    highest.set_left_child_id(Lc.id());
    // The parent might overflow.
    parent.insert_at(index, highest);
    return {};
}

} // namespace cco
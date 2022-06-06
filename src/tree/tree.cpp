#include <optional>
#include "tree.h"
#include "cub/exception.h"
#include "page/cell.h"
#include "page/file_header.h"
#include "page/link.h"
#include "page/node.h"
#include "page/page.h"
#include "pool/interface.h"
#include "utils/layout.h"

namespace cub {

Tree::Tree(Parameters param)
    : m_scratch {get_max_local(param.buffer_pool->page_size())}
    , m_free_list {{param.buffer_pool, param.free_start, param.free_count}}
    , m_pool {param.buffer_pool}
    , m_node_count {param.node_count}
    , m_cell_count {param.cell_count} {}

auto Tree::find_root(bool is_writable) -> Node
{
    return acquire_node(PID::root(), is_writable);
}

auto Tree::find_ge(BytesView key, bool is_writable) -> Result
{
    auto node = find_root(is_writable);
    Node::SearchResult result;
    while (true) {
        result = node.find_ge(key);
        if (result.found_eq || node.is_external())
            break;
        node = acquire_node(node.child_id(result.index), is_writable);
    }
    const auto [index, found_eq] = result;
    return {std::move(node), index, found_eq};
}

auto Tree::collect_value(const Node &node, Index index) const -> std::string
{
    auto cell = node.read_cell(index);
    const auto local = cell.local_value();
    std::string result(cell.value_size(), '\x00');
    auto out = _b(result);

    // Note that it is possible to have no value stored locally but have an overflow page. The happens when
    // the key is of maximal length (i.e. get_max_local(m_header->page_size())).
    if (!local.is_empty())
        mem_copy(out, local, local.size());

    if (!cell.overflow_id().is_null()) {
        CUB_EXPECT_GT(cell.value_size(), cell.local_value().size());
        out.advance(local.size());
        collect_overflow_chain(cell.overflow_id(), out);
    }
    return result;
}

auto Tree::insert(BytesView key, BytesView value) -> void
{
    if (key.is_empty())
        throw InvalidArgumentError {"Key cannot be empty"};

    auto [node, index, found_eq] = find_ge(key, true);

    if (key.size() > get_max_local(node.size()))
        throw InvalidArgumentError {"Key exceeds maximum allowed length"};

    if (found_eq) {
        positioned_modify({std::move(node), index}, value);
    } else {
        positioned_insert({std::move(node), index}, key, value);
    }
}

auto Tree::remove(BytesView key) -> bool
{
    auto [node, index, found_eq] = find_ge(key, true);
    if (found_eq)
        positioned_remove({std::move(node), index});
    return found_eq;
}

auto Tree::positioned_insert(Position position, BytesView key, BytesView value) -> void
{
    CUB_EXPECT_LE(key.size(), get_max_local(m_pool->page_size()));
    auto [node, index] = std::move(position);
    auto cell = make_cell(key, value);
    node.insert_at(index, std::move(cell));
    m_cell_count++;
    
    if (node.is_overflowing())
        balance_after_overflow(std::move(node));
}

auto Tree::positioned_modify(Position position, BytesView value) -> void
{
    auto [node, index] = std::move(position);
    auto old_cell = node.read_cell(index);
    // Make a copy of the key. The data backing the old key slice will be written over when we call
    // remove_at() on the old cell.
    const auto key = _s(old_cell.key());
    auto new_cell = make_cell(_b(key), value);

    if (old_cell.overflow_size())
        destroy_overflow_chain(old_cell.overflow_id(), old_cell.overflow_size());

    if (!node.is_external())
        new_cell.set_left_child_id(old_cell.left_child_id());

    node.remove_at(index, old_cell.size());
    node.insert_at(index, std::move(new_cell));

    if (node.is_overflowing())
        balance_after_overflow(std::move(node));
}

auto Tree::positioned_remove(Position position) -> void
{
    auto [node, index] = std::move(position);
    CUB_EXPECT_LT(index, node.cell_count());
    m_cell_count--;

    auto cell = node.read_cell(index);
    auto anchor = _s(cell.key());
    if (cell.overflow_size())
        destroy_overflow_chain(cell.overflow_id(), cell.overflow_size());

    // Here, we swap the cell to be removed with its inorder predecessor (always exists
    // if the node is internal).
    if (!node.is_external()) {
        auto [other, other_index] = find_local_max(acquire_node(node.child_id(index), true));
        auto other_cell = other.extract_cell(other_index, m_scratch.get());
        anchor = _s(other_cell.key()); // TODO: Avoid this if possible. Seems unnecessary?
        other_cell.set_left_child_id(node.child_id(index));
        node.remove_at(index, node.read_cell(index).size());

        // The current node can overflow here. We may have to rebalance before proceeding.
        node.insert_at(index, std::move(other_cell));
        if (node.is_overflowing()) {
            const auto id = other.id();
            other.take();
            balance_after_overflow(std::move(node));
            node = acquire_node(id, true);
        } else {
            node = std::move(other);
        }
    } else {
        node.remove_at(index, node.read_cell(index).size());
    }
    maybe_balance_after_underflow(std::move(node), _b(anchor));
}

auto Tree::find_local_min(Node root) -> Position
{
    while (!root.is_external())
        root = acquire_node(root.child_id(0), true);
    return {std::move(root), 0};
}

auto Tree::find_local_max(Node root) -> Position
{
    while (!root.is_external())
        root = acquire_node(root.rightmost_child_id(), true);
    const auto index = root.cell_count() - 1;
    return {std::move(root), index};
}

auto Tree::allocate_node(PageType type) -> Node
{
    auto page = m_free_list.pop();
    if (page) {
        page->set_type(type);
    } else {
        page = m_pool->allocate(type);
    }
    m_node_count++;
    return {std::move(*page), true};
}

auto Tree::acquire_node(PID id, bool is_writable) -> Node
{
    return {m_pool->acquire(id, is_writable), false};
}

auto Tree::destroy_node(Node node) -> void
{
    CUB_EXPECT_FALSE(node.is_overflowing());
    m_free_list.push(node.take());
    m_node_count--;
}

auto Tree::balance_after_overflow(Node node) -> void
{
    CUB_EXPECT(node.is_overflowing());
    while (node.is_overflowing()) {
        if (node.id().is_root()) {
            node = split_root(std::move(node));
        } else {
            node = split_non_root(std::move(node));
        }
    }
}

auto Tree::maybe_balance_after_underflow(Node node, BytesView anchor) -> void
{
    // We always attempt to rebalance up to the root.
    while (!node.id().is_root()) {
        auto parent = acquire_node(node.parent_id(), true);
        // NOTE: Searching for the anchor key from the node we removed a cell from should
        //       always give us the correct cell ID due to the B-Tree ordering rules.
        const auto [index, found_eq] = parent.find_ge(anchor);
        if (!fix_non_root(std::move(node), parent, index))
            return;
        node = std::move(parent);
    }
    CUB_EXPECT_TRUE(node.id().is_root());
    if (!node.cell_count())
        fix_root(std::move(node));
}

/**
 * Balancing routine for fixing an over-full root node.
 */
auto Tree::split_root(Node node) -> Node
{
    CUB_EXPECT_TRUE(node.id().is_root());
    CUB_EXPECT_TRUE(node.is_overflowing());

    auto child = allocate_node(node.type());
    do_split_root(node, child);

    maybe_fix_child_parent_connections(child);
    CUB_EXPECT_TRUE(child.is_overflowing());
    return child;
}

auto Tree::split_non_root(Node node) -> Node
{
    CUB_EXPECT(node.is_overflowing());
    CUB_EXPECT_FALSE(node.id().is_root());

    // Sibling and source nodes,
    auto sibling = allocate_node(node.type());
    auto parent = acquire_node(node.parent_id(), true);

    // Transformation:
    //                      index ---.
    //                               |
    //                               v
    //      P:[...,                  x, ...]                 P:[...,         c,          x, ...]
    // [...]       L:[a, b, c, d, e]   G    [...]  -->  [...]       L:[a, b]    R:[d, e]   G    [...]
    //            A     B  C  D  E  F                              A     B  C  D     E  F
    auto median = do_split_non_root(node, sibling, m_scratch.get());
    auto [index, found_eq] = parent.find_ge(median.key());
    CUB_EXPECT_FALSE(found_eq);

    parent.insert_at(index, std::move(median));
    CUB_EXPECT_FALSE(node.is_overflowing());
    CUB_EXPECT_FALSE(sibling.is_overflowing());

    const auto offset = !parent.is_overflowing();
    parent.set_child_id(index + offset, sibling.id());
    maybe_fix_child_parent_connections(sibling);
    return parent;
}

auto Tree::maybe_fix_child_parent_connections(Node &node) -> void
{
    if (!node.is_external()) {
        const auto fix_connection = [&node, this](PID child_id) {
            auto child = acquire_node(child_id, true);
            child.set_parent_id(node.id());
        };

        for (Index index {}; index <= node.cell_count(); ++index)
            fix_connection(node.child_id(index));

        if (node.is_overflowing())
            fix_connection(node.overflow_cell().left_child_id());
    }
}

/**
 * Note that the key and value must exist until the cell is safely embedded in the B-Tree. If
 * the tree is balanced and there are no overflow cells then this is guaranteed to be true.
 */
auto Tree::make_cell(BytesView key, BytesView value) -> Cell
{
    CUB_EXPECT_FALSE(key.is_empty());
    const auto local_value_size = get_local_value_size(key.size(), value.size(), m_pool->page_size());
    Cell::Parameters param;
    param.key = key;
    param.local_value = value;
    param.value_size = value.size();

    if (local_value_size != value.size()) {
        CUB_EXPECT_LT(local_value_size, value.size());
        param.local_value.truncate(local_value_size);
        param.overflow_id = allocate_overflow_chain(value.range(local_value_size));
    }
    return Cell {param};
}

auto Tree::allocate_overflow_chain(BytesView overflow) -> PID
{
    CUB_EXPECT_FALSE(overflow.is_empty());
    std::optional<Link> prev;
    auto head = PID::root();

    while (!overflow.is_empty()) {
        auto page = !m_free_list.is_empty()
            ? *m_free_list.pop()
            : m_pool->allocate(PageType::OVERFLOW_LINK);
        page.set_type(PageType::OVERFLOW_LINK);
        Link link {std::move(page)};
        auto content = link.mut_content(std::min(overflow.size(), link.content_size()));
        mem_copy(content, overflow, content.size());
        overflow.advance(content.size());

        if (prev) {
            prev->set_next_id(link.id());
            prev.reset();
        } else {
            head = link.id();
        }
        prev = std::move(link);
    }
    return head;
}

auto Tree::collect_overflow_chain(PID id, Bytes out) const -> void
{
    while (!out.is_empty()) {
        auto page = m_pool->acquire(id, false);
        CUB_EXPECT_EQ(page.type(), PageType::OVERFLOW_LINK);
        Link link {std::move(page)};
        auto content = link.ref_content();
        const auto chunk = std::min(out.size(), content.size());
        mem_copy(out, content, chunk);
        out.advance(chunk);
        id = link.next_id();
    }
}

auto Tree::destroy_overflow_chain(PID id, Size size) -> void
{
    while (size) {
        auto page = m_pool->acquire(id, true);
        CUB_EXPECT_EQ(page.type(), PageType::OVERFLOW_LINK); // TODO
        Link link {std::move(page)};
        id = link.next_id();
        size -= std::min(size, link.ref_content().size());
        m_free_list.push(link.take());
    }
}

auto Tree::fix_non_root(Node node, Node &parent, Index index) -> bool
{
    CUB_EXPECT_FALSE(node.id().is_root());
    CUB_EXPECT_FALSE(node.is_overflowing());
    CUB_EXPECT_FALSE(parent.is_overflowing());
    if (index > 0) {
        auto Lc = acquire_node(parent.child_id(index - 1), true);
        if (can_merge_siblings(Lc, node, parent.read_cell(index - 1))) {
            merge_right(Lc, node, parent, index - 1);
            maybe_fix_child_parent_connections(Lc);
            destroy_node(std::move(node));
            return true;
        }
    }
    if (index < parent.cell_count()) {
        auto rc = acquire_node(parent.child_id(index + 1), true);
        if (can_merge_siblings(node, rc, parent.read_cell(index))) {
            merge_left(node, rc, parent, index);
            maybe_fix_child_parent_connections(node);
            destroy_node(std::move(rc));
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

            // This may not actually be the correct parent, depending on whether index
            // was before or after the median. Either way, both new nodes resulting from
            // the split are more-or-less balanced, so we'll stop rebalancing here.
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
        auto left_sibling = acquire_node(parent.child_id(index - 1), true);
        const auto cell_count = left_sibling.cell_count();
        siblings[0] = {std::move(left_sibling), cell_count};
    }
    if (index < parent.cell_count()) {
        auto right_sibling = acquire_node(parent.child_id(index + 1), true);
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
        CUB_EXPECT_NE(left_sibling, std::nullopt);
        siblings[1].node.reset();
        rotate_right(parent, *left_sibling, node, index - 1);
        CUB_EXPECT_FALSE(node.is_overflowing());
        left_sibling.reset();
        return maybe_fix_parent();
    } else {
        auto [right_sibling, cell_count] = std::move(siblings[1]);
        CUB_EXPECT_NE(right_sibling, std::nullopt);
        siblings[0].node.reset();
        rotate_left(parent, node, *right_sibling, index);
        CUB_EXPECT_FALSE(node.is_overflowing());
        right_sibling.reset();
        return maybe_fix_parent();
    }
    return true;
}

auto Tree::fix_root(Node node) -> void
{
    CUB_EXPECT_TRUE(node.id().is_root());
    CUB_EXPECT_TRUE(node.is_underflowing());

    // If the root is external here, the whole tree must be empty.
    if (!node.is_external()) {
        auto child = acquire_node(node.rightmost_child_id(), true);

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
            do_merge_root(node, child);
            destroy_node(std::move(child));
        }
        maybe_fix_child_parent_connections(node);
    }
}

auto Tree::save_header(FileHeader &header) -> void
{
    header.set_node_count(m_node_count);
    header.set_key_count(m_cell_count);
    m_free_list.save_header(header);
}

auto Tree::load_header(const FileHeader &header) -> void
{
    m_node_count = header.node_count();
    m_cell_count = header.key_count();
    m_free_list.load_header(header);
}

auto Tree::rotate_left(Node &parent, Node &Lc, Node &rc, Index index) -> void
{
    CUB_EXPECT_FALSE(parent.is_external());
    CUB_EXPECT_GT(parent.cell_count(), 0);
    CUB_EXPECT_GT(rc.cell_count(), 1);

    auto separator = parent.extract_cell(index, m_scratch.get());
    if (!Lc.is_external()) {
        auto child = acquire_node(rc.child_id(0), true);
        separator.set_left_child_id(Lc.rightmost_child_id());
        Lc.set_rightmost_child_id(child.id());
        child.set_parent_id(Lc.id());
    } else {
        separator.set_left_child_id(PID{});
    }
    // Lc was deficient so it cannot overflow here.
    Lc.insert_at(Lc.cell_count(), std::move(separator));
    CUB_EXPECT_FALSE(Lc.is_overflowing());

    auto lowest = rc.extract_cell(0, m_scratch.get());
    lowest.set_left_child_id(Lc.id());
    // Parent might overflow.
    parent.insert_at(index, std::move(lowest));
}

auto Tree::rotate_right(Node &parent, Node &Lc, Node &rc, Index index) -> void
{
    CUB_EXPECT_FALSE(parent.is_external());
    CUB_EXPECT_GT(parent.cell_count(), 0);
    CUB_EXPECT_GT(Lc.cell_count(), 1);
    auto separator = parent.extract_cell(index, m_scratch.get());
    if (!rc.is_external()) {
        CUB_EXPECT_FALSE(Lc.is_external());
        auto child = acquire_node(Lc.rightmost_child_id(), true);
        separator.set_left_child_id(child.id());
        Lc.set_rightmost_child_id(Lc.child_id(Lc.cell_count() - 1));
        child.set_parent_id(rc.id());
    } else {
        separator.set_left_child_id(PID{});
    }
    // Rc was deficient so it cannot overflow here.
    rc.insert_at(0, std::move(separator));
    CUB_EXPECT_FALSE(rc.is_overflowing());

    auto highest = Lc.extract_cell(Lc.cell_count() - 1, m_scratch.get());
    highest.set_left_child_id(Lc.id());
    // The parent might overflow.
    parent.insert_at(index, std::move(highest));
}

auto do_split_root(Node &root, Node &child) -> void
{
    // Copy the cells.
    auto offset = root.cell_area_offset();
    auto size = root.size() - offset;
    child.page().write(root.page().range(offset).truncate(size), offset);

    // Copy the header and cell pointers.
    offset = child.header_offset();
    size = NodeLayout::HEADER_SIZE + root.cell_count()*CELL_POINTER_SIZE;
    child.page().write(root.page().range(root.header_offset()).truncate(size), offset);

    CUB_EXPECT_TRUE(root.is_overflowing());
    child.set_overflow_cell(root.take_overflow_cell());

    root.reset(true);
    root.page().set_type(PageType::INTERNAL_NODE);
    root.set_rightmost_child_id(child.id());
    child.set_parent_id(PID::root());
}

auto do_merge_root(Node &root, Node &child) -> void
{
    CUB_EXPECT(root.rightmost_child_id() == child.id());
    child.defragment();

    // Copy the cell content area.
    auto offset = child.cell_area_offset();
    auto size = child.size() - offset;
    root.page().write(child.page().range(offset).truncate(size), offset);

    // Copy the header and cell pointers.
    offset = root.header_offset();
    size = NodeLayout::HEADER_SIZE + child.cell_count()*CELL_POINTER_SIZE;
    root.page().write(child.page().range(child.header_offset()).truncate(size), offset);
    root.page().set_type(child.type());
}

auto do_split_non_root(Node &Ln, Node &rn, Scratch scratch) -> Cell
{
    // get the overflow cell. The caller should make sure the node is overflowing
    // before calling this method.
    CUB_EXPECT_TRUE(Ln.is_overflowing());
    auto overflow = Ln.take_overflow_cell();

    // Include the overflow cell in our count.
    const auto n = Ln.cell_count() + 1;
    const auto overflow_key = overflow.key();

    // Figure out where the overflow cell should go.
    const auto [overflow_idx, should_be_false]{Ln.find_ge(overflow.key())};
    CUB_EXPECT_FALSE(should_be_false);

    // TODO: Get rid of the optional(s). It's a hack from when Cell was changed to be non-copyable.
    std::optional<Cell> median {};
    std::optional<Cell> temp {std::move(overflow)};
    auto median_index = n / 2;
    if (overflow_idx == median_index) {
        median = std::move(*temp);
    } else {
        median_index -= overflow_idx < median_index;
        median = Ln.extract_cell(median_index, std::move(scratch));
    }
    if (Ln.is_external()) {
        rn.set_right_sibling_id(Ln.right_sibling_id());
        Ln.set_right_sibling_id(rn.id());
    } else {
        rn.set_rightmost_child_id(Ln.rightmost_child_id());
        Ln.set_rightmost_child_id(median->left_child_id());
    }
    rn.set_parent_id(Ln.parent_id());
    median->set_left_child_id(Ln.id());

    // Transformation:
    //    P:[...]                            P:[...,         c,         ...]
    // ...       L:[a, b, c, d, e]   -->  ...       L:[a, b]    R:[d, e]
    //          A     B  C  D  E  F                A     B  C  D     E  F

    // Transfer cells after the median cell to the right sibling node.
    const auto cell_count = Ln.cell_count();
    for (auto index = median_index; index < cell_count; ++index) {
        auto cell = Ln.read_cell(median_index);
        const auto cell_size = cell.size();
        rn.insert_at(rn.cell_count(), std::move(cell));
        Ln.remove_at(median_index, cell_size);
    }
    const auto do_transfer = [](Node &target, Cell cell) {
        const auto [index, found_eq]{target.find_ge(cell.key())};
        CUB_EXPECT_FALSE(found_eq);
        target.insert_at(index, std::move(cell));
    };
    switch (compare_three_way(overflow_key, median->key())) {
        case ThreeWayComparison::EQ:
            // No transfer. The median/overflow key is returned.
            break;
        case ThreeWayComparison::LT:
            do_transfer(Ln, std::move(*temp));
            break;
        case ThreeWayComparison::GT:
            do_transfer(rn, std::move(*temp));
            break;
    }
    return std::move(*median);
}

} // cub

#include "tree.h"
#include "pager/pager.h"
#include "utils/utils.h"

namespace Calico {

[[nodiscard]]
static auto is_overflowing(const Node &node) -> bool
{
    return node.overflow.has_value();
}

[[nodiscard]]
static auto is_underflowing(const Node &node) -> bool
{
    return node.header.cell_count == 0;
}

static auto reset_node(Node &node) -> void
{
    CALICO_EXPECT_FALSE(is_overflowing(node));
    auto header = NodeHeader {};
    auto scratch = node.scratch;
    header.cell_start = static_cast<PageSize>(node.page.size());

    node = Node {std::move(node).take(), scratch};
    node.header = header;
}

class BPlusTreeInternal {
public:
    [[nodiscard]]
    static auto is_pointer_map(BPlusTree &tree, Id pid) -> bool
    {
        return tree.m_pointers.lookup(pid) == pid;
    }

    [[nodiscard]]
    static auto collect_value(BPlusTree &tree, Node node, Size index) -> tl::expected<std::string, Status>
    {
        auto cell = read_cell(node, index);
        const Slice local {cell.key + cell.key_size, cell.local_ps - cell.key_size};

        auto total = local.to_string();
        tree.release(std::move(node));
        total.resize(cell.total_ps - cell.key_size);

        if (local.size() != total.size()) {
            CALICO_EXPECT_LT(local.size(), total.size());
            Span out {total};
            out.advance(local.size());
            Calico_Try_R(tree.m_overflow.read_chain(read_overflow_id(cell), out));
        }
        return total;
    }

    [[nodiscard]]
    static auto find_external_slot(BPlusTree &tree, const Slice &key, Node node) -> tl::expected<SearchResult, Status>
    {
        for (; ; ) {
            Node::Iterator itr {node};
            const auto exact = itr.seek(key);

            if (node.header.is_external) {
                return SearchResult {std::move(node), itr.index(), exact};
            }
            const auto next_id = read_child_id(node, itr.index() + exact);
            CALICO_EXPECT_NE(next_id, node.page.id()); // Infinite loop.
            tree.release(std::move(node));
            Calico_Put_R(node, tree.acquire(next_id));
        }
    }

    static auto find_external_slot(BPlusTree &tree, const Slice &key) -> tl::expected<SearchResult, Status>
    {
        Calico_New_R(root, tree.acquire(Id::root()));
        return find_external_slot(tree, key, std::move(root));
    }

    [[nodiscard]]
    static auto find_parent_id(BPlusTree &tree, Id pid) -> tl::expected<Id, Status>
    {
        Calico_New_R(entry, tree.m_pointers.read_entry(pid));
        return entry.back_ptr;
    }

    [[nodiscard]]
    static auto fix_parent_id(BPlusTree &tree, Id pid, Id parent_id, PointerMap::Type type = PointerMap::NODE) -> tl::expected<void, Status>
    {
        PointerMap::Entry entry {parent_id, type};
        Calico_Try_R(tree.m_pointers.write_entry(pid, entry));
        return {};
    }

    [[nodiscard]]
    static auto maybe_fix_overflow_chain(BPlusTree &tree, const Cell &cell, Id parent_id) -> tl::expected<void, Status>
    {
        if (cell.local_ps != cell.total_ps) {
            CALICO_EXPECT_LT(cell.local_ps, cell.total_ps);
            return fix_parent_id(tree, read_overflow_id(cell), parent_id, PointerMap::OVERFLOW_HEAD);
        }
        return {};
    }

    [[nodiscard]]
    static auto fix_links(BPlusTree &tree, Node &node) -> tl::expected<void, Status>
    {
        if (node.header.is_external) {
            for (Size index {}; index < node.header.cell_count; ++index) {
                const auto cell = read_cell(node, index);
                Calico_Try_R(maybe_fix_overflow_chain(tree, cell, node.page.id()));
            }
            if (node.overflow) {
                Calico_Try_R(maybe_fix_overflow_chain(tree, *node.overflow, node.page.id()));
            }
        } else {
            for (Size index {}; index <= node.header.cell_count; ++index) {
                Calico_Try_R(fix_parent_id(tree, read_child_id(node, index), node.page.id()));
            }
            if (node.overflow) {
                Calico_Try_R(fix_parent_id(tree, read_child_id(*node.overflow), node.page.id()));
            }
        }
        return {};
    }

    /*
     * Build a cell directly in an external node, if the cell will fit (may allocate overflow chain pages). If the cell does not fit, build
     * it in scratch memory and set it as the node's overflow cell. The caller should then call the appropriate overflow resolution routine.
     */
    [[nodiscard]]
    static auto emplace_cell(BPlusTree &tree, Node &node, Size index, const Slice &key, const Slice &value) -> tl::expected<void, Status>
    {
        CALICO_EXPECT_TRUE(node.header.is_external);

        auto local_size = value.size();
        const auto total_size = determine_cell_size(key.size(), local_size, *node.meta);
        const auto local = value.range(0, local_size);
        const auto remote = value.range(local_size);

        Id overflow_id {};
        if (!remote.is_empty()) {
            Calico_Put_R(overflow_id, tree.m_overflow.write_chain(node.page.id(), remote));
        }

        const auto emplace = [k = key, v = local, o = overflow_id, value](auto *out) {
            ::Calico::emplace_cell(out, value.size(), k, v, o);
        };

        if (const auto offset = allocate_block(node, PageSize(index), PageSize(total_size))) {
            // Write directly into the node.
            emplace(node.page.data() + offset);
        } else {
            // The node has overflowed. Write the cell to scratch memory.
            auto *scratch = tree.scratch(0);
            emplace(scratch);
            node.overflow = parse_external_cell(*node.meta, scratch);
            node.overflow->is_free = true;
        }
        return {};
    }

    [[nodiscard]]
    static auto resolve_overflow(BPlusTree &tree, Node node) -> tl::expected<void, Status>
    {
        tl::expected<Node, Status> (*fp)(BPlusTree &, Node);

        while (is_overflowing(node)) {
            if (node.page.id().is_root()) {
                fp = split_root;
            } else {
                fp = split_non_root;
            }
            Calico_New_R(temp, fp(tree, std::move(node)));
            node = std::move(temp);
        }
        tree.release(std::move(node));
        return {};
    }

    [[nodiscard]]
    static auto split_root(BPlusTree &tree, Node root) -> tl::expected<Node, Status>
    {
        Calico_New_R(child, tree.allocate(root.header.is_external));

        // Copy the cells.
        static constexpr auto after_root_headers = FileHeader::SIZE + NodeHeader::SIZE;
        auto out = child.page.span(after_root_headers, root.page.size() - after_root_headers);
        mem_copy(out, root.page.view(after_root_headers, out.size()));

        // Copy the header and cell pointers.
        child.header = root.header;
        out = child.page.span(NodeHeader::SIZE, root.header.cell_count * sizeof(PageSize));
        mem_copy(out, root.page.view(after_root_headers, out.size()));

        CALICO_EXPECT_TRUE(is_overflowing(root));
        std::swap(child.overflow, root.overflow);
        child.overflow_index = root.overflow_index;

        reset_node(root);
        root.header.is_external = false;
        root.header.next_id = child.page.id();

        Calico_Try_R(fix_parent_id(tree, child.page.id(), root.page.id()));
        child.gap_size = root.gap_size + FileHeader::SIZE;
        tree.release(std::move(root));
        Calico_Try_R(fix_links(tree, child));
        return child;
    }

    template<class Predicate>
    [[nodiscard]]
    static auto transfer_cells_right_while(BPlusTree &tree, Node &left, Node &right, const Predicate &predicate) -> tl::expected<void, Status>
    {
        CALICO_EXPECT_EQ(left.header.is_external, right.header.is_external);

        const auto &count = left.header.cell_count;
        Size counter {};

        while (count && predicate(left, right, counter++)) {
            const auto cell = read_cell(left, count - 1);
            write_cell(right, 0, cell);
            // Fix the back pointer for an overflow chain that was previously rooted at "left".
            if (left.header.is_external) {
                Calico_Try_R(maybe_fix_overflow_chain(tree, cell, right.page.id()));
            }
            CALICO_EXPECT_FALSE(is_overflowing(right));
            erase_cell(left, count - 1, cell.size);
        }
        return {};
    }

    [[nodiscard]]
    static auto split_external_non_root(BPlusTree &tree, Node &left, Node &right, Id parent_id) -> tl::expected<Cell, Status>
    {
        CALICO_EXPECT_TRUE(left.header.is_external);
        CALICO_EXPECT_TRUE(right.header.is_external);
        CALICO_EXPECT_TRUE(is_overflowing(left));
        const auto overflow_idx = left.overflow_index;
        auto overflow = *left.overflow;
        left.overflow.reset();

        right.header.next_id = left.header.next_id;
        right.header.prev_id = left.page.id();
        left.header.next_id = right.page.id();

        if (!right.header.next_id.is_null()) {
            Calico_New_R(right_right, tree.acquire(right.header.next_id, true));
            right_right.header.prev_id = right.page.id();
            tree.release(std::move(right_right));
        }

        Calico_Try_R(fix_parent_id(tree, right.page.id(), parent_id));

        if (overflow_idx == left.header.cell_count) {
            // Just transfer a single cell in this case. This should reduce the number of splits during a sequential write.
            Calico_Try_R(transfer_cells_right_while(tree, left, right, [](const auto &, const auto &, auto counter) {
                return !counter;
            }));
            Calico_Try_R(tree.write_external_cell(right, right.header.cell_count, overflow));
            CALICO_EXPECT_FALSE(is_overflowing(right));
        } else if (overflow_idx == 0) {
            // We need the `!counter` because the condition following it may not be true if we got here from split_root().
            Calico_Try_R(transfer_cells_right_while(tree, left, right, [](const auto &src, const auto &dst, auto counter) {
                return !counter || usable_space(src) < usable_space(dst);
            }));
            Calico_Try_R(tree.write_external_cell(left, 0, overflow));
            CALICO_EXPECT_FALSE(is_overflowing(left));
        } else {
            // We need to insert the overflow cell into either left or right, no matter what, even if it ends up being the separator.
            Calico_Try_R(transfer_cells_right_while(tree, left, right, [&overflow, overflow_idx](const auto &src, const auto &, auto counter) {
                const auto goes_in_src = src.header.cell_count > overflow_idx;
                const auto has_no_room = usable_space(src) < overflow.size + sizeof(PageSize);
                return !counter || (goes_in_src && has_no_room);
            }));

            if (left.header.cell_count > overflow_idx) {
                Calico_Try_R(tree.write_external_cell(left, overflow_idx, overflow));
                CALICO_EXPECT_FALSE(is_overflowing(left));
            } else {
                Calico_Try_R(tree.write_external_cell(right, 0, overflow));
                CALICO_EXPECT_FALSE(is_overflowing(right));
            }
        }

        auto separator = read_cell(right, 0);
        promote_cell(separator);
        return separator;
    }

    [[nodiscard]]
    static auto split_internal_non_root(BPlusTree &tree, Node &left, Node &right, Id parent_id) -> tl::expected<Cell, Status>
    {
        CALICO_EXPECT_FALSE(left.header.is_external);
        CALICO_EXPECT_FALSE(right.header.is_external);
        CALICO_EXPECT_TRUE(is_overflowing(left));
        const auto overflow_idx = left.overflow_index;
        auto overflow = *left.overflow;
        left.overflow.reset();

        // In internal nodes, the next_id field refers to the rightmost child ID, and the prev_id field is unused.
        right.header.next_id = left.header.next_id;

        Calico_Try_R(fix_parent_id(tree, right.page.id(), parent_id));

        if (overflow_idx == left.header.cell_count) {
            Calico_Try_R(transfer_cells_right_while(tree, left, right, [](const auto &, const auto &, auto counter) {
                return !counter;
            }));
            write_cell(right, right.header.cell_count, overflow);
            CALICO_EXPECT_FALSE(is_overflowing(right));
        } else if (overflow_idx == 0) {
            Calico_Try_R(transfer_cells_right_while(tree, left, right, [](const auto &src, const auto &dst, auto counter) {
                return !counter || usable_space(src) < usable_space(dst);
            }));
            write_cell(left, 0, overflow);
            CALICO_EXPECT_FALSE(is_overflowing(left));
        } else {
            left.header.next_id = read_child_id(overflow);
            Calico_Try_R(transfer_cells_right_while(tree, left, right, [overflow_idx](const auto &src, const auto &, auto) {
                return src.header.cell_count > overflow_idx;
            }));
            return overflow;
        }

        auto separator = read_cell(left, left.header.cell_count - 1);
        detach_cell(separator, tree.scratch(1));
        erase_cell(left, left.header.cell_count - 1, separator.size);
        left.header.next_id = read_child_id(separator);
        return separator;
    }

    [[nodiscard]]
    static auto split_non_root(BPlusTree &tree, Node node) -> tl::expected<Node, Status>
    {
        CALICO_EXPECT_FALSE(node.page.id().is_root());
        CALICO_EXPECT_TRUE(is_overflowing(node));

        Calico_New_R(parent_id, find_parent_id(tree, node.page.id()));
        CALICO_EXPECT_FALSE(parent_id.is_null());

        Calico_New_R(parent, tree.acquire(parent_id, true));
        Calico_New_R(sibling, tree.allocate(node.header.is_external));

        Cell separator;
        if (node.header.is_external) {
            Calico_Put_R(separator, split_external_non_root(tree, node, sibling, parent_id));
        } else {
            Calico_Put_R(separator, split_internal_non_root(tree, node, sibling, parent_id));
        }
        Node::Iterator itr {parent};
        itr.seek(read_key(separator));
        write_cell(parent, itr.index(), separator);

        if (parent.overflow) {
            // Only detach the cell if it couldn't fit in the parent. In this case, we want to release node before
            // we return, so cell cannot be attached to it anymore. The separator should have already been promoted.
            if (!separator.is_free) {
                detach_cell(*parent.overflow, tree.scratch(0));
            }
            CALICO_EXPECT_TRUE(parent.overflow->is_free);
            write_child_id(*parent.overflow, node.page.id());
        } else {
            write_child_id(parent, itr.index(), node.page.id());
        }

        CALICO_EXPECT_FALSE(is_overflowing(node));
        CALICO_EXPECT_FALSE(is_overflowing(sibling));

        const auto offset = !is_overflowing(parent);
        write_child_id(parent, itr.index() + offset, sibling.page.id());
        Calico_Try_R(fix_links(tree, sibling));
        tree.release(std::move(sibling));
        tree.release(std::move(node));
        return parent;
    }

    static auto resolve_underflow(BPlusTree &tree, Node node, const Slice &anchor) -> tl::expected<void, Status>
    {
        while (is_underflowing(node)) {
            if (node.page.id().is_root()) {
                return fix_root(tree, std::move(node));
            }
            Calico_New_R(parent_id, find_parent_id(tree, node.page.id()));
            CALICO_EXPECT_FALSE(parent_id.is_null());
            Calico_New_R(parent, tree.acquire(parent_id, true));
            // NOTE: Searching for the anchor key from the node we took from should always
            //       give us the correct index due to the B+-tree ordering rules.
            Node::Iterator itr {parent};
            const auto exact = itr.seek(anchor);
            const auto index = itr.index() + exact;
            Calico_Try_R(fix_non_root(tree, std::move(node), parent, index));
            node = std::move(parent);
        }
        tree.release(std::move(node));
        return {};
    }

    static auto transfer_first_cell_left(Node &src, Node &dst) -> void
    {
        CALICO_EXPECT_EQ(src.header.is_external, dst.header.is_external);
        auto cell = read_cell(src, 0);
        write_cell(dst, dst.header.cell_count, cell);
        CALICO_EXPECT_FALSE(is_overflowing(dst));
        erase_cell(src, 0, cell.size);
    }

    static auto internal_merge_left(BPlusTree &tree, Node &left, Node &right, Node &parent, Size index) -> tl::expected<void, Status>
    {
        CALICO_EXPECT_TRUE(is_underflowing(left));
        CALICO_EXPECT_FALSE(left.header.is_external);
        CALICO_EXPECT_FALSE(right.header.is_external);
        CALICO_EXPECT_FALSE(parent.header.is_external);

        const auto separator = read_cell(parent, index);
        const auto sep_index = left.header.cell_count;
        write_cell(left, sep_index, separator);
        write_child_id(left, sep_index, left.header.next_id);
        erase_cell(parent, index, separator.size);

        while (right.header.cell_count) {
            Calico_Try_R(fix_parent_id(tree, read_child_id(right, 0), left.page.id()));
            transfer_first_cell_left(right, left);
        }
        CALICO_EXPECT_FALSE(is_overflowing(left));

        left.header.next_id = right.header.next_id;
        write_child_id(parent, index, left.page.id());
        return {};
    }

    static auto external_merge_left(BPlusTree &tree, Node &left, Node &right, Node &parent, Size index) -> tl::expected<void, Status>
    {
        CALICO_EXPECT_TRUE(is_underflowing(left));
        CALICO_EXPECT_TRUE(left.header.is_external);
        CALICO_EXPECT_TRUE(right.header.is_external);
        CALICO_EXPECT_FALSE(parent.header.is_external);

        left.header.next_id = right.header.next_id;

        const auto separator = read_cell(parent, index);
        erase_cell(parent, index, separator.size);

        while (right.header.cell_count) {
            Calico_Try_R(maybe_fix_overflow_chain(tree, read_cell(right, 0), left.page.id()));
            transfer_first_cell_left(right, left);
        }
        CALICO_EXPECT_FALSE(is_overflowing(left));
        write_child_id(parent, index, left.page.id());
        return {};
    }

    static auto merge_left(BPlusTree &tree, Node &left, Node &right, Node &parent, Size index) -> tl::expected<void, Status>
    {
        if (left.header.is_external) {
            return external_merge_left(tree, left, right, parent, index);
        } else {
            return internal_merge_left(tree, left, right, parent, index);
        }
    }

    static auto internal_merge_right(BPlusTree &tree, Node &left, Node &right, Node &parent, Size index) -> tl::expected<void, Status>
    {
        CALICO_EXPECT_TRUE(is_underflowing(right));
        CALICO_EXPECT_FALSE(left.header.is_external);
        CALICO_EXPECT_FALSE(right.header.is_external);
        CALICO_EXPECT_FALSE(parent.header.is_external);

        const auto separator = read_cell(parent, index);
        const auto sep_index = left.header.cell_count;

        write_cell(left, sep_index, separator);
        write_child_id(left, sep_index, left.header.next_id);
        left.header.next_id = right.header.next_id;

        CALICO_EXPECT_EQ(read_child_id(parent, index + 1), right.page.id());
        write_child_id(parent, index + 1, left.page.id());
        erase_cell(parent, index, separator.size);

        // Transfer the rest of the cells. left shouldn't overflow.
        while (right.header.cell_count) {
            Calico_Try_R(fix_parent_id(tree, read_child_id(right, 0), left.page.id()));
            transfer_first_cell_left(right, left);
            CALICO_EXPECT_FALSE(is_overflowing(left));
        }
        return {};
    }

    static auto external_merge_right(BPlusTree &tree, Node &left, Node &right, Node &parent, Size index) -> tl::expected<void, Status>
    {
        CALICO_EXPECT_TRUE(is_underflowing(right));
        CALICO_EXPECT_TRUE(left.header.is_external);
        CALICO_EXPECT_TRUE(right.header.is_external);
        CALICO_EXPECT_FALSE(parent.header.is_external);

        left.header.next_id = right.header.next_id;
        const auto separator = read_cell(parent, index);
        CALICO_EXPECT_EQ(read_child_id(parent, index + 1), right.page.id());
        write_child_id(parent, index + 1, left.page.id());
        erase_cell(parent, index, separator.size);

        while (right.header.cell_count) {
            Calico_Try_R(maybe_fix_overflow_chain(tree, read_cell(right, 0), left.page.id()));
            transfer_first_cell_left(right, left);
        }
        CALICO_EXPECT_FALSE(is_overflowing(left));
        return {};
    }

    static auto merge_right(BPlusTree &tree, Node &left, Node &right, Node &parent, Size index) -> tl::expected<void, Status>
    {
        if (left.header.is_external) {
            return external_merge_right(tree, left, right, parent, index);
        } else {
            return internal_merge_right(tree, left, right, parent, index);
        }
    }

    [[nodiscard]]
    static auto fix_non_root(BPlusTree &tree, Node node, Node &parent, Size index) -> tl::expected<bool, Status>
    {
        CALICO_EXPECT_FALSE(node.page.id().is_root());
        CALICO_EXPECT_TRUE(is_underflowing(node));
        CALICO_EXPECT_FALSE(is_overflowing(parent));

        if (index > 0) {
            Calico_New_R(left, tree.acquire(read_child_id(parent, index - 1), true));
            if (left.header.cell_count == 1) {
                Calico_Try_R(merge_right(tree, left, node, parent, index - 1));
                Calico_Try_R(fix_links(tree, left));
                if (node.header.is_external && !node.header.next_id.is_null()) {
                    Calico_New_R(right, tree.acquire(node.header.next_id, true));
                    right.header.prev_id = left.page.id();
                    tree.release(std::move(right));
                }
                tree.release(std::move(left));
                Calico_Try_R(tree.destroy(std::move(node)));
                CALICO_EXPECT_FALSE(is_overflowing(parent));
                return {};
            }
            Calico_Try_R(rotate_right(tree, parent, left, node, index - 1));
            tree.release(std::move(left));
        } else {
            // B+-tree rules guarantee a right sibling in this case.
            CALICO_EXPECT_LT(index, parent.header.cell_count);

            Calico_New_R(right, tree.acquire(read_child_id(parent, index + 1), true));
            if (right.header.cell_count == 1) {
                Calico_Try_R(merge_left(tree, node, right, parent, index));
                Calico_Try_R(fix_links(tree, node));
                if (right.header.is_external && !right.header.next_id.is_null()) {
                    Calico_New_R(right_right, tree.acquire(right.header.next_id, true));
                    right_right.header.prev_id = node.page.id();
                    tree.release(std::move(right_right));
                }
                tree.release(std::move(node));
                Calico_Try_R(tree.destroy(std::move(right)));
                CALICO_EXPECT_FALSE(is_overflowing(parent));
                return {};
            }
            Calico_Try_R(rotate_left(tree, parent, node, right, index));
            tree.release(std::move(right));
        }

        CALICO_EXPECT_FALSE(is_overflowing(node));
        tree.release(std::move(node));

        if (is_overflowing(parent)) {
            const auto saved_id = parent.page.id();
            Calico_Try_R(resolve_overflow(tree, std::move(parent)));
            Calico_Put_R(parent, tree.acquire(saved_id, true));
        }
        return {};
    }

    [[nodiscard]]
    static auto fix_root(BPlusTree &tree, Node root) -> tl::expected<void, Status>
    {
        CALICO_EXPECT_TRUE(root.page.id().is_root());

        // If the root is external here, the whole tree must be empty.
        if (!root.header.is_external) {
            Calico_New_R(child, tree.acquire(root.header.next_id, true));

            // We don't have enough room to transfer the child contents into the root, due to the space occupied by
            // the file header. In this case, we'll just split the child and insert the median cell into the root.
            // Note that the child needs an overflow cell for the split routine to work. We'll just fake it by
            // extracting an arbitrary cell and making it the overflow cell.
            if (usable_space(child) < FileHeader::SIZE) {
                child.overflow = read_cell(child, 0);
                detach_cell(*child.overflow, tree.scratch(0));
                erase_cell(child, 0);
                tree.release(std::move(root));
                Calico_New_R(parent, split_non_root(tree, std::move(child)));
                tree.release(std::move(parent));
                Calico_Put_R(root, tree.acquire(Id::root(), true));
            } else {
                merge_root(root, child);
                Calico_Try_R(tree.destroy(std::move(child)));
            }
            Calico_Try_R(fix_links(tree, root));
        }
        tree.release(std::move(root));
        return {};
    }

    [[nodiscard]]
    static auto rotate_left(BPlusTree &tree, Node &parent, Node &left, Node &right, Size index) -> tl::expected<void, Status>
    {
        if (left.header.is_external) {
            return external_rotate_left(tree, parent, left, right, index);
        } else {
            return internal_rotate_left(tree, parent, left, right, index);
        }
    }

    static auto external_rotate_left(BPlusTree &tree, Node &parent, Node &left, Node &right, Size index) -> tl::expected<void, Status>
    {
        CALICO_EXPECT_TRUE(left.header.is_external);
        CALICO_EXPECT_TRUE(right.header.is_external);
        CALICO_EXPECT_FALSE(parent.header.is_external);
        CALICO_EXPECT_GT(parent.header.cell_count, 0);
        CALICO_EXPECT_GT(right.header.cell_count, 1);

        auto lowest = read_cell(right, 0);
        write_cell(left, left.header.cell_count, lowest);
        CALICO_EXPECT_FALSE(is_overflowing(left));
        Calico_Try_R(maybe_fix_overflow_chain(tree, lowest, left.page.id()));
        erase_cell(right, 0);

        auto separator = read_cell(right, 0);
        promote_cell(separator);
        detach_cell(separator, tree.scratch(1));
        write_child_id(separator, left.page.id());

        erase_cell(parent, index, read_cell(parent, index).size);
        write_cell(parent, index, separator);
        return {};
    }

    [[nodiscard]]
    static auto internal_rotate_left(BPlusTree &tree, Node &parent, Node &left, Node &right, Size index) -> tl::expected<void, Status>
    {
        CALICO_EXPECT_FALSE(parent.header.is_external);
        CALICO_EXPECT_FALSE(left.header.is_external);
        CALICO_EXPECT_FALSE(right.header.is_external);
        CALICO_EXPECT_GT(parent.header.cell_count, 0);
        CALICO_EXPECT_GT(right.header.cell_count, 1);

        Calico_New_R(child, tree.acquire(read_child_id(right, 0), true));
        const auto saved_id = left.header.next_id;
        left.header.next_id = child.page.id();
        Calico_Try_R(fix_parent_id(tree, child.page.id(), left.page.id()));
        tree.release(std::move(child));

        auto separator = read_cell(parent, index);
        write_cell(left, left.header.cell_count, separator);
        CALICO_EXPECT_FALSE(is_overflowing(left));
        write_child_id(left, left.header.cell_count - 1, saved_id);
        erase_cell(parent, index, separator.size);

        auto lowest = read_cell(right, 0);
        detach_cell(lowest, tree.scratch(2));
        erase_cell(right, 0);
        write_child_id(lowest, left.page.id());
        write_cell(parent, index, lowest);
        return {};
    }

    [[nodiscard]]
    static auto rotate_right(BPlusTree &tree, Node &parent, Node &left, Node &right, Size index) -> tl::expected<void, Status>
    {
        if (left.header.is_external) {
            return external_rotate_right(tree, parent, left, right, index);
        } else {
            return internal_rotate_right(tree, parent, left, right, index);
        }
    }

    [[nodiscard]]
    static auto external_rotate_right(BPlusTree &tree, Node &parent, Node &left, Node &right, Size index) -> tl::expected<void, Status>
    {
        CALICO_EXPECT_TRUE(left.header.is_external);
        CALICO_EXPECT_TRUE(right.header.is_external);
        CALICO_EXPECT_FALSE(parent.header.is_external);
        CALICO_EXPECT_GT(parent.header.cell_count, 0);
        CALICO_EXPECT_GT(left.header.cell_count, 1);

        auto highest = read_cell(left, left.header.cell_count - 1);
        write_cell(right, 0, highest);
        CALICO_EXPECT_FALSE(is_overflowing(right));

        // Update the back pointer for the overflow chain, if it exists.
        Calico_Try_R(maybe_fix_overflow_chain(tree, highest, right.page.id()));

        auto separator = highest;
        promote_cell(separator);
        detach_cell(separator, tree.scratch(1));
        write_child_id(separator, left.page.id());

        // Don't erase the cell until it has been detached.
        erase_cell(left, left.header.cell_count - 1);

        erase_cell(parent, index, read_cell(parent, index).size);
        write_cell(parent, index, separator);
        return {};
    }

    [[nodiscard]]
    static auto internal_rotate_right(BPlusTree &tree, Node &parent, Node &left, Node &right, Size index) -> tl::expected<void, Status>
    {
        CALICO_EXPECT_FALSE(parent.header.is_external);
        CALICO_EXPECT_FALSE(left.header.is_external);
        CALICO_EXPECT_FALSE(right.header.is_external);
        CALICO_EXPECT_GT(parent.header.cell_count, 0);
        CALICO_EXPECT_GT(left.header.cell_count, 1);

        Calico_New_R(child, tree.acquire(left.header.next_id, true));
        const auto child_id = child.page.id();
        Calico_Try_R(fix_parent_id(tree, child.page.id(), right.page.id()));
        left.header.next_id = read_child_id(left, left.header.cell_count - 1);
        tree.release(std::move(child));

        auto separator = read_cell(parent, index);
        write_cell(right, 0, separator);
        CALICO_EXPECT_FALSE(is_overflowing(right));
        write_child_id(right, 0, child_id);
        erase_cell(parent, index, separator.size);

        auto highest = read_cell(left, left.header.cell_count - 1);
        detach_cell(highest, tree.scratch(2));
        write_child_id(highest, left.page.id());
        erase_cell(left, left.header.cell_count - 1, highest.size);
        write_cell(parent, index, highest);
        return {};
    }
};

BPlusTree::BPlusTree(Pager &pager)
    : m_pointers {pager},
      m_freelist {pager, m_pointers},
      m_overflow {pager, m_freelist, m_pointers},
      m_pager {&pager}
{
    // min_local and max_local fields are only needed in external nodes.
    m_external_meta.min_local = compute_min_local(pager.page_size());
    m_external_meta.max_local = compute_max_local(pager.page_size());

    m_external_meta.cell_size = external_cell_size;
    m_external_meta.read_key = read_external_key;
    m_external_meta.parse_cell = parse_external_cell;

    m_internal_meta.cell_size = internal_cell_size;
    m_internal_meta.read_key = read_internal_key;
    m_internal_meta.parse_cell = parse_internal_cell;

    // Scratch memory for defragmenting nodes and storing cells.
    m_scratch[0].resize(pager.page_size());
    m_scratch[1].resize(pager.page_size());
    m_scratch[2].resize(pager.page_size());
    m_scratch[3].resize(pager.page_size());
}

auto BPlusTree::setup() -> tl::expected<Node, Status>
{
    CALICO_EXPECT_EQ(m_pager->page_count(), 0);
    Calico_New_R(root, allocate(true));
    CALICO_EXPECT_EQ(m_pager->page_count(), 1);
    return root;
}


auto BPlusTree::make_fresh_node(Page page, bool is_external) -> Node
{
    NodeHeader header;
    header.is_external = is_external;
    header.cell_start = static_cast<PageSize>(page.size());
    header.write(page);

    return make_existing_node(std::move(page));
}

auto BPlusTree::make_existing_node(Page page) -> Node
{
    Node node {std::move(page), m_scratch.back().data()};
    if (node.header.is_external) {
        node.meta = &m_external_meta;
    } else {
        node.meta = &m_internal_meta;
    }
    return node;
}

auto BPlusTree::scratch(Size index) -> Byte *
{
    // Reserve the last scratch buffer for defragmentation. Also, reserve 4 bytes at the front in case the cell
    // needs to be promoted.
    CALICO_EXPECT_LT(index, m_scratch.size() - 1);
    return m_scratch[index].data() + EXTERNAL_SHIFT;
}

auto BPlusTree::allocate(bool is_external) -> tl::expected<Node, Status>
{
    const auto fetch_unused_page = [this]() -> tl::expected<Page, Status> {
        if (m_freelist.is_empty()) {
            Calico_New_R(page, m_pager->allocate());
            // Since this is a fresh page from the end of the file, it could be a pointer map page. If so,
            // it is already blank, so just skip it and allocate another. It'll get filled in as the pages
            // following it are used.
            if (BPlusTreeInternal::is_pointer_map(*this, page.id())) {
                m_pager->release(std::move(page));
                Calico_Put_R(page, m_pager->allocate());
            }
            return page;
        } else {
            return m_freelist.pop();
        }
    };
    Calico_New_R(page, fetch_unused_page());
    CALICO_EXPECT_FALSE(BPlusTreeInternal::is_pointer_map(*this, page.id()));
    return make_fresh_node(std::move(page), is_external);
}

auto BPlusTree::acquire(Id pid, bool upgrade) -> tl::expected<Node, Status>
{
    CALICO_EXPECT_FALSE(BPlusTreeInternal::is_pointer_map(*this, pid));
    Calico_New_R(page, m_pager->acquire(pid));

    if (upgrade) {
        m_pager->upgrade(page);
    }
    return make_existing_node(std::move(page));
}

auto BPlusTree::release(Node node) const -> void
{
    m_pager->release(std::move(node).take());
}

auto BPlusTree::destroy(Node node) -> tl::expected<void, Status>
{
    // Pointer map pages should never be explicitly destroyed.
    CALICO_EXPECT_FALSE(BPlusTreeInternal::is_pointer_map(*this, node.page.id()));
    return m_freelist.push(std::move(node).take());
}

auto BPlusTree::insert(const Slice &key, const Slice &value) -> tl::expected<bool, Status>
{
    Calico_New_R(slot, BPlusTreeInternal::find_external_slot(*this, key));
    auto [node, index, exact] = std::move(slot);
    m_pager->upgrade(node.page);

    if (exact) {
        const auto cell = read_cell(node, index);
        if (cell.local_ps != cell.total_ps) {
            CALICO_EXPECT_LT(cell.local_ps, cell.total_ps);
            const auto overflow_id = read_overflow_id(cell);
            Calico_Try_R(m_overflow.erase_chain(overflow_id, cell.total_ps - cell.local_ps));
        }
        erase_cell(node, index, cell.size);
    }

    Calico_Try_R(BPlusTreeInternal::emplace_cell(*this, node, index, key, value));
    Calico_Try_R(BPlusTreeInternal::resolve_overflow(*this, std::move(node)));
    return !exact;
}

auto BPlusTree::erase(const Slice &key) -> tl::expected<void, Status>
{
    Calico_New_R(slot, BPlusTreeInternal::find_external_slot(*this, key));
    auto [node, index, exact] = std::move(slot);

    if (exact) {
        const auto cell = read_cell(node, index);
        const auto anchor = read_key(cell).to_string();
        if (const auto remote_size = cell.total_ps - cell.local_ps) {
            Calico_Try_R(m_overflow.erase_chain(read_overflow_id(cell), remote_size));
        }
        m_pager->upgrade(node.page);
        erase_cell(node, index);
        Calico_Try_R(BPlusTreeInternal::resolve_underflow(*this, std::move(node), anchor));
        return {};
    }
    release(std::move(node));
    return tl::make_unexpected(Status::not_found("not found"));
}

auto BPlusTree::write_external_cell(Node &node, Size index, const Cell &cell) -> tl::expected<void, Status>
{
    CALICO_EXPECT_TRUE(node.header.is_external);
    ::Calico::write_cell(node, index, cell);
    return BPlusTreeInternal::maybe_fix_overflow_chain(*this, cell, node.page.id());
}

auto BPlusTree::lowest() -> tl::expected<Node, Status>
{
    Calico_New_R(node, acquire(Id::root()));
    while (!node.header.is_external) {
        const auto next_id = read_child_id(node, 0);
        release(std::move(node));
        Calico_Put_R(node, acquire(next_id));
    }
    return node;
}

auto BPlusTree::highest() -> tl::expected<Node, Status>
{
    Calico_New_R(node, acquire(Id::root()));
    while (!node.header.is_external) {
        const auto next_id = node.header.next_id;
        release(std::move(node));
        Calico_Put_R(node, acquire(next_id));
    }
    return node;
}

auto BPlusTree::collect(Node node, Size index) -> tl::expected<std::string, Status>
{
    return BPlusTreeInternal::collect_value(*this, std::move(node), index);
}

auto BPlusTree::search(const Slice &key) -> tl::expected<SearchResult, Status>
{
    return BPlusTreeInternal::find_external_slot(*this, key);
}

auto BPlusTree::vacuum_step(Page &free, Id last_id) -> tl::expected<void, Status>
{
    Calico_New_R(entry, m_pointers.read_entry(last_id));

    const auto fix_basic_link = [&entry, &free, this]() -> tl::expected<void, Status> {
        Calico_New_R(parent, m_pager->acquire(entry.back_ptr));
        m_pager->upgrade(parent);
        write_next_id(parent, free.id());
        m_pager->release(std::move(parent));
        return {};
    };

    switch (entry.type) {
        case PointerMap::FREELIST_LINK: {
            if (last_id == m_freelist.m_head) {
                m_freelist.m_head = free.id();
            } else {
                // Back pointer points to another freelist page.
                CALICO_EXPECT_FALSE(entry.back_ptr.is_null());
                Calico_Try_R(fix_basic_link());
                Calico_New_R(last, m_pager->acquire(last_id));
                if (const auto next_id = read_next_id(last); !next_id.is_null()) {
                    const PointerMap::Entry next_entry {free.id(), PointerMap::FREELIST_LINK};
                    Calico_Try_R(m_pointers.write_entry(next_id, next_entry));
                }
                m_pager->release(std::move(last));
            }
            break;
        }
        case PointerMap::OVERFLOW_LINK: {
            // Back pointer points to another overflow chain link, or the head of the chain.
            Calico_Try_R(fix_basic_link());
            break;
        }
        case PointerMap::OVERFLOW_HEAD: {
            // Back pointer points to the node that the overflow chain is rooted in. Search through that nodes cells
            // for the target overflow cell.
            Calico_New_R(parent, acquire(entry.back_ptr, true));
            CALICO_EXPECT_TRUE(parent.header.is_external);
            bool found {};
            for (Size i {}; i < parent.header.cell_count; ++i) {
                auto cell = read_cell(parent, i);
                found = cell.local_ps != cell.total_ps &&
                        read_overflow_id(cell) == last_id;
                if (found) {
                    write_overflow_id(cell, free.id());
                    break;
                }
            }
            CALICO_EXPECT_TRUE(found);
            release(std::move(parent));
            break;
        }
        case PointerMap::NODE: {
            // Back pointer points to another node. Search through that node for the target child pointer.
            Calico_New_R(parent, acquire(entry.back_ptr, true));
            CALICO_EXPECT_FALSE(parent.header.is_external);
            bool found {};
            for (Size i {}; !found && i <= parent.header.cell_count; ++i) {
                const auto child_id = read_child_id(parent, i);
                found = child_id == last_id;
                if (found) {
                    write_child_id(parent, i, free.id());
                }
            }
            CALICO_EXPECT_TRUE(found);
            release(std::move(parent));
            // Update references.
            Calico_New_R(last, acquire(last_id, true));
            if (last.header.is_external) {
                for (Size i {}; i < last.header.cell_count; ++i) {
                    const auto cell = read_cell(last, i);
                    Calico_Try_R(BPlusTreeInternal::maybe_fix_overflow_chain(*this, cell, free.id()));
                }
                if (!last.header.prev_id.is_null()) {
                    Calico_New_R(prev, acquire(last.header.prev_id, true));
                    prev.header.next_id = free.id();
                    release(std::move(prev));
                }
                if (!last.header.next_id.is_null()) {
                    Calico_New_R(next, acquire(last.header.next_id, true));
                    next.header.prev_id = free.id();
                    release(std::move(next));
                }
            } else {
                for (Size i {}; i <= last.header.cell_count; ++i) {
                    const auto child_id = read_child_id(last, i);
                    const PointerMap::Entry child_entry {free.id(), PointerMap::NODE};
                    Calico_Try_R(m_pointers.write_entry(child_id, child_entry));
                }
            }
            release(std::move(last));
        }
    }
    Calico_Try_R(m_pointers.write_entry(last_id, {}));
    Calico_Try_R(m_pointers.write_entry(free.id(), entry));
    Calico_New_R(last, m_pager->acquire(last_id));
    if (entry.type != PointerMap::NODE) {
        if (const auto next_id = read_next_id(last); !next_id.is_null()) {
            Calico_New_R(next_entry, m_pointers.read_entry(next_id));
            next_entry.back_ptr = free.id();
            Calico_Try_R(m_pointers.write_entry(next_id, next_entry));
        }
    }
    mem_copy(free.span(sizeof(Lsn), free.size() - sizeof(Lsn)),
             last.view(sizeof(Lsn), last.size() - sizeof(Lsn)));
    m_pager->release(std::move(last));
    return {};
}

auto BPlusTree::vacuum_one(Id target) -> tl::expected<bool, Status>
{
    if (BPlusTreeInternal::is_pointer_map(*this, target)) {
        return true;
    }
    if (target.is_root() || m_freelist.is_empty()) {
        return false;
    }

    // Swap the head of the freelist with the last page in the file.
    Calico_New_R(head, m_freelist.pop());
    if (target != head.id()) {
        // Swap the last page with the freelist head.
        Calico_Try_R(vacuum_step(head, target));
    }
    m_pager->release(std::move(head));
    return true;
}

auto BPlusTree::save_state(FileHeader &header) const -> void
{
    header.freelist_head = m_freelist.m_head;
}

auto BPlusTree::load_state(const FileHeader &header) -> void
{
    m_freelist.m_head = header.freelist_head;
}

using Callback = std::function<void(Node &, Size)>;

class BPlusTreeValidator {
public:
    explicit BPlusTreeValidator(BPlusTree &tree)
        : m_tree {&tree}
    {}

    struct PrintData {
        std::vector<std::string> levels;
        std::vector<Size> spaces;
    };

    auto collect_levels(PrintData &data, Node node, Size level) -> void
    {
        const auto &header = node.header;
        ensure_level_exists(data, level);
        for (Size cid {}; cid < header.cell_count; ++cid) {
            const auto is_first = cid == 0;
            const auto not_last = cid + 1 < header.cell_count;
            auto cell = read_cell(node, cid);

            if (!header.is_external) {
                collect_levels(data, m_tree->acquire(read_child_id(cell), false).value(), level + 1);
            }

            if (is_first) {
                add_to_level(data, std::to_string(node.page.id().value) + ":[", level);
            }

            const auto key = Slice {cell.key, cell.key_size}.to_string();
            add_to_level(data, key, level);

            if (not_last) {
                add_to_level(data, ",", level);
            } else {
                add_to_level(data, "]", level);
            }
        }
        if (!node.header.is_external)
            collect_levels(data, m_tree->acquire(node.header.next_id, false).value(), level + 1);

        m_tree->release(std::move(node));
    }

    auto traverse_inorder(const Callback &callback) -> void
    {
        traverse_inorder_helper(m_tree->acquire(Id::root(), false).value(), callback);
    }

    auto validate_freelist(Id head) -> void
    {
        auto &pager = *m_tree->m_pager;
        auto &freelist = m_tree->m_freelist;
        if (freelist.is_empty()) {
            return;
        }
        CALICO_EXPECT_FALSE(head.is_null());
        auto page = pager.acquire(head).value();
        CALICO_EXPECT_EQ(BPlusTreeInternal::find_parent_id(*m_tree, page.id()).value(), Id::null());

        for (; ; ) {
            const auto next_id = read_next_id(page);
            if (next_id.is_null()) {
                break;
            }
            CALICO_EXPECT_EQ(BPlusTreeInternal::find_parent_id(*m_tree, next_id).value(), page.id());
            pager.release(std::move(page));
            page = pager.acquire(next_id).value();
        }
        pager.release(std::move(page));
    }

    auto validate_overflow(Id overflow_id, Id parent_id, Size overflow_size) -> void
    {
        auto &pager = *m_tree->m_pager;
        CALICO_EXPECT_EQ(BPlusTreeInternal::find_parent_id(*m_tree, overflow_id).value(), parent_id);
        (void)parent_id;
        auto page = pager.acquire(overflow_id).value();
        for (Size n {}; n < overflow_size; ) {
            const Id next_id {get_u64(page.data() + sizeof(Lsn))};
            n += pager.page_size() - sizeof(Lsn) - sizeof(Id);
            if (n >= overflow_size) {
                CALICO_EXPECT_TRUE(next_id.is_null());
                break;
            }
            CALICO_EXPECT_EQ(BPlusTreeInternal::find_parent_id(*m_tree, next_id).value(), page.id());
            pager.release(std::move(page));
            page = pager.acquire(next_id).value();
        }
        pager.release(std::move(page));
    }

    auto validate_siblings() -> void
    {
        const auto validate_possible_overflows = [this](auto &node) {
            for (Size i {}; i < node.header.cell_count; ++i) {
                const auto cell = read_cell(node, i);
                if (cell.local_ps != cell.total_ps) {
                    CALICO_EXPECT_LT(cell.local_ps, cell.total_ps);
                    validate_overflow(read_overflow_id(cell), node.page.id(), cell.total_ps - cell.local_ps);
                }
            }
        };

        // Find the leftmost external node.
        auto node = m_tree->acquire(Id::root(), false).value();
        while (!node.header.is_external) {
            const auto id = read_child_id(node, 0);
            m_tree->release(std::move(node));
            auto temp = m_tree->acquire(id, false);
            CALICO_EXPECT_TRUE(temp.has_value());
            node = std::move(*temp);
        }
        // Traverse across the sibling chain to the right.
        while (!node.header.next_id.is_null()) {
            validate_possible_overflows(node);
            auto right = m_tree->acquire(node.header.next_id, false);
            CALICO_EXPECT_TRUE(right.has_value());
            CALICO_EXPECT_LT(read_key(node, 0), read_key(*right, 0));
            CALICO_EXPECT_EQ(right->header.prev_id, node.page.id());
            m_tree->release(std::move(node));
            node = std::move(*right);
        }
        validate_possible_overflows(node);
        m_tree->release(std::move(node));
    }

    auto validate_parent_child() -> void
    {
        auto check = [this](auto &node, auto index) -> void {
            auto child = m_tree->acquire(read_child_id(node, index), false).value();
            const auto parent_id = BPlusTreeInternal::find_parent_id(*m_tree, child.page.id());
            CALICO_EXPECT_TRUE(parent_id.has_value());
            CALICO_EXPECT_EQ(parent_id, node.page.id());
            m_tree->release(std::move(child));
        };
        traverse_inorder([f = std::move(check)](Node &node, Size index) -> void {
            const auto count = node.header.cell_count;
            CALICO_EXPECT_LT(index, count);
            if (!node.header.is_external) {
                f(node, index);
                // Rightmost child.
                if (index + 1 == count) {
                    f(node, index + 1);
                }
            }
        });
    }

private:
    auto traverse_inorder_helper(Node node, const Callback &callback) -> void
    {
        for (Size index {}; index <= node.header.cell_count; ++index) {
            if (!node.header.is_external) {
                const auto saved_id = node.page.id();
                const auto next_id = read_child_id(node, index);
                // "node" must be released while we traverse, otherwise we are limited in how long of a traversal we can
                // perform by the number of pager frames.
                m_tree->release(std::move(node));
                auto next = m_tree->acquire(next_id, false);
                CALICO_EXPECT_TRUE(next.has_value());
                traverse_inorder_helper(std::move(*next), callback);
                node = m_tree->acquire(saved_id, false).value();
            }
            if (index < node.header.cell_count) {
                callback(node, index);
            }
        }
        m_tree->release(std::move(node));
    }

    auto add_to_level(PrintData &data, const std::string &message, Size target) -> void
    {
        // If target is equal to levels.size(), add spaces to all levels.
        CALICO_EXPECT_LE(target, data.levels.size());
        Size i {};

        auto s_itr = begin(data.spaces);
        auto L_itr = begin(data.levels);
        while (s_itr != end(data.spaces)) {
            CALICO_EXPECT_NE(L_itr, end(data.levels));
            if (i++ == target) {
                // Don't leave trailing spaces. Only add them if there will be more text.
                L_itr->resize(L_itr->size() + *s_itr, ' ');
                L_itr->append(message);
                *s_itr = 0;
            } else {
                *s_itr += message.size();
            }
            L_itr++;
            s_itr++;
        }
    }

    auto ensure_level_exists(PrintData &data, Size level) -> void
    {
        while (level >= data.levels.size()) {
            data.levels.emplace_back();
            data.spaces.emplace_back();
        }
        CALICO_EXPECT_GT(data.levels.size(), level);
        CALICO_EXPECT_EQ(data.levels.size(), data.spaces.size());
    }

    BPlusTree *m_tree {};
};

auto BPlusTree::TEST_to_string() -> std::string
{
    std::string repr;
    BPlusTreeValidator::PrintData data;
    BPlusTreeValidator validator {*this};

    auto root = acquire(Id::root()).value();
    validator.collect_levels(data, std::move(root), 0);
    for (const auto &level: data.levels) {
        repr.append(level + '\n');
    }

    return repr;
}

auto BPlusTree::TEST_check_order() -> void
{
    // NOTE: All keys must fit in main memory (separators included). Doesn't read values.
    std::vector<std::string> keys;
    BPlusTreeValidator validator {*this};

    validator.traverse_inorder([&keys](const auto &node, auto index) -> void {
        keys.emplace_back(read_key(node, index).to_string());
    });
    CALICO_EXPECT_TRUE(std::is_sorted(cbegin(keys), cend(keys)));
}

auto BPlusTree::TEST_check_links() -> void
{
    BPlusTreeValidator validator {*this};
    validator.validate_siblings();
    validator.validate_parent_child();
    validator.validate_freelist(m_freelist.m_head);
}

auto BPlusTree::TEST_check_nodes() -> void
{
    BPlusTreeValidator validator {*this};
    validator.traverse_inorder([](const auto &node, auto index) -> void {
        // Only validate once per node.
        if (index == 0) {
            node.TEST_validate();
        }
    });
}

auto BPlusTree::TEST_components() -> Components
{
    return {&m_freelist, &m_overflow, &m_pointers};
}

} // namespace Calico
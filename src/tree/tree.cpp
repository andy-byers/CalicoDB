#include "tree.h"
#include "pager/pager.h"
#include "utils/utils.h"

namespace Calico {

struct SeekResult {
    unsigned index {};
    bool exact {};
};

using FetchKey = std::function<Slice (Size)>;

static auto seek_binary(unsigned n, const Slice &key, const FetchKey &fetch) -> SeekResult
{
    auto upper{n};
    unsigned lower {};

    while (lower < upper) {
        const auto mid = (lower+upper) / 2;
        const auto rhs = fetch(mid);

        switch (compare_three_way(key, rhs)) {
            case ThreeWayComparison::LT:
                upper = mid;
                break;
            case ThreeWayComparison::GT:
                lower = mid + 1;
                break;
            case ThreeWayComparison::EQ:
                return {mid, true};
        }
    }
    return {lower, false};
}

NodeIterator::NodeIterator(Node &node, const Parameters &param)
    : m_overflow {param.overflow},
      m_lhs_key {param.lhs_key},
      m_rhs_key {param.rhs_key},
      m_node {&node}
{
    CALICO_EXPECT_NE(m_overflow, nullptr);
    CALICO_EXPECT_NE(m_lhs_key, nullptr);
    CALICO_EXPECT_NE(m_rhs_key, nullptr);
}

// NOTE: "buffer" is only used if the key is fragmented.
auto NodeIterator::fetch_key(std::string &buffer, const Cell &cell, Slice &out) const -> Status
{
    if (!cell.has_remote || cell.key_size <= cell.local_size) {
        out = Slice {cell.key, cell.key_size};
        return Status::ok();
    }

    if (buffer.size() < cell.key_size) {
        buffer.resize(cell.key_size);
    }
    Span key {buffer.data(), cell.key_size};
    mem_copy(key, {cell.key, cell.local_size});
    key.advance(cell.local_size);

    Calico_Try(m_overflow->read_chain(key, read_overflow_id(cell)));
    out = Slice {buffer}.truncate(cell.key_size);
    return Status::ok();
}

auto NodeIterator::is_valid() const -> bool
{
    return m_index < m_node->header.cell_count;
}

auto NodeIterator::index() const -> Size
{
    return m_index;
}

auto NodeIterator::seek(const Slice &key) -> bool
{
    CALICO_EXPECT_TRUE(m_status.is_ok());

    const auto fetch = [this](auto index) {
        Slice out;
        m_status = fetch_key(*m_lhs_key, read_cell(*m_node, index), out);
        return out;
    };

    const auto [index, exact] = seek_binary(
        m_node->header.cell_count, key, fetch);

    m_index = index;
    return exact;
}

auto NodeIterator::seek(const Cell &cell) -> bool
{
    CALICO_EXPECT_TRUE(m_status.is_ok());

    if (!cell.has_remote) {
        return seek({cell.key, cell.key_size});
    }
    Slice key;
    m_status = fetch_key(*m_rhs_key, cell, key);
    if (m_status.is_ok()) {
        return seek(key);
    }
    return false;
}

auto NodeIterator::status() const -> Status
{
    return m_status;
}

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

class BPlusTreeInternal {
public:
    [[nodiscard]]
    static auto is_pointer_map(const BPlusTree &tree, Id pid) -> bool
    {
        return tree.m_pointers.lookup(pid) == pid;
    }

    [[nodiscard]]
    static auto find_external_slot(BPlusTree &tree, const Slice &key, Node node, SearchResult &out) -> Status
    {
        for (; ; ) {
            NodeIterator itr {node, {
                &tree.m_overflow,
                &tree.m_lhs_key,
                &tree.m_rhs_key,
            }};
            const auto exact = itr.seek(key);
            Calico_Try(itr.status());

            if (node.header.is_external) {
                out.node = std::move(node);
                out.index = itr.index();
                out.exact = exact;
                return Status::ok();
            }
            const auto next_id = read_child_id(node, itr.index() + exact);
            CALICO_EXPECT_NE(next_id, node.page.id()); // Infinite loop.
            tree.release(std::move(node));
            Calico_Try(tree.acquire(node, next_id));
        }
    }

    static auto find_external_slot(BPlusTree &tree, const Slice &key, SearchResult &out) -> Status
    {
        Node root;
        Calico_Try(tree.acquire(root, Id::root()));
        return find_external_slot(tree, key, std::move(root), out);
    }

    [[nodiscard]]
    static auto find_parent_id(BPlusTree &tree, Id pid, Id &out) -> Status
    {
        PointerMap::Entry entry;
        Calico_Try(tree.m_pointers.read_entry(pid, entry));
        out = entry.back_ptr;
        return Status::ok();
    }

    [[nodiscard]]
    static auto fix_parent_id(BPlusTree &tree, Id pid, Id parent_id, PointerMap::Type type = PointerMap::NODE) -> Status
    {
        PointerMap::Entry entry {parent_id, type};
        return tree.m_pointers.write_entry(pid, entry);
    }

    [[nodiscard]]
    static auto maybe_fix_overflow_chain(BPlusTree &tree, const Cell &cell, Id parent_id) -> Status
    {
        if (cell.has_remote) {
            return fix_parent_id(tree, read_overflow_id(cell), parent_id, PointerMap::OVERFLOW_HEAD);
        }
        return Status::ok();
    }

    [[nodiscard]]
    static auto insert_cell(BPlusTree &tree, Node &node, Size index, const Cell &cell) -> Status
    {
        write_cell(node, index, cell);
        if (!node.header.is_external) {
            Calico_Try(fix_parent_id(tree, read_child_id(cell), node.page.id()));
        }
        return maybe_fix_overflow_chain(tree, cell, node.page.id());
    }

    [[nodiscard]]
    static auto remove_cell(BPlusTree &tree, Node &node, Size index) -> Status
    {
        return remove_cell(tree, node, index, read_cell(node, index));
    }

    [[nodiscard]]
    static auto remove_cell(BPlusTree &tree, Node &node, Size index, const Cell &cell) -> Status
    {
        if (cell.has_remote) {
            Calico_Try(tree.m_overflow.erase_chain(read_overflow_id(cell)));
        }
        erase_cell(node, index, cell.size);
        return Status::ok();
    }

    [[nodiscard]]
    static auto fix_links(BPlusTree &tree, Node &node) -> Status
    {
        for (Size index {}; index < node.header.cell_count; ++index) {
            const auto cell = read_cell(node, index);
            Calico_Try(maybe_fix_overflow_chain(tree, cell, node.page.id()));
            if (!node.header.is_external) {
                Calico_Try(fix_parent_id(tree, read_child_id(cell), node.page.id()));
            }
        }
        if (!node.header.is_external) {
            Calico_Try(fix_parent_id(tree, node.header.next_id, node.page.id()));
        }
        if (node.overflow) {
            Calico_Try(maybe_fix_overflow_chain(tree, *node.overflow, node.page.id()));
            if (!node.header.is_external) {
                Calico_Try(fix_parent_id(tree, read_child_id(*node.overflow), node.page.id()));
            }
        }
        return Status::ok();
    }

    [[nodiscard]]
    static auto resolve_overflow(BPlusTree &tree, Node node) -> Status
    {
        Status (*fp)(BPlusTree &, Node, Node &);
        Node next;

        while (is_overflowing(node)) {
            if (node.page.id().is_root()) {
                fp = split_root;
            } else {
                fp = split_non_root;
            }
            Calico_Try(fp(tree, std::move(node), next));
            node = std::move(next);
        }
        tree.release(std::move(node));
        return Status::ok();
    }

    [[nodiscard]]
    static auto split_root(BPlusTree &tree, Node root, Node &out) -> Status
    {
        Node child;
        Calico_Try(tree.allocate(child, root.header.is_external));

        // Copy the cells.
        static constexpr auto after_root_headers = FileHeader::SIZE + NodeHeader::SIZE;
        auto data = child.page.span(after_root_headers, root.page.size() - after_root_headers);
        mem_copy(data, root.page.view(after_root_headers, data.size()));

        // Copy the header and cell pointers.
        child.header = root.header;
        data = child.page.span(NodeHeader::SIZE, root.header.cell_count * sizeof(PageSize));
        mem_copy(data, root.page.view(after_root_headers, data.size()));

        CALICO_EXPECT_TRUE(is_overflowing(root));
        std::swap(child.overflow, root.overflow);
        child.overflow_index = root.overflow_index;
        child.gap_size = root.gap_size + FileHeader::SIZE;

        root.header = NodeHeader {};
        root.header.is_external = false;
        root.header.next_id = child.page.id();
        root.initialize();

        Calico_Try(fix_parent_id(tree, child.page.id(), root.page.id()));
        tree.release(std::move(root));

        Calico_Try(fix_links(tree, child));
        out = std::move(child);
        return Status::ok();
    }

    template<class Predicate>
    [[nodiscard]]
    static auto transfer_cells_right_while(BPlusTree &tree, Node &left, Node &right, const Predicate &predicate) -> Status
    {
        CALICO_EXPECT_EQ(left.header.is_external, right.header.is_external);

        const auto &count = left.header.cell_count;
        Size counter {};

        while (count && predicate(left, right, counter++)) {
            const auto cell = read_cell(left, count - 1);
            Calico_Try(insert_cell(tree, right, 0, cell));
            CALICO_EXPECT_FALSE(is_overflowing(right));
            erase_cell(left, count - 1, cell.size);
        }
        return Status::ok();
    }

    [[nodiscard]]
    static auto split_external_non_root(BPlusTree &tree, Node &left, Node &right, Cell &separator) -> Status
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
            Node right_sibling;
            Calico_Try(tree.acquire(right_sibling, right.header.next_id, true));
            right_sibling.header.prev_id = right.page.id();
            tree.release(std::move(right_sibling));
        }

        if (overflow_idx == left.header.cell_count) {
            // Just transfer a single cell in this case. This should reduce the number of splits during a sequential write.
            Calico_Try(transfer_cells_right_while(tree, left, right, [](const auto &, const auto &, auto counter) {
                return !counter;
            }));
            Calico_Try(insert_cell(tree, right, right.header.cell_count, overflow));
            CALICO_EXPECT_FALSE(is_overflowing(right));
        } else if (overflow_idx == 0) {
            // We need the `!counter` because the condition following it may not be true if we got here from split_root().
            Calico_Try(transfer_cells_right_while(tree, left, right, [](const auto &src, const auto &dst, auto counter) {
                return !counter || usable_space(src) < usable_space(dst);
            }));
            Calico_Try(insert_cell(tree, left, 0, overflow));
            CALICO_EXPECT_FALSE(is_overflowing(left));
        } else {
            // We need to insert the overflow cell into either left or right, no matter what, even if it ends up being the separator.
            Calico_Try(transfer_cells_right_while(tree, left, right, [&overflow, overflow_idx](const auto &src, const auto &, auto counter) {
                const auto goes_in_src = src.header.cell_count > overflow_idx;
                const auto has_no_room = usable_space(src) < overflow.size + sizeof(PageSize);
                return !counter || (goes_in_src && has_no_room);
            }));

            if (left.header.cell_count > overflow_idx) {
                Calico_Try(insert_cell(tree, left, overflow_idx, overflow));
                CALICO_EXPECT_FALSE(is_overflowing(left));
            } else {
                Calico_Try(insert_cell(tree, right, 0, overflow));
                CALICO_EXPECT_FALSE(is_overflowing(right));
            }
        }

        separator = read_cell(right, 0);
        detach_cell(separator, tree.scratch(1));
        return Status::ok();
    }

    [[nodiscard]]
    static auto split_internal_non_root(BPlusTree &tree, Node &left, Node &right, Cell &separator) -> Status
    {
        CALICO_EXPECT_FALSE(left.header.is_external);
        CALICO_EXPECT_FALSE(right.header.is_external);
        CALICO_EXPECT_TRUE(is_overflowing(left));
        const auto overflow_idx = left.overflow_index;
        auto overflow = *left.overflow;
        left.overflow.reset();

        // In internal nodes, the next_id field refers to the rightmost child ID, and the prev_id field is unused.
        right.header.next_id = left.header.next_id;

        if (overflow_idx == left.header.cell_count) {
            Calico_Try(transfer_cells_right_while(tree, left, right, [](const auto &, const auto &, auto counter) {
                return !counter;
            }));
            Calico_Try(insert_cell(tree, right, right.header.cell_count, overflow));
            CALICO_EXPECT_FALSE(is_overflowing(right));
        } else if (overflow_idx == 0) {
            Calico_Try(transfer_cells_right_while(tree, left, right, [](const auto &src, const auto &dst, auto counter) {
                return !counter || usable_space(src) < usable_space(dst);
            }));
            Calico_Try(insert_cell(tree, left, 0, overflow));
            CALICO_EXPECT_FALSE(is_overflowing(left));
        } else {
            left.header.next_id = read_child_id(overflow);
            Calico_Try(transfer_cells_right_while(tree, left, right, [overflow_idx](const auto &src, const auto &, auto) {
                return src.header.cell_count > overflow_idx;
            }));
            separator = overflow;
            return Status::ok();
        }

        separator = read_cell(left, left.header.cell_count - 1);
        detach_cell(separator, tree.scratch(1));
        // TODO: Everywhere that we fail to erase overflow chains from keys, vacuum will not be able to continue. Use remove_cell() instead of erase_cell() to fix the chains.
        //       Note that we only need to do this when the key needs to be discarded. If we are transferring a cell from one node to another, we can leave the chain and
        //       fix the head back pointer when we call insert_cell().
        erase_cell(left, left.header.cell_count - 1, separator.size);
        left.header.next_id = read_child_id(separator);
        return Status::ok();
    }

    [[nodiscard]]
    static auto split_non_root(BPlusTree &tree, Node node, Node &out) -> Status
    {
        CALICO_EXPECT_FALSE(node.page.id().is_root());
        CALICO_EXPECT_TRUE(is_overflowing(node));
        
        Id parent_id;
        Calico_Try(find_parent_id(tree, node.page.id(), parent_id));
        CALICO_EXPECT_FALSE(parent_id.is_null());
        
        Node parent, sibling;
        Calico_Try(tree.acquire(parent, parent_id, true));
        Calico_Try(tree.allocate(sibling, node.header.is_external));
        Calico_Try(fix_parent_id(tree, sibling.page.id(), parent_id));

        Cell separator;
        if (node.header.is_external) {
            Calico_Try(split_external_non_root(tree, node, sibling, separator));
            Calico_Try(tree.m_payloads.promote(nullptr, separator, parent_id));
        } else {
            Calico_Try(split_internal_non_root(tree, node, sibling, separator));
        }
        NodeIterator itr {parent, {
            &tree.m_overflow,
            &tree.m_lhs_key,
            &tree.m_rhs_key,
        }};
        itr.seek(separator);
        Calico_Try(itr.status());

        CALICO_EXPECT_TRUE(separator.is_free);
        write_child_id(separator, node.page.id());
        Calico_Try(insert_cell(tree, parent, itr.index(), separator));

        CALICO_EXPECT_FALSE(is_overflowing(node));
        CALICO_EXPECT_FALSE(is_overflowing(sibling));

        const auto offset = !is_overflowing(parent);
        write_child_id(parent, itr.index() + offset, sibling.page.id());
        Calico_Try(fix_links(tree, sibling));
        tree.release(std::move(sibling));
        tree.release(std::move(node));
        out = std::move(parent);
        return Status::ok();
    }

    static auto resolve_underflow(BPlusTree &tree, Node node, const Slice &anchor) -> Status
    {
        while (is_underflowing(node)) {
            if (node.page.id().is_root()) {
                return fix_root(tree, std::move(node));
            }
            Id parent_id;
            Calico_Try(find_parent_id(tree, node.page.id(), parent_id));
            CALICO_EXPECT_FALSE(parent_id.is_null());
            
            Node parent;
            Calico_Try(tree.acquire(parent, parent_id, true));
            // NOTE: Searching for the anchor key from the node we took from should always give us the correct index
            //       due to the B+-tree ordering rules.
            NodeIterator itr {parent, {
                &tree.m_overflow,
                &tree.m_lhs_key,
                &tree.m_rhs_key,
            }};
            const auto exact = itr.seek(anchor);
            Calico_Try(itr.status());
            Calico_Try(fix_non_root(tree, std::move(node), parent, itr.index() + exact));
            node = std::move(parent);
        }
        tree.release(std::move(node));
        return Status::ok();
    }

    static auto transfer_first_cell_left(BPlusTree &tree, Node &src, Node &dst) -> Status
    {
        CALICO_EXPECT_EQ(src.header.is_external, dst.header.is_external);
        auto cell = read_cell(src, 0);
        Calico_Try(insert_cell(tree, dst, dst.header.cell_count, cell));
        CALICO_EXPECT_FALSE(is_overflowing(dst));
        erase_cell(src, 0, cell.size);
        CALICO_EXPECT_FALSE(is_overflowing(dst));
        return Status::ok();
    }

    static auto internal_merge_left(BPlusTree &tree, Node &left, Node &right, Node &parent, Size index) -> Status
    {
        CALICO_EXPECT_TRUE(is_underflowing(left));
        CALICO_EXPECT_FALSE(left.header.is_external);
        CALICO_EXPECT_FALSE(right.header.is_external);
        CALICO_EXPECT_FALSE(parent.header.is_external);

        auto separator = read_cell(parent, index);
        const auto sep_index = left.header.cell_count;

        detach_cell(separator, tree.scratch(2));
        write_child_id(separator, left.header.next_id);
        Calico_Try(insert_cell(tree, left, sep_index, separator));
        erase_cell(parent, index, separator.size);

        while (right.header.cell_count) {
            Calico_Try(transfer_first_cell_left(tree, right, left));
        }
        left.header.next_id = right.header.next_id;
        write_child_id(parent, index, left.page.id());
        return Status::ok();
    }

    static auto external_merge_left(BPlusTree &tree, Node &left, Node &right, Node &parent, Size index) -> Status
    {
        CALICO_EXPECT_TRUE(is_underflowing(left));
        CALICO_EXPECT_TRUE(left.header.is_external);
        CALICO_EXPECT_TRUE(right.header.is_external);
        CALICO_EXPECT_FALSE(parent.header.is_external);

        left.header.next_id = right.header.next_id;

        const auto separator = read_cell(parent, index);
        erase_cell(parent, index, separator.size);

        while (right.header.cell_count) {
            Calico_Try(transfer_first_cell_left(tree, right, left));
        }
        write_child_id(parent, index, left.page.id());

        if (!right.header.next_id.is_null()) {
            Node right_sibling;
            Calico_Try(tree.acquire(right_sibling, right.header.next_id, true));
            right_sibling.header.prev_id = left.page.id();
            tree.release(std::move(right_sibling));
        }
        return Status::ok();
    }

    static auto merge_left(BPlusTree &tree, Node &left, Node right, Node &parent, Size index) -> Status
    {
        if (left.header.is_external) {
            Calico_Try(external_merge_left(tree, left, right, parent, index));
        } else {
            Calico_Try(internal_merge_left(tree, left, right, parent, index));
        }
        Calico_Try(fix_links(tree, left));
        return tree.destroy(std::move(right));
    }

    static auto internal_merge_right(BPlusTree &tree, Node &left, Node &right, Node &parent, Size index) -> Status
    {
        CALICO_EXPECT_TRUE(is_underflowing(right));
        CALICO_EXPECT_FALSE(left.header.is_external);
        CALICO_EXPECT_FALSE(right.header.is_external);
        CALICO_EXPECT_FALSE(parent.header.is_external);

        auto separator = read_cell(parent, index);
        const auto sep_index = left.header.cell_count;

        detach_cell(separator, tree.scratch(2));
        write_child_id(separator, left.header.next_id);
        Calico_Try(insert_cell(tree, left, sep_index, separator));
        left.header.next_id = right.header.next_id;

        CALICO_EXPECT_EQ(read_child_id(parent, index + 1), right.page.id());
        write_child_id(parent, index + 1, left.page.id());
        erase_cell(parent, index, separator.size);

        // Transfer the rest of the cells. left shouldn't overflow.
        while (right.header.cell_count) {
            Calico_Try(transfer_first_cell_left(tree, right, left));
        }
        return Status::ok();
    }

    static auto external_merge_right(BPlusTree &tree, Node &left, Node &right, Node &parent, Size index) -> Status
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
            Calico_Try(transfer_first_cell_left(tree, right, left));
        }
        if (!right.header.next_id.is_null()) {
            Node right_sibling;
            Calico_Try(tree.acquire(right_sibling, right.header.next_id, true));
            right_sibling.header.prev_id = left.page.id();
            tree.release(std::move(right_sibling));
        }
        return Status::ok();
    }

    static auto merge_right(BPlusTree &tree, Node &left, Node right, Node &parent, Size index) -> Status
    {
        if (left.header.is_external) {
            Calico_Try(external_merge_right(tree, left, right, parent, index));
        } else {
            Calico_Try(internal_merge_right(tree, left, right, parent, index));
        }
        Calico_Try(fix_links(tree, left));
        return tree.destroy(std::move(right));
    }

    [[nodiscard]]
    static auto fix_non_root(BPlusTree &tree, Node node, Node &parent, Size index) -> Status
    {
        CALICO_EXPECT_FALSE(node.page.id().is_root());
        CALICO_EXPECT_TRUE(is_underflowing(node));
        CALICO_EXPECT_FALSE(is_overflowing(parent));

        if (index > 0) {
            Node left;
            Calico_Try(tree.acquire(left, read_child_id(parent, index - 1), true));
            if (left.header.cell_count == 1) {
                Calico_Try(merge_right(tree, left, std::move(node), parent, index - 1));
                tree.release(std::move(left));
                CALICO_EXPECT_FALSE(is_overflowing(parent));
                return Status::ok();
            }
            Calico_Try(rotate_right(tree, parent, left, node, index - 1));
            tree.release(std::move(left));
        } else {
            Node right;
            Calico_Try(tree.acquire(right, read_child_id(parent, index + 1), true));
            if (right.header.cell_count == 1) {
                Calico_Try(merge_left(tree, node, std::move(right), parent, index));
                tree.release(std::move(node));
                CALICO_EXPECT_FALSE(is_overflowing(parent));
                return Status::ok();
            }
            Calico_Try(rotate_left(tree, parent, node, right, index));
            tree.release(std::move(right));
        }

        CALICO_EXPECT_FALSE(is_overflowing(node));
        tree.release(std::move(node));

        if (is_overflowing(parent)) {
            const auto saved_id = parent.page.id();
            Calico_Try(resolve_overflow(tree, std::move(parent)));
            Calico_Try(tree.acquire(parent, saved_id, true));
        }
        return Status::ok();
    }

    [[nodiscard]]
    static auto fix_root(BPlusTree &tree, Node root) -> Status
    {
        CALICO_EXPECT_TRUE(root.page.id().is_root());

        // If the root is external here, the whole tree must be empty.
        if (!root.header.is_external) {
            Node child;
            Calico_Try(tree.acquire(child, root.header.next_id, true));

            // We don't have enough room to transfer the child contents into the root, due to the space occupied by
            // the file header. In this case, we'll just split the child and insert the median cell into the root.
            // Note that the child needs an overflow cell for the split routine to work. We'll just fake it by
            // extracting an arbitrary cell and making it the overflow cell.
            if (usable_space(child) < FileHeader::SIZE) {
                child.overflow = read_cell(child, 0);
                detach_cell(*child.overflow, tree.scratch(0));
                erase_cell(child, 0);
                tree.release(std::move(root));
                Node parent;
                Calico_Try(split_non_root(tree, std::move(child), parent));
                tree.release(std::move(parent));
                Calico_Try(tree.acquire(root, Id::root(), true));
            } else {
                merge_root(root, child);
                root.meta = child.meta; // TODO: Don't parse the cell incorrectly when fixing links in the root!!!
                Calico_Try(tree.destroy(std::move(child)));
            }
            Calico_Try(fix_links(tree, root));
        }
        tree.release(std::move(root));
        return Status::ok();
    }

    [[nodiscard]]
    static auto rotate_left(BPlusTree &tree, Node &parent, Node &left, Node &right, Size index) -> Status
    {
        if (left.header.is_external) {
            return external_rotate_left(tree, parent, left, right, index);
        } else {
            return internal_rotate_left(tree, parent, left, right, index);
        }
    }

    static auto external_rotate_left(BPlusTree &tree, Node &parent, Node &left, Node &right, Size index) -> Status
    {
        CALICO_EXPECT_TRUE(left.header.is_external);
        CALICO_EXPECT_TRUE(right.header.is_external);
        CALICO_EXPECT_FALSE(parent.header.is_external);
        CALICO_EXPECT_GT(parent.header.cell_count, 0);
        CALICO_EXPECT_GT(right.header.cell_count, 1);

        auto lowest = read_cell(right, 0);
        Calico_Try(insert_cell(tree, left, left.header.cell_count, lowest));
        CALICO_EXPECT_FALSE(is_overflowing(left));
        erase_cell(right, 0);

        auto separator = read_cell(right, 0);
        Calico_Try(tree.m_payloads.promote(tree.scratch(1), separator, parent.page.id()));
        write_child_id(separator, left.page.id());

        erase_cell(parent, index, read_cell(parent, index).size);
        return insert_cell(tree, parent, index, separator);
    }

    [[nodiscard]]
    static auto internal_rotate_left(BPlusTree &tree, Node &parent, Node &left, Node &right, Size index) -> Status
    {
        CALICO_EXPECT_FALSE(parent.header.is_external);
        CALICO_EXPECT_FALSE(left.header.is_external);
        CALICO_EXPECT_FALSE(right.header.is_external);
        CALICO_EXPECT_GT(parent.header.cell_count, 0);
        CALICO_EXPECT_GT(right.header.cell_count, 1);

        Node child;
        Calico_Try(tree.acquire(child, read_child_id(right, 0), true));
        const auto saved_id = left.header.next_id;
        left.header.next_id = child.page.id();
        Calico_Try(fix_parent_id(tree, child.page.id(), left.page.id()));
        tree.release(std::move(child));

        auto separator = read_cell(parent, index);
        Calico_Try(insert_cell(tree, left, left.header.cell_count, separator));
        CALICO_EXPECT_FALSE(is_overflowing(left));
        write_child_id(left, left.header.cell_count - 1, saved_id);
        erase_cell(parent, index, separator.size);

        auto lowest = read_cell(right, 0);
        detach_cell(lowest, tree.scratch(2));
        erase_cell(right, 0);
        write_child_id(lowest, left.page.id());
        return insert_cell(tree, parent, index, lowest);
    }

    [[nodiscard]]
    static auto rotate_right(BPlusTree &tree, Node &parent, Node &left, Node &right, Size index) -> Status
    {
        if (left.header.is_external) {
            return external_rotate_right(tree, parent, left, right, index);
        } else {
            return internal_rotate_right(tree, parent, left, right, index);
        }
    }

    [[nodiscard]]
    static auto external_rotate_right(BPlusTree &tree, Node &parent, Node &left, Node &right, Size index) -> Status
    {
        CALICO_EXPECT_TRUE(left.header.is_external);
        CALICO_EXPECT_TRUE(right.header.is_external);
        CALICO_EXPECT_FALSE(parent.header.is_external);
        CALICO_EXPECT_GT(parent.header.cell_count, 0);
        CALICO_EXPECT_GT(left.header.cell_count, 1);

        auto highest = read_cell(left, left.header.cell_count - 1);
        Calico_Try(insert_cell(tree, right, 0, highest));
        CALICO_EXPECT_FALSE(is_overflowing(right));

        auto separator = highest;
        Calico_Try(tree.m_payloads.promote(tree.scratch(1), separator, parent.page.id()));
        write_child_id(separator, left.page.id());

        // Don't erase the cell until it has been detached.
        erase_cell(left, left.header.cell_count - 1);

        erase_cell(parent, index, read_cell(parent, index).size);
        Calico_Try(insert_cell(tree, parent, index, separator));
        return Status::ok();
    }

    [[nodiscard]]
    static auto internal_rotate_right(BPlusTree &tree, Node &parent, Node &left, Node &right, Size index) -> Status
    {
        CALICO_EXPECT_FALSE(parent.header.is_external);
        CALICO_EXPECT_FALSE(left.header.is_external);
        CALICO_EXPECT_FALSE(right.header.is_external);
        CALICO_EXPECT_GT(parent.header.cell_count, 0);
        CALICO_EXPECT_GT(left.header.cell_count, 1);

        Node child;
        Calico_Try(tree.acquire(child, left.header.next_id, true));
        const auto child_id = child.page.id();
        Calico_Try(fix_parent_id(tree, child.page.id(), right.page.id()));
        left.header.next_id = read_child_id(left, left.header.cell_count - 1);
        tree.release(std::move(child));

        auto separator = read_cell(parent, index);
        Calico_Try(insert_cell(tree, right, 0, separator));
        CALICO_EXPECT_FALSE(is_overflowing(right));
        write_child_id(right, 0, child_id);
        erase_cell(parent, index, separator.size);

        auto highest = read_cell(left, left.header.cell_count - 1);
        detach_cell(highest, tree.scratch(2));
        write_child_id(highest, left.page.id());
        erase_cell(left, left.header.cell_count - 1, highest.size);
        Calico_Try(insert_cell(tree, parent, index, highest));
        return Status::ok();
    }
};

PayloadManager::PayloadManager(const NodeMeta &meta, OverflowList &overflow)
    : m_meta {&meta},
      m_overflow {&overflow}
{}

auto PayloadManager::emplace(Byte *scratch, Node &node, const Slice &key, const Slice &value, Size index) -> Status
{
    CALICO_EXPECT_TRUE(node.header.is_external);

    auto k = key.size();
    auto v = value.size();
    const auto local_size = compute_local_size(k, v, node.meta->min_local, node.meta->max_local);
    const auto has_remote = k + v > local_size;

    if (k > local_size) {
        k = local_size;
        v = 0;
    } else if (has_remote) {
        v = local_size - k;
    }

    CALICO_EXPECT_EQ(k + v, local_size);
    auto total_size = local_size + varint_length(key.size()) + varint_length(value.size());

    Id overflow_id;
    if (has_remote) {
        Calico_Try(m_overflow->write_chain(overflow_id, node.page.id(), key.range(k), value.range(v)));
        total_size += sizeof(overflow_id);
    }

    const auto emplace = [&](auto *out) {
        ::Calico::emplace_cell(out, key.size(), value.size(), key.range(0, k), value.range(0, v), overflow_id);
    };

    if (const auto offset = allocate_block(node, static_cast<PageSize>(index), static_cast<PageSize>(total_size))) {
        // Write directly into the node.
        emplace(node.page.data() + offset);
    } else {
        // The node has overflowed. Write the cell to scratch memory.
        emplace(scratch);
        node.overflow = parse_external_cell(*node.meta, scratch);
        node.overflow->is_free = true;
    }
    return Status::ok();
}

auto PayloadManager::promote(Byte *scratch, Cell &cell, Id parent_id) -> Status
{
    detach_cell(cell, scratch);

    // "scratch" should have enough room before its "m_data" member to write the left child ID.
    const auto header_size = sizeof(Id) + varint_length(cell.key_size);
    cell.ptr = cell.key - header_size;
    cell.local_size = compute_local_size(cell.key_size, 0, m_meta->min_local, m_meta->max_local);
    cell.size = header_size + cell.local_size;
    cell.has_remote = false;

    if (cell.key_size > cell.local_size) {
        // Part of the key is on an overflow page. No value is stored locally in this case, so the local size computation is still correct.
        Id overflow_id;
        Calico_Try(m_overflow->copy_chain(overflow_id, parent_id, read_overflow_id(cell), cell.key_size - cell.local_size));
        write_overflow_id(cell, overflow_id);
        cell.size += sizeof(Id);
        cell.has_remote = true;
    }
    return Status::ok();
}

auto PayloadManager::collect_key(std::string &scratch, const Cell &cell, Slice &out) const -> Status
{
    if (scratch.size() < cell.key_size) {
        scratch.resize(cell.key_size);
    }
    if (!cell.has_remote || cell.key_size <= cell.local_size) {
        mem_copy(scratch, {cell.key, cell.key_size});
        out = Slice {scratch.data(), cell.key_size};
        return Status::ok();
    }
    Span span {scratch};
    span.truncate(cell.key_size);
    mem_copy(span, {cell.key, cell.local_size});

    Calico_Try(m_overflow->read_chain(span.range(cell.local_size), read_overflow_id(cell)));
    out = span.range(0, cell.key_size);
    return Status::ok();
}

auto PayloadManager::collect_value(std::string &scratch, const Cell &cell, Slice &out) const -> Status
{
    Size value_size;
    decode_varint(cell.ptr, value_size);
    if (scratch.size() < value_size) {
        scratch.resize(value_size);
    }
    if (!cell.has_remote) {
        mem_copy(scratch, {cell.key + cell.key_size, value_size});
        out = Slice {scratch.data(), value_size};
        return Status::ok();
    }
    Size remote_key_size {};
    if (cell.key_size > cell.local_size) {
        remote_key_size = cell.key_size - cell.local_size;
    }
    Span span {scratch};
    span.truncate(value_size);

    if (remote_key_size == 0) {
        const auto local_value_size = cell.local_size - cell.key_size;
        mem_copy(span, {cell.key + cell.key_size, local_value_size});
        span.advance(local_value_size);
    }

    Calico_Try(m_overflow->read_chain(span, read_overflow_id(cell), remote_key_size));
    out = Span {scratch}.truncate(value_size);
    return Status::ok();
}

BPlusTree::BPlusTree(Pager &pager)
    // min_local and max_local fields are only needed in external nodes.
    : m_external_meta {
          external_cell_size, parse_external_cell,
          compute_min_local(pager.page_size()),
          compute_max_local(pager.page_size())},
      m_internal_meta {
          internal_cell_size, parse_internal_cell,
          m_external_meta.min_local,
          m_external_meta.max_local},
      m_pointers {pager},
      m_freelist {pager, m_pointers},
      m_overflow {pager, m_freelist, m_pointers},
      m_payloads {m_external_meta, m_overflow},
      m_pager {&pager}
{
    // Scratch memory for defragmenting nodes and storing cells.
    m_scratch[0].resize(pager.page_size());
    m_scratch[1].resize(pager.page_size());
    m_scratch[2].resize(pager.page_size());
    m_scratch[3].resize(pager.page_size());
}

auto BPlusTree::setup(Node &out) -> Status
{
    CALICO_EXPECT_EQ(m_pager->page_count(), 0);
    Calico_Try(allocate(out, true));
    CALICO_EXPECT_EQ(m_pager->page_count(), 1);
    return Status::ok();
}

auto BPlusTree::make_fresh_node(Node &out, bool is_external) const -> void
{
    out.header = NodeHeader {};
    out.header.is_external = is_external;
    out.scratch = m_scratch.back().data();
    if (out.header.is_external) {
        out.meta = &m_external_meta;
    } else {
        out.meta = &m_internal_meta;
    }
    out.initialize();
}

auto BPlusTree::make_existing_node(Node &out) const  -> void
{
    out.scratch = m_scratch.back().data();
    out.header.read(out.page);
    if (out.header.is_external) {
        out.meta = &m_external_meta;
    } else {
        out.meta = &m_internal_meta;
    }
    out.initialize();
}

auto BPlusTree::scratch(Size index) -> Byte *
{
    // Reserve the last scratch buffer for defragmentation.
    CALICO_EXPECT_LT(index, m_scratch.size() - 1);
    return m_scratch[index].data() + sizeof(Id) - 1;
}

auto BPlusTree::allocate(Node &out, bool is_external) -> Status
{
    const auto fetch_unused_page = [this](Page &page) {
        if (m_freelist.is_empty()) {
            Calico_Try(m_pager->allocate(page));
            // Since this is a fresh page from the end of the file, it could be a pointer map page. If so,
            // it is already blank, so just skip it and allocate another. It'll get filled in as the pages
            // following it are used.
            if (BPlusTreeInternal::is_pointer_map(*this, page.id())) {
                m_pager->release(std::move(page));
                Calico_Try(m_pager->allocate(page));
            }
            return Status::ok();
        } else {
            return m_freelist.pop(page);
        }
    };
    Calico_Try(fetch_unused_page(out.page));
    CALICO_EXPECT_FALSE(BPlusTreeInternal::is_pointer_map(*this, out.page.id()));
    make_fresh_node(out, is_external);
    return Status::ok();
}

auto BPlusTree::acquire(Node &out, Id pid, bool upgrade) const -> Status
{
    CALICO_EXPECT_FALSE(BPlusTreeInternal::is_pointer_map(*this, pid));
    Calico_Try(m_pager->acquire(pid, out.page));

    if (upgrade) {
        m_pager->upgrade(out.page);
    }
    make_existing_node(out);
    return Status::ok();
}

auto BPlusTree::release(Node node) const -> void
{
    m_pager->release(std::move(node).take());
}

auto BPlusTree::destroy(Node node) -> Status
{
    // Pointer map pages should never be explicitly destroyed.
    CALICO_EXPECT_FALSE(BPlusTreeInternal::is_pointer_map(*this, node.page.id()));
    return m_freelist.push(std::move(node.page));
}

auto BPlusTree::insert(const Slice &key, const Slice &value, bool &exists) -> Status
{
    exists = false;

    SearchResult slot;
    Calico_Try(BPlusTreeInternal::find_external_slot(*this, key, slot));
    auto [node, index, exact] = std::move(slot);
    m_pager->upgrade(node.page);

    if (exact) {
        Calico_Try(BPlusTreeInternal::remove_cell(*this, node, index));
    }

    Calico_Try(m_payloads.emplace(scratch(0), node, key, value, index));
    Calico_Try(BPlusTreeInternal::resolve_overflow(*this, std::move(node)));
    exists = exact;
    return Status::ok();
}

auto BPlusTree::erase(const Slice &key) -> Status
{
    SearchResult slot;
    Calico_Try(BPlusTreeInternal::find_external_slot(*this, key, slot));
    auto [node, index, exact] = std::move(slot);

    if (exact) {
        const auto cell = read_cell(node, index);
        Slice anchor;
        Calico_Try(collect_key(m_anchor, cell, anchor));

        m_pager->upgrade(node.page);
        Calico_Try(BPlusTreeInternal::remove_cell(*this, node, index));
        return BPlusTreeInternal::resolve_underflow(*this, std::move(node), anchor);
    }
    release(std::move(node));
    return Status::not_found("not found");
}

auto BPlusTree::lowest(Node &out) const -> Status
{
    Calico_Try(acquire(out, Id::root()));
    while (!out.header.is_external) {
        const auto next_id = read_child_id(out, 0);
        release(std::move(out));
        Calico_Try(acquire(out, next_id));
    }
    return Status::ok();
}

auto BPlusTree::highest(Node &out) const -> Status
{
    Calico_Try(acquire(out, Id::root()));
    while (!out.header.is_external) {
        const auto next_id = out.header.next_id;
        release(std::move(out));
        Calico_Try(acquire(out, next_id));
    }
    return Status::ok();
}

auto BPlusTree::collect_key(std::string &scratch, const Cell &cell, Slice &out) const -> Status
{
    return m_payloads.collect_key(scratch, cell, out);
}

auto BPlusTree::collect_value(std::string &scratch, const Cell &cell, Slice &out) const -> Status
{
    return m_payloads.collect_value(scratch, cell, out);
}

auto BPlusTree::search(const Slice &key, SearchResult &out) -> Status
{
    return BPlusTreeInternal::find_external_slot(*this, key, out);
}

auto BPlusTree::vacuum_step(Page &free, Id last_id) -> Status
{
    PointerMap::Entry entry;
    Calico_Try(m_pointers.read_entry(last_id, entry));

    const auto fix_basic_link = [&entry, &free, this]() -> Status {
        Page parent;
        Calico_Try(m_pager->acquire(entry.back_ptr, parent));
        m_pager->upgrade(parent);
        write_next_id(parent, free.id());
        m_pager->release(std::move(parent));
        return Status::ok();
    };

    switch (entry.type) {
        case PointerMap::FREELIST_LINK: {
            if (last_id == m_freelist.m_head) {
                m_freelist.m_head = free.id();
            } else {
                // Back pointer points to another freelist page.
                CALICO_EXPECT_FALSE(entry.back_ptr.is_null());
                Calico_Try(fix_basic_link());
                Page last;
                Calico_Try(m_pager->acquire(last_id, last));
                if (const auto next_id = read_next_id(last); !next_id.is_null()) {
                    Calico_Try(BPlusTreeInternal::fix_parent_id(*this, next_id, free.id(), PointerMap::FREELIST_LINK));
                }
                m_pager->release(std::move(last));
            }
            break;
        }
        case PointerMap::OVERFLOW_LINK: {
            // Back pointer points to another overflow chain link, or the head of the chain.
            Calico_Try(fix_basic_link());
            break;
        }
        case PointerMap::OVERFLOW_HEAD: {
            // Back pointer points to the node that the overflow chain is rooted in. Search through that nodes cells
            // for the target overflowing cell.
            Node parent;
            Calico_Try(acquire(parent, entry.back_ptr, true));
            bool found {};
            for (Size i {}; i < parent.header.cell_count; ++i) {
                auto cell = read_cell(parent, i);
                found = cell.has_remote && read_overflow_id(cell) == last_id;
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
            Node parent;
            Calico_Try(acquire(parent, entry.back_ptr, true));
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
            Node last;
            Calico_Try(acquire(last, last_id, true));
            for (Size i {}; i < last.header.cell_count; ++i) {
                const auto cell = read_cell(last, i);
                Calico_Try(BPlusTreeInternal::maybe_fix_overflow_chain(*this, cell, free.id()));
                if (!last.header.is_external) {
                    Calico_Try(BPlusTreeInternal::fix_parent_id(*this, read_child_id(last, i), free.id()));
                }
            }
            if (!last.header.is_external) {
                Calico_Try(BPlusTreeInternal::fix_parent_id(*this, last.header.next_id, free.id()));
            } else {
                if (!last.header.prev_id.is_null()) {
                    Node prev;
                    Calico_Try(acquire(prev, last.header.prev_id, true));
                    prev.header.next_id = free.id();
                    release(std::move(prev));
                }
                if (!last.header.next_id.is_null()) {
                    Node next;
                    Calico_Try(acquire(next, last.header.next_id, true));
                    next.header.prev_id = free.id();
                    release(std::move(next));
                }
            }
            release(std::move(last));
        }
    }
    Calico_Try(m_pointers.write_entry(last_id, {}));
    Calico_Try(m_pointers.write_entry(free.id(), entry));
    Page last;
    Calico_Try(m_pager->acquire(last_id, last));
    if (entry.type != PointerMap::NODE) {
        if (const auto next_id = read_next_id(last); !next_id.is_null()) {
            PointerMap::Entry next_entry;
            Calico_Try(m_pointers.read_entry(next_id, next_entry));
            next_entry.back_ptr = free.id();
            Calico_Try(m_pointers.write_entry(next_id, next_entry));
        }
    }
    mem_copy(free.span(sizeof(Lsn), free.size() - sizeof(Lsn)),
             last.view(sizeof(Lsn), last.size() - sizeof(Lsn)));
    m_pager->release(std::move(last));
    return Status::ok();
}

auto BPlusTree::vacuum_one(Id target, bool &success) -> Status
{
    if (BPlusTreeInternal::is_pointer_map(*this, target)) {
        success = true;
        return Status::ok();
    }
    if (target.is_root() || m_freelist.is_empty()) {
        success = false;
        return Status::ok();
    }

    // Swap the head of the freelist with the last page in the file.
    Page head;
    Calico_Try(m_freelist.pop(head));
    if (target != head.id()) {
        // Swap the last page with the freelist head.
        Calico_Try(vacuum_step(head, target));
    }
    m_pager->release(std::move(head));
    success = true;
    return Status::ok();
}

auto BPlusTree::save_state(FileHeader &header) const -> void
{
    header.freelist_head = m_freelist.m_head;
}

auto BPlusTree::load_state(const FileHeader &header) -> void
{
    m_freelist.m_head = header.freelist_head;
}

#define CHECK_OK(expr) \
    do { \
        if (const auto check_s = (expr); !check_s.is_ok()) { \
            std::fprintf(stderr, "error: encountered %s status \"%s\"\n", get_status_name(check_s), check_s.what().data()); \
            std::abort(); \
        } \
    } while (0)

#define CHECK_TRUE(expr) \
    do { \
        if (!(expr)) { \
            std::fprintf(stderr, "error: \"%s\" was false\n", #expr); \
            std::abort(); \
        } \
    } while (0)

class BPlusTreeValidator {
public:
    using Callback = std::function<void(Node &, Size)>;

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
                Node next;
                CHECK_OK(m_tree->acquire(next, read_child_id(cell), false));
                collect_levels(data, std::move(next), level + 1);
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
        if (!node.header.is_external) {
            Node next;
            CHECK_OK(m_tree->acquire(next, node.header.next_id, false));
            collect_levels(data, std::move(next), level + 1);
        }

        m_tree->release(std::move(node));
    }

    auto traverse_inorder(const Callback &callback) -> void
    {
        Node root;
        CHECK_OK(m_tree->acquire(root, Id::root(), false));
        traverse_inorder_helper(std::move(root), callback);
    }

    auto validate_freelist(Id head) -> void
    {
        auto &pager = *m_tree->m_pager;
        auto &freelist = m_tree->m_freelist;
        if (freelist.is_empty()) {
            return;
        }
        CHECK_TRUE(!head.is_null());
        Page page;
        CHECK_OK(pager.acquire(head, page));
        Id parent_id;
        CHECK_OK(BPlusTreeInternal::find_parent_id(*m_tree, page.id(), parent_id));
        CHECK_TRUE(parent_id == Id::null());

        for (; ; ) {
            const auto next_id = read_next_id(page);
            if (next_id.is_null()) {
                break;
            }
            Id found_id;
            CHECK_OK(BPlusTreeInternal::find_parent_id(*m_tree, next_id, found_id));
            CHECK_TRUE(found_id == page.id());
            pager.release(std::move(page));
            CHECK_OK(pager.acquire(next_id, page));
        }
        pager.release(std::move(page));
    }

    auto validate_overflow(Id overflow_id, Id parent_id, Size overflow_size) -> void
    {
        auto &pager = *m_tree->m_pager;

        Id found_id;
        CHECK_OK(BPlusTreeInternal::find_parent_id(*m_tree, overflow_id, found_id));
        CHECK_TRUE(found_id == parent_id);

        Page page;
        CHECK_OK(pager.acquire(overflow_id, page));

        for (Size n {}; n < overflow_size; ) {
            const Id next_id {get_u64(page.data() + sizeof(Lsn))};
            n += pager.page_size() - sizeof(Lsn) - sizeof(Id);
            if (n >= overflow_size) {
                CHECK_TRUE(next_id.is_null());
                break;
            }
            CHECK_OK(BPlusTreeInternal::find_parent_id(*m_tree, next_id, found_id));
            CHECK_TRUE(found_id == page.id());

            pager.release(std::move(page));
            CHECK_OK(pager.acquire(next_id, page));
        }
        pager.release(std::move(page));
    }

    auto validate_siblings() -> void
    {
        const auto validate_possible_overflows = [this](auto &node) {
            for (Size i {}; i < node.header.cell_count; ++i) {
                const auto cell = read_cell(node, i);
                if (cell.has_remote) {
                    Size value_size;
                    decode_varint(cell.ptr, value_size);
                    const auto remote_size = cell.key_size + value_size - cell.local_size;
                    validate_overflow(read_overflow_id(cell), node.page.id(), remote_size);
                }
            }
        };

        // Find the leftmost external node.
        Node node;
        CHECK_OK(m_tree->acquire(node, Id::root(), false));
        while (!node.header.is_external) {
            const auto id = read_child_id(node, 0);
            m_tree->release(std::move(node));
            CHECK_OK(m_tree->acquire(node, id, false));
        }
        // Traverse across the sibling chain to the right.
        while (!node.header.next_id.is_null()) {
            validate_possible_overflows(node);
            Node right;
            CHECK_OK(m_tree->acquire(right, node.header.next_id, false));
            std::string lhs_buffer, rhs_buffer;
            Slice lhs_key;
            CHECK_OK(m_tree->collect_key(lhs_buffer, read_cell(node, 0), lhs_key));
            Slice rhs_key;
            CHECK_OK(m_tree->collect_key(rhs_buffer, read_cell(right, 0), rhs_key));
            CHECK_TRUE(lhs_key < rhs_key);
            CHECK_TRUE(right.header.prev_id == node.page.id());
            m_tree->release(std::move(node));
            node = std::move(right);
        }
        validate_possible_overflows(node);
        m_tree->release(std::move(node));
    }

    auto validate_parent_child() -> void
    {
        auto check = [this](auto &node, auto index) -> void {
            Node child;
            CHECK_OK(m_tree->acquire(child, read_child_id(node, index), false));

            Id parent_id;
            CHECK_OK(BPlusTreeInternal::find_parent_id(*m_tree, child.page.id(), parent_id));
            CHECK_TRUE(parent_id == node.page.id());

            m_tree->release(std::move(child));
        };
        traverse_inorder([f = std::move(check)](Node &node, Size index) -> void {
            const auto count = node.header.cell_count;
            CHECK_TRUE(index < count);

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

                Node next;
                CHECK_OK(m_tree->acquire(next, next_id, false));
                traverse_inorder_helper(std::move(next), callback);

                CHECK_OK(m_tree->acquire(node, saved_id, false));
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
        CHECK_TRUE(target <= data.levels.size());
        Size i {};

        auto s_itr = begin(data.spaces);
        auto L_itr = begin(data.levels);
        while (s_itr != end(data.spaces)) {
            CHECK_TRUE(L_itr != end(data.levels));
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
        CHECK_TRUE(data.levels.size() > level);
        CHECK_TRUE(data.levels.size() == data.spaces.size());
    }

    BPlusTree *m_tree {};
};

auto BPlusTree::TEST_to_string() -> std::string
{
    std::string repr;
    BPlusTreeValidator::PrintData data;
    BPlusTreeValidator validator {*this};

    Node root;
    CHECK_OK(acquire(root, Id::root()));
    validator.collect_levels(data, std::move(root), 0);
    for (const auto &level: data.levels) {
        repr.append(level + '\n');
    }

    return repr;
}

auto BPlusTree::TEST_check_order() -> void
{
    BPlusTreeValidator validator {*this};
    std::string last_key;
    auto is_first = true;

    validator.traverse_inorder([&](auto &node, auto index) -> void {
        std::string buffer;
        Slice key;
        CHECK_OK(collect_key(buffer, read_cell(node, index), key));
        if (is_first) {
            is_first = false;
        } else {
            CHECK_TRUE(!key.is_empty());
            CHECK_TRUE(last_key <= key);
        }
        last_key = key.to_string();
    });
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
    validator.traverse_inorder([](auto &node, auto index) -> void {
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

#undef CHECK_TRUE
#undef CHECK_OK

} // namespace Calico
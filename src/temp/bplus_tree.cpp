#include "bplus_tree.h"
#include "overflow.h"
#include "pager/pager.h"
#include "utils/utils.h"

namespace Calico {

class BPlusTreeInternal {
public:
    [[nodiscard]]
    static auto make_node(BPlusTree &tree, Page page) -> Node
    {
        Node node {std::move(page), scratch_at(tree, 3)};
        if (node.header.is_external) {
            node.meta = &tree.m_external_meta;
        } else {
            node.meta = &tree.m_internal_meta;
        }
        return node;
    }

    static auto init_node(Node &node) -> void
    {
        std::memset(reinterpret_cast<Byte *>(&node.header), 0, sizeof(node.header));
        node.header.cell_start = static_cast<PageSize>(node.page.size());
    }

    [[nodiscard]]
    static auto scratch_at(BPlusTree &tree, Size index) -> Byte *
    {
        CALICO_EXPECT_LT(index, tree.m_scratch.size());
        return tree.m_scratch[index].data();
    }

    [[nodiscard]]
    static auto allocate_node(BPlusTree &tree, bool is_external) -> tl::expected<Node, Status>
    {
        CALICO_NEW_R(page, tree.m_pager->allocate_());
        tree.m_pager->upgrade_(page);
        if (is_external) {
            page.span(sizeof(Id), 1)[0] = 1;
        }
        return make_node(tree, std::move(page));
    }
    
    [[nodiscard]]
    static auto acquire_node(BPlusTree &tree, Id pid, bool upgrade = false) -> tl::expected<Node, Status>
    {
        CALICO_NEW_R(page, tree.m_pager->acquire_(pid));
        if (upgrade) {
            tree.m_pager->upgrade_(page);
        }
        return make_node(tree, std::move(page));
    }
    
    static auto release_node(BPlusTree &tree, Node node) -> void
    {
        tree.m_pager->release_(std::move(node).take());
    }

    [[nodiscard]]
    static auto find_external_slot(BPlusTree &tree, const Slice &key, Node node) -> tl::expected<BPlusTree::FindResult, Status>
    {
        for (; ; ) {
            Node::Iterator itr {node};
            const auto exact = itr.seek(key);

            if (node.header.is_external)
                return BPlusTree::FindResult {std::move(node), itr.index(), exact};

            const auto next_id = read_child_id(node, itr.index());
            release_node(tree, std::move(node));
            CALICO_PUT_R(node, acquire_node(tree, next_id));
        }
    }

    [[nodiscard]]
    static auto maybe_fix_child_parent_links(BPlusTree &tree, Node &node) -> tl::expected<void, Status>
    {
        if (!node.header.is_external) {
            const auto parent_id = node.page.id();
            const auto fix_connection = [&](Id child_id) -> tl::expected<void, Status> {
                CALICO_NEW_R(child, acquire_node(tree, child_id, true));
                child.header.parent_id = parent_id;
                release_node(tree, std::move(child));
                return {};
            };

            for (Size index {}; index <= node.header.cell_count; ++index) {
                CALICO_TRY_R(fix_connection(read_child_id(node, index)));
            }

            if (node.overflow.has_value()) {
                CALICO_TRY_R(fix_connection(read_child_id(*node.overflow)));
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
            CALICO_PUT_R(overflow_id, write_chain(*tree.m_pager, tree.m_free_list, remote));
        }

        const auto emplace = [k = key, v = local, o = overflow_id, value](auto *out) {
            ::Calico::emplace_cell(out, value.size(), k, v, o);
        };

        // Copy the data directly into the node.
        if (const auto offset = allocate_block(node, PageSize(index), PageSize(total_size))) {
            emplace(node.page.data() + offset);
        } else {
            // The node has overflowed. Copy the payload data to scratch memory.
            auto *scratch = scratch_at(tree, 0) + EXTERNAL_SHIFT;
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

        while (node.overflow.has_value()) {
            if (node.page.id().is_root()) {
                fp = split_root;
            } else {
                fp = split_non_root;
            }
            CALICO_NEW_R(temp, fp(tree, std::move(node)));
            node = std::move(temp);
        }
        release_node(tree, std::move(node));
        return {};
    }

    [[nodiscard]]
    static auto split_root(BPlusTree &tree, Node root) -> tl::expected<Node, Status>
    {
        CALICO_NEW_R(child, allocate_node(tree, root.header.is_external));

        // Copy the cells.
        static constexpr auto after_root_headers = FileHeader::SIZE + NodeHeader::SIZE;
        auto out = child.page.span(after_root_headers, root.page.size() - after_root_headers);
        mem_copy(out, root.page.view(after_root_headers, out.size()));

        // Copy the header and cell pointers.
        child.header = root.header;
        out = child.page.span(NodeHeader::SIZE, root.header.cell_count * sizeof(PageSize));
        mem_copy(out, root.page.view(after_root_headers, out.size()));

        CALICO_EXPECT_TRUE(root.overflow.has_value());
        child.overflow = std::exchange(root.overflow, std::nullopt);
        child.overflow_index = root.overflow_index;

        init_node(root);
        root.header.is_external = false;
        root.header.next_id = child.page.id();
        child.header.parent_id = root.page.id();
        child.gap_size = root.gap_size + FileHeader::SIZE;
        release_node(tree, std::move(root));
        CALICO_TRY_R(maybe_fix_child_parent_links(tree, child));

        if (!child.overflow->is_free)
            detach_cell(*child.overflow, scratch_at(tree, 0) + EXTERNAL_SHIFT);
        return child;
    }

    template<class Predicate>
    static auto transfer_cells_left_while(Node &src, Node &dst, const Predicate &predicate) -> void
    {
        const auto &header = src.header;
        Size counter {};

        while (header.cell_count && predicate(src, dst, counter++)) {
            const auto last = static_cast<Size>(header.cell_count - 1);
            const auto cell = read_cell(src, last);
            write_cell(dst, 0, cell);
            CALICO_EXPECT_FALSE(dst.overflow.has_value());
            erase_cell(src, last, cell.size);
        }
    }

    template<class Predicate>
    static auto transfer_cells_right_while(Node &left, Node &right, const Predicate &predicate) -> void
    {
        const auto &header = left.header;
        Size counter {};
        
        while (header.cell_count && predicate(left, right, counter++)) {
            const auto last = static_cast<Size>(header.cell_count - 1);
            const auto cell = read_cell(left, last);
            write_cell(right, 0, cell);
            CALICO_EXPECT_FALSE(right.overflow.has_value());
            erase_cell(left, last, cell.size);
        }
    }

    [[nodiscard]]
    static auto split_external_non_root(Node &left, Node &right) -> Cell
    {
        CALICO_EXPECT_TRUE(left.header.is_external);
        CALICO_EXPECT_TRUE(right.header.is_external);
        CALICO_EXPECT_TRUE(left.overflow.has_value());
        auto overflow = *std::exchange(left.overflow, std::nullopt);
        const auto overflow_idx = left.overflow_index;

        // Warning: We don't have access to the former right sibling of left, but we need to set its left child ID.
        //          We need to make sure to do that in the caller.
        right.header.next_id = left.header.next_id;
        right.header.prev_id = left.page.id();
        right.header.parent_id = left.header.parent_id;
        left.header.next_id = right.page.id();

        if (overflow_idx == left.header.cell_count) {
            // Just transfer a single cell in this case. This should reduce the number of splits during a sequential write.
            transfer_cells_right_while(left, right, [](const auto &, const auto &, auto counter) {
                return !counter;
            });
            write_cell(right, right.header.cell_count, overflow);
            CALICO_EXPECT_FALSE(right.overflow.has_value());
        } else if (overflow_idx == 0) {
            // We need the `!counter` because the condition following it may not be true if we got here from split_root().
            transfer_cells_right_while(left, right, [](const auto &src, const auto &dst, auto counter) {
                return !counter || usable_space(src) < usable_space(dst);
            });
            write_cell(left, 0, overflow);
            CALICO_EXPECT_FALSE(left.overflow.has_value());
        } else {
            // We need to insert the overflow cell into either left or right, no matter what, even if it ends up being the separator.
            transfer_cells_right_while(left, right, [&overflow, overflow_idx](const auto &src, const auto &, auto counter) {
                const auto goes_in_src = src.header.cell_count > overflow_idx;
                const auto has_no_room = usable_space(src) < overflow.size + sizeof(PageSize);
                return !counter || (goes_in_src && has_no_room);
            });

            if (left.header.cell_count > overflow_idx) {
                write_cell(left, overflow_idx, overflow);
                CALICO_EXPECT_FALSE(left.overflow.has_value());
            } else {
                write_cell(right, 0, overflow);
                CALICO_EXPECT_FALSE(right.overflow.has_value());
            }
        }

        auto separator = read_cell(right, 0);
        promote_cell(separator);
        return separator;
    }

    [[nodiscard]]
    static auto split_internal_non_root(BPlusTree &tree, Node &left, Node &right) -> Cell
    {
        CALICO_EXPECT_FALSE(left.header.is_external);
        CALICO_EXPECT_FALSE(right.header.is_external);
        CALICO_EXPECT_TRUE(left.overflow.has_value());
        auto overflow = *std::exchange(left.overflow, std::nullopt);
        const auto overflow_idx = left.overflow_index;

        // In internal nodes, the next_id field refers to the rightmost child ID, and the prev_id field is unused.
        right.header.next_id = left.header.next_id;
        right.header.parent_id = left.header.parent_id;

        if (overflow_idx == left.header.cell_count) {
            transfer_cells_right_while(left, right, [](const auto &, const auto &, auto counter) {
                return !counter;
            });
            write_cell(right, right.header.cell_count, overflow);
            CALICO_EXPECT_FALSE(right.overflow.has_value());
        } else if (overflow_idx == 0) {
            transfer_cells_right_while(left, right, [](const auto &src, const auto &dst, Size counter) {
                return !counter || usable_space(src) < usable_space(dst);
            });
            write_cell(left, 0, overflow);
            CALICO_EXPECT_FALSE(left.overflow.has_value());
        } else {
            left.header.next_id = read_child_id(overflow);
            transfer_cells_right_while(left, right, [overflow_idx](const auto &src, const auto &, Size) {
                return src.header.cell_count > overflow_idx;
            });
            return overflow;
        }

        auto separator = read_cell(left, left.header.cell_count - 1);
        detach_cell(separator, scratch_at(tree, 1));
        erase_cell(left, left.header.cell_count - 1, separator.size);
        left.header.next_id = read_child_id(separator);
        return separator;
    }
    
    [[nodiscard]]
    static auto split_non_root(BPlusTree &tree, Node node) -> tl::expected<Node, Status>
    {
        CALICO_EXPECT_FALSE(node.page.id().is_root());
        CALICO_EXPECT_FALSE(node.header.parent_id.is_null());
        CALICO_EXPECT_TRUE(node.overflow.has_value());
        
        CALICO_NEW_R(parent, acquire_node(tree, node.header.parent_id, true));
        CALICO_NEW_R(sibling, allocate_node(tree, node.header.is_external));

        Cell separator;
        if (node.header.is_external) {
            separator = split_external_non_root(node, sibling);
        } else {
            separator = split_internal_non_root(tree, node, sibling);
        }
        if (node.header.is_external && !sibling.header.next_id.is_null()) {
            CALICO_NEW_R(right, acquire_node(tree, sibling.header.next_id, true));
            right.header.prev_id = sibling.page.id();
            release_node(tree, std::move(right));
        }
        Node::Iterator itr {parent};
        const auto exact = itr.seek(read_key(separator));
        CALICO_EXPECT_FALSE(exact);

        write_cell(parent, itr.index(), separator);

        if (parent.overflow.has_value()) {
            // Only detach the cell if it couldn't fit in the parent. In this case, we want to release "node" before
            // we return, so cell cannot be attached to it anymore. The separator should have already been promoted.
            if (!separator.is_free) {
                detach_cell(*parent.overflow, scratch_at(tree, 0));
            }
            CALICO_EXPECT_TRUE(parent.overflow->is_free);
            write_child_id(*parent.overflow, node.page.id());
        } else {
            write_child_id(parent, itr.index(), node.page.id());
        }
    
        CALICO_EXPECT_FALSE(node.overflow.has_value());
        CALICO_EXPECT_FALSE(sibling.overflow.has_value());

        const auto offset = !parent.overflow.has_value();
        write_child_id(parent, itr.index() + offset, sibling.page.id());
        CALICO_TRY_R(maybe_fix_child_parent_links(tree, sibling));
        release_node(tree, std::move(sibling));
        release_node(tree, std::move(node));
        return parent;
    }
    
    static auto resolve_underflow(BPlusTree &tree, Node node) -> tl::expected<void, Status>
    {
        release_node(tree, std::move(node));
        return {};
    }

//    static auto rotate_left(BPlusTree_ &tree, Node_ node) -> void
//    {
//
//    }
//
//    static auto rotate_right(BPlusTree_ &tree, Node_ node) -> void
//    {
//
//    }
//
//    static auto merge_left(BPlusTree_ &tree, Node_ node) -> void
//    {
//
//    }
//
//    static auto merge_right(BPlusTree_ &tree, Node_ node) -> void
//    {
//
//    }
//
//    static auto fix_root(BPlusTree_ &tree, Node_ node) -> void
//    {
//
//    }
//
//    static auto fix_nonroot(BPlusTree_ &tree, Node_ node) -> void
//    {
//
//    }
};

BPlusTree::BPlusTree(Pager &pager)
    : m_free_list {pager},
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

auto BPlusTree::insert(const Slice &key, const Slice &value) -> tl::expected<bool, Status>
{
    CALICO_NEW_R(slot, find(key));
    auto [node, index, exact] = std::move(slot);
    m_pager->upgrade_(node.page);

    if (exact) {
        const auto cell = read_cell(node, index);
        if (cell.local_ps != cell.total_ps) {
            CALICO_EXPECT_LT(cell.local_ps, cell.total_ps);
            const auto overflow_id = read_overflow_id(cell);
            CALICO_TRY_R(erase_chain(*m_pager, m_free_list, overflow_id, cell.total_ps - cell.local_ps));
        }
        erase_cell(node, index, cell.size);
    }

    CALICO_TRY_R(BPlusTreeInternal::emplace_cell(*this, node, index, key, value));
    CALICO_TRY_R(BPlusTreeInternal::resolve_overflow(*this, std::move(node)));
    return !exact;
}

auto BPlusTree::erase(const Slice &key) -> tl::expected<bool, Status>
{
    CALICO_NEW_R(page, m_pager->acquire_(Id::root()));

    auto root = BPlusTreeInternal::make_node(*this, std::move(page));
    CALICO_NEW_R(slot, BPlusTreeInternal::find_external_slot(*this, key, std::move(root)));
    auto [node, index, exact] = std::move(slot);

    if (exact) {
        const auto cell = read_cell(node, index);
        if (const auto remote_size = cell.total_ps - cell.local_ps) {
            CALICO_TRY_R(erase_chain(*m_pager, m_free_list, read_overflow_id(cell), remote_size));
        }
        m_pager->upgrade_(node.page);
        erase_cell(node, index);
        CALICO_TRY_R(BPlusTreeInternal::resolve_underflow(*this, std::move(node)));
    } else {
        BPlusTreeInternal::release_node(*this, std::move(node));
    }
    return exact;
}

auto BPlusTree::find(const Slice &key) -> tl::expected<FindResult, Status>
{
    CALICO_NEW_R(root, BPlusTreeInternal::acquire_node(*this, Id::root()));
    return BPlusTreeInternal::find_external_slot(*this, key, std::move(root));
}

auto BPlusTree::save_state(FileHeader &header) const -> void
{
    header.free_list_id = m_free_list.m_head;
}

auto BPlusTree::load_state(const FileHeader &header) -> void
{
    m_free_list.m_head = header.free_list_id;
}

using Callback = std::function<void(Node &, Size)>;

static auto traverse_inorder_helper(BPlusTree &tree, Node node, const Callback &callback) -> void
{
    for (Size index {}; index <= node.header.cell_count; ++index) {
        if (!node.header.is_external) {
            auto next = BPlusTreeInternal::acquire_node(tree, read_child_id(node, index), false);
            CALICO_EXPECT_TRUE(next.has_value());
            traverse_inorder_helper(tree, std::move(*next), callback);
        }
        if (index < node.header.cell_count)
            callback(node, index);
    }
    BPlusTreeInternal::release_node(tree, std::move(node));
}

static auto traverse_inorder(BPlusTree &tree, const Callback &callback) -> void
{
    auto root = BPlusTreeInternal::acquire_node(tree, Id::root(), false);
    CALICO_EXPECT_TRUE(root.has_value());
    traverse_inorder_helper(tree, std::move(*root), callback);
}

static auto validate_siblings(BPlusTree &tree) -> void
{
    // Find the leftmost external node.
    auto node = *BPlusTreeInternal::acquire_node(tree, Id::root(), false);
    while (!node.header.is_external) {
        const auto id = read_child_id(node, 0);
        BPlusTreeInternal::release_node(tree, std::move(node));
        auto temp = BPlusTreeInternal::acquire_node(tree, id, false);
        CALICO_EXPECT_TRUE(temp.has_value());
        node = std::move(*temp);
    }
    // Traverse across the sibling chain to the right.
    while (!node.header.next_id.is_null()) {
        auto right = BPlusTreeInternal::acquire_node(tree, node.header.next_id, false);
        CALICO_EXPECT_TRUE(right.has_value());
        CALICO_EXPECT_LT(node.read_key(node.get_slot(0)), right->read_key(right->get_slot(0)));
        CALICO_EXPECT_EQ(right->header.prev_id, node.page.id());
        BPlusTreeInternal::release_node(tree, std::move(node));
        node = std::move(*right);
    }
    BPlusTreeInternal::release_node(tree, std::move(node));
}

auto validate_parent_child(BPlusTree &tree) -> void
{
    auto check = [&tree](auto &node, auto index) -> void {
        auto child = *BPlusTreeInternal::acquire_node(tree, read_child_id(node, index), false);
        CALICO_EXPECT_EQ(child.header.parent_id, node.page.id());
        BPlusTreeInternal::release_node(tree, std::move(child));
    };
    traverse_inorder(tree, [f = std::move(check)](Node &node, Size index) -> void {
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

struct PrintData {
    std::vector<std::string> levels;
    std::vector<Size> spaces;
};

static auto add_to_level(PrintData &data, const std::string &message, Size target) -> void
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

static auto ensure_level_exists(PrintData &data, Size level) -> void
{
    while (level >= data.levels.size()) {
        data.levels.emplace_back();
        data.spaces.emplace_back();
    }
    CALICO_EXPECT_GT(data.levels.size(), level);
    CALICO_EXPECT_EQ(data.levels.size(), data.spaces.size());
}

static auto collect_levels(BPlusTree &tree, PrintData &data, Node node, Size level) -> void
{
    const auto &header = node.header;
    ensure_level_exists(data, level);
    for (Size cid {}; cid < header.cell_count; ++cid) {
        const auto is_first = cid == 0;
        const auto not_last = cid + 1 < header.cell_count;
        auto cell = node.parse_cell(node.get_slot(cid));

        if (!header.is_external)
            collect_levels(tree, data, *BPlusTreeInternal::acquire_node(tree, read_child_id(cell), false), level + 1);

        if (is_first)
            add_to_level(data, std::to_string(node.page.id().value) + ":[", level);

        const auto key = Slice {cell.key, cell.key_size}.to_string();
        add_to_level(data, key, level);

        if (not_last) {
            add_to_level(data, ",", level);
        } else {
            add_to_level(data, "]", level);
        }
    }
    if (!node.header.is_external)
        collect_levels(tree, data, *BPlusTreeInternal::acquire_node(tree, node.header.next_id, false), level + 1);

    BPlusTreeInternal::release_node(tree, std::move(node));
}

auto BPlusTree::TEST_to_string() -> std::string
{
    std::string repr;
    PrintData data;

    auto root = BPlusTreeInternal::acquire_node(*this, Id::root());
    collect_levels(*this, data, std::move(*root), 0);
    for (const auto &level: data.levels) {
        repr.append(level + '\n');
    }

    return repr;
}

auto BPlusTree::TEST_check_order() -> void
{
    // NOTE: All keys must fit in main memory (separators included). Doesn't read values.
    std::vector<std::string> keys;
    traverse_inorder(*this, [&keys](const auto &node, auto index) -> void {
        keys.emplace_back(node.read_key(node.get_slot(index)).to_string());
    });
    CALICO_EXPECT_TRUE(std::is_sorted(cbegin(keys), cend(keys)));
}

auto BPlusTree::TEST_check_links() -> void
{
    validate_siblings(*this);
    validate_parent_child(*this);
}

auto BPlusTree::TEST_check_nodes() -> void
{
    traverse_inorder(*this, [](const auto &node, auto index) -> void {
        // Only validate once per node.
        if (index == 0) {
            node.TEST_validate();
        }
    });
}


} // namespace Calico
// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "tree.h"
#include "encoding.h"
#include "logging.h"
#include "pager.h"
#include "schema.h"
#include "utils.h"
#include <functional>

namespace calicodb
{

static constexpr U32 kCellPtrSize = sizeof(U16);

auto Tree::corrupted_page(Id page_id) const -> Status
{
    std::string msg;
    append_fmt_string(msg, "corruption detected (root=%u, page=%u)", root().value, page_id.value);
    auto s = Status::corruption(msg);
    m_pager->set_status(s);
    return s;
}

[[nodiscard]] static auto cell_slots_offset(const Node &node) -> U32
{
    return page_offset(node.ref->page_id) + NodeHdr::kSize;
}

[[nodiscard]] static auto cell_area_offset(const Node &node) -> U32
{
    return cell_slots_offset(node) + NodeHdr::get_cell_count(node.hdr()) * kCellPtrSize;
}

[[nodiscard]] static auto read_next_id(const PageRef &page) -> Id
{
    return Id(get_u32(page.page + page_offset(page.page_id)));
}

static auto write_next_id(PageRef &page, Id next_id) -> void
{
    put_u32(page.page + page_offset(page.page_id), next_id.value);
}

[[nodiscard]] static auto read_child_id(const Cell &cell)
{
    return Id(get_u32(cell.ptr));
}

[[nodiscard]] static auto read_overflow_id(const Cell &cell)
{
    return Id(get_u32(cell.key + cell.local_pl_size));
}

static auto write_overflow_id(Cell &cell, Id overflow_id)
{
    put_u32(cell.key + cell.local_pl_size, overflow_id.value);
}

static auto write_child_id(Cell &cell, Id child_id)
{
    put_u32(cell.ptr, child_id.value);
}

[[nodiscard]] static auto merge_root(Node &root, Node &child, char *scratch)
{
    CALICODB_EXPECT_EQ(NodeHdr::get_next_id(root.hdr()), child.ref->page_id);
    if (NodeHdr::get_free_start(child.hdr()) > 0) {
        if (child.defrag(scratch)) {
            return -1;
        }
    }

    // Copy the cell content area.
    const auto cell_start = NodeHdr::get_cell_start(child.hdr());
    CALICODB_EXPECT_GE(cell_start, cell_slots_offset(root));
    auto area_size = kPageSize - cell_start;
    auto *area = root.ref->page + cell_start;
    std::memcpy(area, child.ref->page + cell_start, area_size);

    // Copy the header and cell pointers.
    area_size = NodeHdr::get_cell_count(child.hdr()) * kCellPtrSize;
    area = root.ref->page + cell_slots_offset(root);
    std::memcpy(area, child.ref->page + cell_slots_offset(child), area_size);
    std::memcpy(root.hdr(), child.hdr(), NodeHdr::kSize);
    root.parser = child.parser;
    return 0;
}

[[nodiscard]] static auto is_underflowing(const Node &node)
{
    return NodeHdr::get_cell_count(node.hdr()) == 0;
}

static constexpr U32 kLinkContentOffset = sizeof(U32);
static constexpr U32 kLinkContentSize = kPageSize - kLinkContentOffset;

struct PayloadManager {
    static auto promote(Pager &pager, Cell &cell, Id parent_id) -> Status
    {
        const auto header_size = sizeof(U32) + varint_length(cell.key_size);

        // The buffer that `scratch` points into should have enough room before `scratch` to write
        // the left child ID.
        cell.ptr = cell.key - header_size;
        cell.local_pl_size = compute_local_pl_size(cell.key_size, 0);
        cell.total_pl_size = cell.key_size;
        cell.footprint = static_cast<U32>(header_size + cell.local_pl_size);

        Status s;
        if (cell.key_size > cell.local_pl_size) {
            // Part of the key is on an overflow page. No value is stored locally in this case, so
            // the local size computation is still correct. Copy the overflow key, page-by-page,
            // to a new overflow chain.
            Id ovfl_id;
            auto rest = cell.key_size - cell.local_pl_size;
            auto pgno = read_overflow_id(cell);
            PageRef *prev = nullptr;
            auto dst_type = PointerMap::kOverflowHead;
            auto dst_bptr = parent_id;
            while (s.is_ok() && rest > 0) {
                PageRef *src, *dst;
                // Allocate a new overflow page.
                s = pager.allocate(dst);
                if (!s.is_ok()) {
                    break;
                }
                // Acquire the old overflow page.
                s = pager.acquire(pgno, src);
                if (!s.is_ok()) {
                    pager.release(dst);
                    break;
                }
                const auto copy_size = std::min(rest, kLinkContentSize);
                std::memcpy(dst->page + kLinkContentOffset,
                            src->page + kLinkContentOffset,
                            copy_size);

                if (prev) {
                    put_u32(prev->page, dst->page_id.value);
                    pager.release(prev, Pager::kNoCache);
                } else {
                    write_overflow_id(cell, dst->page_id);
                }
                rest -= copy_size;
                dst_type = PointerMap::kOverflowLink;
                dst_bptr = dst->page_id;
                prev = dst;
                pgno = read_next_id(*src);
                pager.release(src, Pager::kNoCache);

                s = PointerMap::write_entry(
                    pager, dst->page_id, {dst_bptr, dst_type});
            }
            if (s.is_ok()) {
                CALICODB_EXPECT_NE(nullptr, prev);
                put_u32(prev->page, 0);
                cell.footprint += sizeof(U32);
            }
            pager.release(prev, Pager::kNoCache);
        }
        return s;
    }

    static auto access(
        Pager &pager,
        const Cell &cell,   // The `cell` containing the payload being accessed
        std::size_t offset, // `offset` within the payload being accessed
        std::size_t length, // Number of bytes to access
        const char *in_buf, // Write buffer of size at least `length` bytes, or nullptr if not a write
        char *out_buf       // Read buffer of size at least `length` bytes, or nullptr if not a read
        ) -> Status
    {
        CALICODB_EXPECT_TRUE(in_buf || out_buf);
        if (offset <= cell.local_pl_size) {
            const auto n = std::min(length, cell.local_pl_size - offset);
            if (in_buf) {
                std::memcpy(cell.key + offset, in_buf, n);
                in_buf += n;
            } else {
                std::memcpy(out_buf, cell.key + offset, n);
                out_buf += n;
            }
            length -= n;
            offset = 0;
        } else {
            offset -= cell.local_pl_size;
        }

        Status s;
        if (length) {
            auto pgno = read_overflow_id(cell);
            while (!pgno.is_null()) {
                PageRef *ovfl;
                s = pager.acquire(pgno, ovfl);
                if (!s.is_ok()) {
                    break;
                }
                std::size_t len;
                if (offset >= kLinkContentSize) {
                    offset -= kLinkContentSize;
                    len = 0;
                } else {
                    len = std::min(length, kLinkContentSize - offset);
                    if (in_buf) {
                        std::memcpy(ovfl->page + kLinkContentOffset + offset, in_buf, len);
                        in_buf += len;
                    } else {
                        std::memcpy(out_buf, ovfl->page + kLinkContentOffset + offset, len);
                        out_buf += len;
                    }
                    offset = 0;
                }
                pgno = read_next_id(*ovfl);
                pager.release(ovfl, Pager::kNoCache);
                length -= len;
                if (length == 0) {
                    break;
                }
            }
        }
        return s;
    }
};

auto Tree::create(Pager &pager, Id *root_id_out) -> Status
{
    PageRef *page;
    auto s = pager.allocate(page);
    if (s.is_ok()) {
        auto *hdr = page->page + page_offset(page->page_id);
        std::memset(hdr, 0, NodeHdr::kSize);
        NodeHdr::put_type(hdr, NodeHdr::kExternal);
        NodeHdr::put_cell_start(hdr, kPageSize);

        s = PointerMap::write_entry(
            pager,
            page->page_id,
            {Id::null(), PointerMap::kTreeRoot});
        if (root_id_out) {
            *root_id_out = s.is_ok() ? page->page_id : Id::null();
        }
        pager.release(page);
    }
    return s;
}

auto Tree::find_external(const Slice &key, bool &exact) const -> Status
{
    m_c.seek_root();
    exact = false;

    while (m_c.is_valid()) {
        const auto found = m_c.seek(key);
        if (m_c.is_valid()) {
            if (m_c.node().is_leaf()) {
                exact = found;
                break;
            }
            const auto next_id = m_c.node().read_child_id(m_c.index());
            CALICODB_EXPECT_NE(next_id, m_c.node().ref->page_id); // Infinite loop.
            m_c.move_down(next_id);
        }
    }
    return m_c.status();
}

auto Tree::read_key(Node &node, std::size_t index, std::string &scratch, Slice *key_out, std::size_t limit) const -> Status
{
    Cell cell;
    if (node.read(index, cell)) {
        return corrupted_page(node.ref->page_id);
    }
    return read_key(cell, scratch, key_out, limit);
}
auto Tree::read_key(const Cell &cell, std::string &scratch, Slice *key_out, std::size_t limit) const -> Status
{
    if (limit == 0 || limit > cell.key_size) {
        limit = cell.key_size;
    }
    if (scratch.size() < limit) {
        scratch.resize(limit);
    }
    auto s = PayloadManager::access(*m_pager, cell, 0, limit, nullptr, scratch.data());
    if (s.is_ok() && key_out) {
        *key_out = Slice(scratch).truncate(limit);
    }
    return s;
}

auto Tree::read_value(Node &node, std::size_t index, std::string &scratch, Slice *value_out) const -> Status
{
    Cell cell;
    if (node.read(index, cell)) {
        return corrupted_page(node.ref->page_id);
    }
    return read_value(cell, scratch, value_out);
}
auto Tree::read_value(const Cell &cell, std::string &scratch, Slice *value_out) const -> Status
{
    const auto value_size = cell.total_pl_size - cell.key_size;
    if (scratch.size() < value_size) {
        scratch.resize(value_size);
    }
    auto s = PayloadManager::access(*m_pager, cell, cell.key_size, value_size, nullptr, scratch.data());
    if (s.is_ok() && value_out) {
        *value_out = Slice(scratch).truncate(value_size);
    }
    return s;
}

auto Tree::write_key(Node &node, std::size_t index, const Slice &key) -> Status
{
    Cell cell;
    if (node.read(index, cell)) {
        return corrupted_page(node.ref->page_id);
    }
    return PayloadManager::access(*m_pager, cell, 0, key.size(), key.data(), nullptr);
}

auto Tree::write_value(Node &node, std::size_t index, const Slice &value) -> Status
{
    Cell cell;
    if (node.read(index, cell)) {
        return corrupted_page(node.ref->page_id);
    }
    return PayloadManager::access(*m_pager, cell, cell.key_size, value.size(), value.data(), nullptr);
}

auto Tree::find_parent_id(Id page_id, Id &out) const -> Status
{
    PointerMap::Entry entry;
    auto s = PointerMap::read_entry(*m_pager, page_id, entry);
    if (s.is_ok()) {
        out = entry.back_ptr;
    } else {
        out = Id::null();
    }
    return s;
}

auto Tree::fix_parent_id(Id page_id, Id parent_id, PointerMap::Type type) -> Status
{
    PointerMap::Entry entry = {parent_id, type};
    return PointerMap::write_entry(*m_pager, page_id, entry);
}

auto Tree::maybe_fix_overflow_chain(const Cell &cell, Id parent_id) -> Status
{
    if (cell.local_pl_size != cell.total_pl_size) {
        return fix_parent_id(read_overflow_id(cell), parent_id, PointerMap::kOverflowHead);
    }
    return Status::ok();
}

auto Tree::insert_cell(Node &node, std::size_t index, const Cell &cell) -> Status
{
    const auto rc = node.write(index, cell, m_node_scratch);
    if (rc < 0) {
        return corrupted_page(node.ref->page_id);
    } else if (rc == 0) {
        CALICODB_EXPECT_FALSE(m_ovfl.exists());
        // NOTE: The overflow cell may need to be detached, if the node it is backed by will be released
        //       before it can be written to another node (without that node itself overflowing).
        m_ovfl = {cell, node.ref->page_id, static_cast<U32>(index)};
    }
    if (!node.is_leaf()) {
        auto s = fix_parent_id(
            read_child_id(cell),
            node.ref->page_id,
            PointerMap::kTreeNode);
        if (!s.is_ok()) {
            return s;
        }
    }
    return maybe_fix_overflow_chain(cell, node.ref->page_id);
}

auto Tree::remove_cell(Node &node, std::size_t index) -> Status
{
    Cell cell;
    if (node.read(index, cell)) {
        return corrupted_page(node.ref->page_id);
    }
    Status s;
    if (cell.local_pl_size != cell.total_pl_size) {
        s = free_overflow(read_overflow_id(cell));
    }
    if (s.is_ok() && node.erase(index, cell.footprint)) {
        s = corrupted_page(node.ref->page_id);
    }
    return s;
}

auto Tree::free_overflow(Id head_id) -> Status
{
    Status s;
    while (s.is_ok() && !head_id.is_null()) {
        PageRef *page;
        s = m_pager->acquire(head_id, page);
        if (s.is_ok()) {
            head_id = read_next_id(*page);
            s = m_pager->destroy(page);
        }
    }
    return s;
}

// It is assumed that the children of `node` have incorrect parent pointers. This routine fixes
// these parent pointers using the pointer map. Using a pointer map is vital here: it allows us
// to access way fewer pages when updating the parent pointers (usually just a few as opposed to
// the number of children in `node` which can be very large).
auto Tree::fix_links(Node &node, Id parent_id) -> Status
{
    if (parent_id.is_null()) {
        parent_id = node.ref->page_id;
    }
    for (std::size_t i = 0, n = NodeHdr::get_cell_count(node.hdr()); i < n; ++i) {
        Cell cell;
        if (node.read(i, cell)) {
            return corrupted_page(node.ref->page_id);
        }
        // Fix the back pointer for the head of an overflow chain rooted at `node`.
        auto s = maybe_fix_overflow_chain(cell, parent_id);
        if (s.is_ok() && !node.is_leaf()) {
            // Fix the parent pointer for the current child node.
            s = fix_parent_id(
                read_child_id(cell),
                parent_id,
                PointerMap::kTreeNode);
        }
        if (!s.is_ok()) {
            return s;
        }
    }
    Status s;
    if (!node.is_leaf()) {
        s = fix_parent_id(
            NodeHdr::get_next_id(node.hdr()),
            parent_id,
            PointerMap::kTreeNode);
    }
    if (s.is_ok() && m_ovfl.exists()) {
        s = maybe_fix_overflow_chain(m_ovfl.cell, parent_id);
        if (s.is_ok() && !node.is_leaf()) {
            s = fix_parent_id(
                read_child_id(m_ovfl.cell),
                parent_id,
                PointerMap::kTreeNode);
        }
    }
    return s;
}

auto Tree::resolve_overflow() -> Status
{
    CALICODB_EXPECT_TRUE(m_c.is_valid());

    Status s;
    while (s.is_ok() && m_ovfl.exists()) {
        if (m_c.node().ref->page_id == root()) {
            s = split_root();
        } else {
            s = split_nonroot();
        }
        ++m_stats.stats[kStatSMOCount];
    }
    m_c.clear();
    return s;
}

auto Tree::split_root() -> Status
{
    CALICODB_EXPECT_TRUE(m_c.is_valid());
    CALICODB_EXPECT_EQ(m_c.level, 0);
    auto &root = m_c.node();
    CALICODB_EXPECT_EQ(Tree::root(), root.ref->page_id);

    Node child;
    auto s = allocate(root.is_leaf(), child);
    if (s.is_ok()) {
        // Copy the cell content area.
        const auto after_root_headers = cell_area_offset(root);
        std::memcpy(child.ref->page + after_root_headers,
                    root.ref->page + after_root_headers,
                    kPageSize - after_root_headers);

        // Copy the header and cell pointers.
        std::memcpy(child.hdr(), root.hdr(), NodeHdr::kSize);
        std::memcpy(child.ref->page + cell_slots_offset(child),
                    root.ref->page + cell_slots_offset(root),
                    NodeHdr::get_cell_count(root.hdr()) * kCellPtrSize);

        CALICODB_EXPECT_TRUE(m_ovfl.exists());
        child.gap_size = root.gap_size;
        child.usable_space = root.usable_space;
        if (root.ref->page_id.is_root()) {
            child.gap_size += FileHdr::kSize;
            child.usable_space += FileHdr::kSize;
        }

        root = Node::from_new_page(*root.ref, false);
        NodeHdr::put_next_id(root.hdr(), child.ref->page_id);

        s = fix_parent_id(
            child.ref->page_id,
            root.ref->page_id,
            PointerMap::kTreeNode);
        if (s.is_ok()) {
            s = fix_links(child);
        }

        // Overflow cell is now in the child. m_ovfl.idx stays the same.
        m_ovfl.pid = child.ref->page_id;
        advance_cursor(std::move(child), 1);
        m_c.history[1].index = m_c.history[0].index;
        m_c.history[0].index = 0;
    }
    return s;
}

auto Tree::split_nonroot() -> Status
{
    auto &node = m_c.node();
    CALICODB_EXPECT_TRUE(m_ovfl.exists());
    CALICODB_EXPECT_GT(m_c.level, 0);

    Node parent, left;
    const auto pivot = m_c.history[m_c.level - 1];
    auto s = acquire(pivot.page_id, true, parent);
    if (s.is_ok()) {
        s = allocate(node.is_leaf(), left);
    }
    if (s.is_ok()) {
        if (m_ovfl.idx == NodeHdr::get_cell_count(node.hdr())) {
            // Note the reversal of `left` and `right`. We are splitting the other way. This can greatly improve
            // the performance of sequential writes, since the other routine leaves the cell distribution biased
            // toward the right.
            return split_nonroot_fast(
                std::move(parent),
                std::move(left));
        }
        s = redistribute_cells(left, node, parent, pivot.index);
    }

    if (s.is_ok() && node.is_leaf()) {
        // Add the new node to the leaf sibling chain.
        const auto prev_id = NodeHdr::get_prev_id(node.hdr());
        if (!prev_id.is_null()) {
            Node left_sibling;
            s = acquire(prev_id, true, left_sibling);
            if (s.is_ok()) {
                NodeHdr::put_next_id(left_sibling.hdr(), left.ref->page_id);
                NodeHdr::put_prev_id(left.hdr(), left_sibling.ref->page_id);
                release(std::move(left_sibling));
            }
        }
        NodeHdr::put_prev_id(node.hdr(), left.ref->page_id);
        NodeHdr::put_next_id(left.hdr(), node.ref->page_id);
    }

    advance_cursor(std::move(parent), -1);
    release(std::move(left));
    return s;
}

auto Tree::split_nonroot_fast(Node parent, Node right) -> Status
{
    auto &left = m_c.node();
    InternalCursor::Location last_loc;

    CALICODB_EXPECT_TRUE(m_ovfl.exists());
    const auto ovfl = m_ovfl.cell;
    m_ovfl.clear();

    auto s = insert_cell(right, 0, ovfl);
    CALICODB_EXPECT_FALSE(m_ovfl.exists());

    Cell pivot;
    if (left.is_leaf()) {
        const auto next_id = NodeHdr::get_next_id(left.hdr());
        if (!next_id.is_null()) {
            Node right_sibling;
            s = acquire(next_id, true, right_sibling);
            if (!s.is_ok()) {
                goto cleanup;
            }
            NodeHdr::put_prev_id(right_sibling.hdr(), right.ref->page_id);
            NodeHdr::put_next_id(right.hdr(), right_sibling.ref->page_id);
            release(std::move(right_sibling));
        }
        NodeHdr::put_prev_id(right.hdr(), left.ref->page_id);
        NodeHdr::put_next_id(left.hdr(), right.ref->page_id);

        if (right.read(0, pivot)) {
            s = corrupted_page(right.ref->page_id);
            goto cleanup;
        }
        // NOTE: The overflow cell has already been written to `right`, so cell_scratch(0) is available.
        detach_cell(pivot, cell_scratch());
        s = PayloadManager::promote(
            *m_pager,
            pivot,
            parent.ref->page_id);
    } else {
        auto cell_count = NodeHdr::get_cell_count(left.hdr());
        if (left.read(cell_count - 1, pivot)) {
            s = corrupted_page(left.ref->page_id);
            goto cleanup;
        }
        detach_cell(pivot, cell_scratch());
        left.erase(cell_count - 1, pivot.footprint);

        NodeHdr::put_next_id(right.hdr(), NodeHdr::get_next_id(left.hdr()));
        NodeHdr::put_next_id(left.hdr(), read_child_id(pivot));
        s = fix_parent_id(
            NodeHdr::get_next_id(right.hdr()),
            right.ref->page_id,
            PointerMap::kTreeNode);
        if (s.is_ok()) {
            s = fix_parent_id(
                NodeHdr::get_next_id(left.hdr()),
                left.ref->page_id,
                PointerMap::kTreeNode);
        }
    }
    if (s.is_ok()) {
        CALICODB_EXPECT_GT(m_c.level, 0);
        last_loc = m_c.history[m_c.level - 1];

        // Post the pivot into the parent node. This call will fix the sibling's parent pointer.
        write_child_id(pivot, left.ref->page_id);
        s = insert_cell(parent, last_loc.index, pivot);
        if (s.is_ok()) {
            parent.write_child_id(
                last_loc.index + !m_ovfl.exists(),
                right.ref->page_id);
            s = fix_parent_id(
                right.ref->page_id,
                parent.ref->page_id,
                PointerMap::kTreeNode);
        }
    }

cleanup:
    advance_cursor(std::move(parent), -1);
    release(std::move(right));
    return s;
}

auto Tree::resolve_underflow() -> Status
{
    Status s;
    while (m_c.is_valid() && is_underflowing(m_c.node())) {
        if (root() == m_c.node().ref->page_id) {
            return fix_root();
        }
        CALICODB_EXPECT_GT(m_c.level, 0);

        Node parent;
        const auto [parent_id, idx] = m_c.history[m_c.level - 1];
        s = acquire(parent_id, true, parent);
        if (s.is_ok()) {
            s = fix_nonroot(std::move(parent), idx);
        }
        if (!s.is_ok()) {
            break;
        }
        ++m_stats.stats[kStatSMOCount];
    }
    return s;
}

// This routine redistributes cells between two siblings, `left` and `right`, and their `parent`
// One of the two siblings must be empty. This code handles rebalancing after both put and erase
// operations. When called from put(), there will be an overflow cell in m_ovfl.cell which needs
// to be put in either `left` or `right`, depending on the final distribution. When called from
// erase(), the `left` node may be left totally empty, in which case, it should be freed. If
// `left` is empty, then `parent` will not have a pivot cell for it.
auto Tree::redistribute_cells(Node &left, Node &right, Node &parent, U32 pivot_idx) -> Status
{
    CALICODB_EXPECT_GT(m_c.level, 0);

    Node tmp, *p_src, *p_left, *p_right;
    if (0 < NodeHdr::get_cell_count(left.hdr())) {
        CALICODB_EXPECT_EQ(0, NodeHdr::get_cell_count(right.hdr()));
        p_src = &left;
        p_left = &tmp;
        p_right = &right;
    } else {
        CALICODB_EXPECT_LT(0, NodeHdr::get_cell_count(right.hdr()));
        p_src = &right;
        p_left = &left;
        p_right = &tmp;
    }
    // Create a dummy page reference that uses the node defragmentation scratch as its backing buffer.
    // This is where the new copy of the nonempty sibling node will be built.
    auto ref = *p_src->ref;
    ref.page = m_node_scratch;
    tmp = Node::from_new_page(ref, p_src->is_leaf());
    NodeHdr::put_prev_id(tmp.hdr(), NodeHdr::get_prev_id(p_src->hdr()));
    NodeHdr::put_next_id(tmp.hdr(), NodeHdr::get_next_id(p_src->hdr()));

    // Cells that need to be redistributed, in order.
    std::vector<Cell> cells;
    Cell cell;

    const auto is_split = m_ovfl.exists();
    const auto cell_count = NodeHdr::get_cell_count(p_src->hdr());
    // split_nonroot_fast() handles this case. If the overflow is on the rightmost position, this code path must never be
    // hit, since it doesn't handle that case in particular.
    CALICODB_EXPECT_TRUE(!is_split || m_c.index() < cell_count);

    U32 right_accum = 0;
    cells.reserve(cell_count + 1);
    for (U32 i = 0; i < cell_count;) {
        if (m_ovfl.exists() && i == m_c.index()) {
            right_accum += m_ovfl.cell.footprint;
            cells.emplace_back(m_ovfl.cell);
            m_ovfl.clear();
            continue;
        }
        if (p_src->read(i++, cell)) {
            return corrupted_page(p_src->ref->page_id);
        }
        cells.emplace_back(cell);
        right_accum += cell.footprint;
    }
    CALICODB_EXPECT_FALSE(m_ovfl.exists());
    if (!is_split) {
        if (parent.read(pivot_idx, cell)) {
            return corrupted_page(parent.ref->page_id);
        }
        if (!p_src->is_leaf()) {
            detach_cell(cell, cell_scratch(1));
            // cell is from the `parent`, so it already has room for a left child ID.
            write_child_id(cell, NodeHdr::get_next_id(p_left->hdr()));
            right_accum += cell.footprint;
            if (p_src == &left) {
                cells.emplace_back(cell);
            } else {
                cells.insert(begin(cells), cell);
            }
        }
        parent.erase(pivot_idx, cell.footprint);
    }
    CALICODB_EXPECT_GE(cells.size(), is_split ? 4 : 1);

    auto sep = -1;
    U32 left_accum = 0;
    while (right_accum > p_left->usable_space / 2 &&
           right_accum > left_accum &&
           2 + sep++ < static_cast<int>(cells.size())) {
        left_accum += cells[sep].footprint;
        right_accum -= cells[sep].footprint;
    }
    if (sep == 0) {
        sep = 1;
    }

    Status s;
    auto idx = static_cast<int>(cells.size()) - 1;
    for (; idx > sep; --idx) {
        s = insert_cell(*p_right, 0, cells[idx]);
        CALICODB_EXPECT_FALSE(m_ovfl.exists());
        if (!s.is_ok()) {
            return s;
        }
    }

    CALICODB_EXPECT_TRUE(idx > 0 || idx == -1);
char sc[kPageSize];//TODO
    // Post a pivot to the `parent` which links to p_left. If this connection existed before, we would have erased it
    // when parsing cells earlier.
    if (idx > 0) {
        if (p_src->is_leaf()) {
            ++idx; // Backtrack to the last cell written to p_right.
            detach_cell(cells[idx], cell_scratch(1));
            s = PayloadManager::promote(*m_pager, cells[idx], parent.ref->page_id);

        } else {
            const auto next_id = read_child_id(cells[idx]);
            NodeHdr::put_next_id(p_left->hdr(), next_id);
            s = fix_parent_id(next_id, p_left->ref->page_id, PointerMap::kTreeNode);
            detach_cell(cells[idx], sc); // TODO: Don't mess up one of the sibling cells when writing the child ID below.
        }
        if (s.is_ok()) {
            // Post the pivot. This may cause the `parent` to overflow.
            write_child_id(cells[idx], p_left->ref->page_id);
            s = insert_cell(parent, pivot_idx, cells[idx]);
            if (m_ovfl.exists()) {
                detach_cell(m_ovfl.cell, cell_scratch(2));
            }
            --idx;
        }
    } else if (p_src->is_leaf()) {
        // p_left must be freed by the caller. Go ahead and fix the sibling chain here.
        const auto prev_id = NodeHdr::get_prev_id(p_left->hdr());
        NodeHdr::put_prev_id(p_right->hdr(), prev_id);
        if (!prev_id.is_null()) {
            Node left_sibling;
            s = acquire(prev_id, true, left_sibling);
            if (s.is_ok()) {
                NodeHdr::put_next_id(left_sibling.hdr(), p_right->ref->page_id);
                release(std::move(left_sibling));
            }
        }
    }
    if (!s.is_ok()) {
        return s;
    }

    // Write the rest of the cells to p_left.
    for (; idx >= 0; --idx) {
        s = insert_cell(*p_left, 0, cells[idx]);
        if (!s.is_ok()) {
            return s;
        }
    }

    // Copy the newly-built node back to the initial nonempty sibling.
    std::memcpy(p_src->ref->page, tmp.ref->page, kPageSize);
    auto *saved_ref = p_src->ref;
    *p_src = std::move(tmp);
    p_src->ref = saved_ref;

    // Only the parent is allowed to overflow. The caller is expected to rebalance the parent in this case.
    CALICODB_EXPECT_TRUE(!m_ovfl.exists() || m_ovfl.pid == parent.ref->page_id);
    if (m_ovfl.exists()) {
        detach_cell(m_ovfl.cell, cell_scratch(0));
    }
    return s;
}

auto Tree::fix_nonroot(Node parent, std::size_t index) -> Status
{
    auto &node = m_c.node();
    CALICODB_EXPECT_NE(node.ref->page_id, root());
    CALICODB_EXPECT_TRUE(is_underflowing(node));
    CALICODB_EXPECT_FALSE(m_ovfl.exists());

    Status s;
    Node sibling, *p_left, *p_right;
    if (index > 0) {
        --index; // Correct the pivot `index` to point to p_left.
        s = acquire(parent.read_child_id(index), true, sibling);
        p_left = &sibling;
        p_right = &node;
    } else {
        s = acquire(parent.read_child_id(index + 1), true, sibling);
        p_left = &node;
        p_right = &sibling;
    }
    if (s.is_ok()) {
        s = redistribute_cells(*p_left, *p_right, parent, index);
        // NOTE: If this block isn't hit, then (a) sibling is not acquired, and (b) node will be
        //       released when the cursor is advanced.
        release(std::move(*p_right));
        if (s.is_ok() && 0 == NodeHdr::get_cell_count(p_left->hdr())) {
            // redistribute_cells() performed a merge.
            s = free(std::move(*p_left));
        } else {
            release(std::move(*p_left));
        }
    }

    advance_cursor(std::move(parent), -1);
    if (s.is_ok() && m_ovfl.exists()) {
        s = resolve_overflow();
    }
    return s;
}

auto Tree::fix_root() -> Status
{
    auto &node = m_c.node();
    CALICODB_EXPECT_EQ(node.ref->page_id, root());
    if (node.is_leaf()) {
        // The whole tree is empty.
        return Status::ok();
    }

    Node child;
    auto s = acquire(NodeHdr::get_next_id(node.hdr()), true, child);
    if (s.is_ok()) {
        // We don't have enough room to transfer the child contents into the root, due to the space occupied by
        // the file header. In this case, we'll just split the child and insert the median cell into the root.
        // Note that the child needs an overflow cell for the split routine to work. We'll just fake it by
        // extracting an arbitrary cell and making it the overflow cell.
        if (node.ref->page_id.is_root() && child.usable_space < FileHdr::kSize) {
            Cell cell;
            m_c.history[m_c.level].index = NodeHdr::get_cell_count(child.hdr()) / 2;
            if (child.read(m_c.index(), cell)) {
                s = corrupted_page(node.ref->page_id);
                release(std::move(child));
            } else {
                m_ovfl.cell = cell;
                detach_cell(m_ovfl.cell, cell_scratch());
                child.erase(m_c.index(), cell.footprint);
                advance_cursor(std::move(child), 0);
                s = split_nonroot();
            }
        } else {
            if (merge_root(node, child, m_node_scratch)) {
                s = corrupted_page(node.ref->page_id);
                release(std::move(child));
            } else {
                s = free(std::move(child));
            }
            if (s.is_ok()) {
                s = fix_links(node);
            }
        }
    }
    return s;
}

Tree::Tree(Pager &pager, char *scratch, const Id *root_id)
    : m_c(*this),
      m_node_scratch(scratch + kPageSize),
      m_cell_scratch{
          scratch,
          scratch + kPageSize * 1 / kNumCellBuffers,
          scratch + kPageSize * 2 / kNumCellBuffers,
      },
      m_pager(&pager),
      m_root_id(root_id)
{
    // Make sure that cells written to scratch memory don't interfere with each other.
    static_assert(kPageSize / kNumCellBuffers > kMaxCellHeaderSize + compute_local_pl_size(kPageSize, 0));
}

auto Tree::detach_cell(Cell &cell, char *backing) -> void
{
    CALICODB_EXPECT_NE(backing, nullptr);
    // NOTE: PayloadManager::promote() may move the cell's ptr back a few bytes to make room for
    //       a left child ID.
    if (cell.ptr < backing || cell.ptr + kCellScratchDiff > backing) {
        std::memcpy(backing, cell.ptr, cell.footprint);
        const auto diff = cell.key - cell.ptr;
        cell.ptr = backing;
        cell.key = backing + diff;
    }
}

auto Tree::cell_scratch(std::size_t n) -> char *
{
    CALICODB_EXPECT_LT(n, kNumCellBuffers);
    // Leave space for a child ID. We need the maximum difference between the size of a varint and
    // an Id. When a cell is promoted (i.e. made into an internal cell, so it can be posted to the
    // parent node) it loses a varint (the value size), but gains an Id (the left child pointer).
    // We should be able to write any external cell to this location, and still have room to write
    // the left child ID before the key size field, regardless of the number of bytes occupied by
    // the varint value size.
    return m_cell_scratch[n] + kCellScratchDiff;
}

auto Tree::get(const Slice &key, std::string *value) const -> Status
{
    bool found;
    auto s = find_external(key, found);
    if (!s.is_ok()) {
        // Do nothing. A low-level I/O error has occurred.
    } else if (!found) {
        s = Status::not_found();
    } else if (value) {
        Slice slice;
        s = read_value(m_c.node(), m_c.index(), *value, &slice);
        value->resize(slice.size());
        if (s.is_ok()) {
            m_stats.stats[kStatRead] += slice.size();
        }
    }
    m_c.clear();
    return s;
}

auto Tree::put(const Slice &key, const Slice &value) -> Status
{
    static constexpr auto kMaxLength = std::numeric_limits<U32>::max();
    if (key.is_empty()) {
        return Status::invalid_argument("key is empty");
    } else if (key.size() > kMaxLength) {
        return Status::invalid_argument("key is too long");
    } else if (value.size() > kMaxLength) {
        return Status::invalid_argument("value is too long");
    }
    bool exact;
    auto s = find_external(key, exact);
    if (s.is_ok()) {
        upgrade(m_c.node());
        if (exact) {
            s = remove_cell(m_c.node(), m_c.index());
        }
        if (s.is_ok()) {
            bool overflow;
            // Attempt to write a cell representing the `key` and `value` directly to the page.
            // This routine also populates any overflow pages necessary to hold a `key` and/or
            // `value` that won't fit on a single node page. If the cell cannot fit in `node`,
            // it will be written to cell_scratch(0) instead.
            s = emplace(m_c.node(), key, value, m_c.index(), overflow);

            if (s.is_ok()) {
                if (overflow) {
                    // There wasn't enough room for the cell in `node`, so it was built in
                    // cell_scratch(0) instead.
                    Cell ovfl;
                    const auto rc = m_c.node().parser(cell_scratch(0), cell_scratch(1), &ovfl);
                    if (rc) {
                        s = corrupted_page(m_c.node().ref->page_id);
                    } else {
                        CALICODB_EXPECT_FALSE(m_ovfl.exists());
                        m_ovfl = { ovfl, m_c.node().ref->page_id, static_cast<U32>(m_c.index())};
                        s = resolve_overflow();
                    }
                }
                m_stats.stats[kStatWrite] += key.size() + value.size();
            }
        }
    }
    m_c.clear();
    return s;
}

auto Tree::emplace(Node &node, const Slice &key, const Slice &value, std::size_t index, bool &overflow) -> Status
{
    CALICODB_EXPECT_TRUE(node.is_leaf());
    auto k = key.size();
    auto v = value.size();
    const auto local_pl_size = compute_local_pl_size(k, v);
    const auto has_remote = k + v > local_pl_size;

    if (k > local_pl_size) {
        k = local_pl_size;
        v = 0;
    } else if (has_remote) {
        v = local_pl_size - k;
    }

    CALICODB_EXPECT_EQ(k + v, local_pl_size);
    // Serialize the cell header for the external cell and determine the number
    // of bytes needed for the cell.
    char header[kVarintMaxLength * 2];
    auto *ptr = header;
    ptr = encode_varint(ptr, static_cast<U32>(value.size()));
    ptr = encode_varint(ptr, static_cast<U32>(key.size()));
    const auto hdr_size = static_cast<std::uintptr_t>(ptr - header);
    const auto cell_size = local_pl_size + hdr_size + sizeof(U32) * has_remote;

    // Attempt to allocate space for the cell in the node. If this is not possible,
    // write the cell to scratch memory. allocate_block() should not return an offset
    // that would interfere with the node header/indirection vector or cause an out-of-
    // bounds write (this only happens if the node is corrupted).
    ptr = cell_scratch();
    const auto local_offset = node.alloc(
        static_cast<U32>(index),
        static_cast<U32>(cell_size),
        m_node_scratch);
    if (local_offset > 0) {
        ptr = node.ref->page + local_offset;
        overflow = false;
    } else if (local_offset == 0) {
        overflow = true;
    } else {
        return corrupted_page(node.ref->page_id);
    }
    // Write the cell header.
    std::memcpy(ptr, header, hdr_size);
    ptr += hdr_size;

    PageRef *prev = nullptr;
    auto payload_left = key.size() + value.size();
    auto prev_pgno = node.ref->page_id;
    auto prev_type = PointerMap::kOverflowHead;
    auto *next_ptr = ptr + local_pl_size;
    auto len = local_pl_size;
    auto src = key;

    Status s;
    while (s.is_ok()) {
        const auto n = std::min(len, static_cast<U32>(src.size()));
        // Copy a chunk of the payload to a page. ptr either points to where the local payload
        // should go in node, or somewhere in prev, which holds the overflow page being written.
        std::memcpy(ptr, src.data(), n);
        src.advance(n);
        payload_left -= n;
        if (payload_left == 0) {
            break;
        }
        ptr += n;
        len -= n;
        if (src.is_empty()) {
            src = value;
        }
        CALICODB_EXPECT_FALSE(src.is_empty());
        if (len == 0) {
            PageRef *ovfl;
            s = m_pager->allocate(ovfl);
            if (s.is_ok()) {
                put_u32(next_ptr, ovfl->page_id.value);
                len = kLinkContentSize;
                ptr = ovfl->page + sizeof(U32);
                next_ptr = ovfl->page;
                if (prev) {
                    m_pager->release(prev, Pager::kNoCache);
                }
                s = PointerMap::write_entry(
                    *m_pager, ovfl->page_id, {prev_pgno, prev_type});
                prev_type = PointerMap::kOverflowLink;
                prev_pgno = ovfl->page_id;
                prev = ovfl;
            }
        }
    }
    if (prev) {
        // prev holds the last page in the overflow chain.
        put_u32(prev->page, 0);
        m_pager->release(prev, Pager::kNoCache);
    }
    return s;
}

auto Tree::erase(const Slice &key) -> Status
{
    bool exact;
    auto s = find_external(key, exact);
    if (s.is_ok() && exact) {
        upgrade(m_c.node());
        s = remove_cell(m_c.node(), m_c.index());
        if (s.is_ok() && is_underflowing(m_c.node())) {
            s = resolve_underflow();
        }
    }
    m_c.clear();
    return s;
}

auto Tree::find_lowest(Node &node_out) const -> Status
{
    auto s = acquire(root(), false, node_out);
    while (s.is_ok() && !node_out.is_leaf()) {
        const auto next_id = node_out.read_child_id(0);
        release(std::move(node_out));
        s = acquire(next_id, false, node_out);
    }
    return s;
}

auto Tree::find_highest(Node &node_out) const -> Status
{
    auto s = acquire(root(), false, node_out);
    while (s.is_ok() && !node_out.is_leaf()) {
        const auto next_id = NodeHdr::get_next_id(node_out.hdr());
        release(std::move(node_out));
        s = acquire(next_id, false, node_out);
    }
    return s;
}

[[nodiscard]] static constexpr auto is_overflow_type(PointerMap::Type type) -> bool
{
    return type == PointerMap::kOverflowHead ||
           type == PointerMap::kOverflowLink;
}

auto Tree::vacuum_step(PageRef &free, PointerMap::Entry entry, Schema &schema, Id last_id) -> Status
{
    CALICODB_EXPECT_NE(free.page_id, last_id);

    Status s;
    switch (entry.type) {
        case PointerMap::kOverflowLink:
            // Back pointer points to another overflow chain link, or the head of the chain.
            if (!entry.back_ptr.is_null()) {
                PageRef *parent;
                s = m_pager->acquire(entry.back_ptr, parent);
                if (s.is_ok()) {
                    m_pager->mark_dirty(*parent);
                    write_next_id(*parent, free.page_id);
                    m_pager->release(parent, Pager::kNoCache);
                }
            }
            break;
        case PointerMap::kOverflowHead: {
            // Back pointer points to the node that the overflow chain is rooted in. Search through that node's cells
            // for the target overflowing cell.
            Node parent;
            s = acquire(entry.back_ptr, true, parent);
            bool found = false;
            for (U32 i = 0, n = NodeHdr::get_cell_count(parent.hdr()); i < n; ++i) {
                Cell cell;
                if (parent.read(i, cell)) {
                    s = corrupted_page(parent.ref->page_id);
                    break;
                }
                found = cell.local_pl_size != cell.total_pl_size && read_overflow_id(cell) == last_id;
                if (found) {
                    write_overflow_id(cell, free.page_id);
                    break;
                }
            }
            const auto page_id = parent.ref->page_id;
            release(std::move(parent));
            if (s.is_ok() && !found) {
                s = corrupted_page(page_id);
            }
            break;
        }
        case PointerMap::kTreeRoot: {
            schema.vacuum_reroot(last_id, free.page_id);
            // Tree root pages are also node pages (with no parent page). Handle them the same, but
            // note the guard against updating the parent page's child pointers below.
            [[fallthrough]];
        }
        case PointerMap::kTreeNode: {
            if (entry.type != PointerMap::kTreeRoot) {
                // Back pointer points to another node, i.e. this is not a root. Search through the
                // parent for the target child pointer and overwrite it with the new page ID.
                Node parent;
                s = acquire(entry.back_ptr, true, parent);
                if (!s.is_ok()) {
                    return s;
                }
                CALICODB_EXPECT_FALSE(parent.is_leaf());
                bool found = false;
                for (U32 i = 0, n = NodeHdr::get_cell_count(parent.hdr()); !found && i <= n; ++i) {
                    const auto child_id = parent.read_child_id(i);
                    found = child_id == last_id;
                    if (found) {
                        parent.write_child_id(i, free.page_id);
                    }
                }
                if (!found) {
                    s = corrupted_page(parent.ref->page_id);
                }
                release(std::move(parent));
            }
            if (!s.is_ok()) {
                return s;
            }
            // Update references.
            Node last;
            s = acquire(last_id, true, last);
            if (!s.is_ok()) {
                return s;
            }
            s = fix_links(last, free.page_id);
            if (!s.is_ok()) {
                return s;
            }
            if (last.is_leaf()) {
                // Fix sibling links. fix_links() only fixes back pointers (parent pointers and overflow chain
                // head back pointers).
                const auto prev_id = NodeHdr::get_prev_id(last.hdr());
                if (!prev_id.is_null()) {
                    Node prev;
                    s = acquire(prev_id, true, prev);
                    if (!s.is_ok()) {
                        return s;
                    }
                    NodeHdr::put_next_id(prev.hdr(), free.page_id);
                    release(std::move(prev));
                }
                const auto next_id = NodeHdr::get_prev_id(last.hdr());
                if (!next_id.is_null()) {
                    Node next;
                    s = acquire(next_id, true, next);
                    if (!s.is_ok()) {
                        return s;
                    }
                    NodeHdr::put_prev_id(next.hdr(), free.page_id);
                    release(std::move(next));
                }
            }
            break;
        }
        default:
            return corrupted_page(PointerMap::lookup(last_id));
    }

    if (s.is_ok()) {
        s = PointerMap::write_entry(*m_pager, last_id, {});
    }
    if (s.is_ok()) {
        s = PointerMap::write_entry(*m_pager, free.page_id, entry);
    }
    if (s.is_ok()) {
        PageRef *last;
        s = m_pager->acquire(last_id, last);
        if (s.is_ok()) {
            if (is_overflow_type(entry.type)) {
                const auto next_id = read_next_id(*last);
                if (!next_id.is_null()) {
                    s = PointerMap::read_entry(*m_pager, next_id, entry);
                    if (s.is_ok()) {
                        entry.back_ptr = free.page_id;
                        s = PointerMap::write_entry(*m_pager, next_id, entry);
                    }
                }
            }
            if (s.is_ok()) {
                std::memcpy(free.page, last->page, kPageSize);
            }
            m_pager->release(last, Pager::kDiscard);
        }
    }
    return s;
}

// Determine what the last page number should be after a vacuum operation completes on a database with the
// given number of pages `db_size` and number of freelist (trunk + leaf) pages `free_size`. This computation
// was taken from SQLite (src/btree.c:finalDbSize()).
static auto vacuum_end_page(U32 db_size, U32 free_size) -> Id
{
    // Number of entries that can fit on a pointer map page.
    static constexpr auto kEntriesPerMap = kPageSize / 5;
    // PageRef *ID of the most-recent pointer map page (the page that holds the back pointer for the last page
    // in the database file).
    const auto pm_page = PointerMap::lookup(Id(db_size));
    // Number of pointer map pages between the current last page and the after-vacuum last page.
    const auto pm_size = (free_size + pm_page.value + kEntriesPerMap - db_size) / kEntriesPerMap;

    auto end_page = Id(db_size - free_size - pm_size);
    end_page.value -= PointerMap::is_map(end_page);
    return end_page;
}

// The CalicoDB database file format does not store the number of free pages; this number must be determined
// by iterating through the freelist trunk pages. At present, this only happens when a vacuum is performed.
[[nodiscard]] static auto determine_freelist_size(Pager &pager, Id free_head, U32 &size_out) -> Status
{
    Status s;
    size_out = 0;
    while (!free_head.is_null()) {
        PageRef *trunk;
        s = pager.acquire(free_head, trunk);
        if (!s.is_ok()) {
            return s;
        }
        size_out += 1 + get_u32(trunk->page + sizeof(U32));
        free_head.value = get_u32(trunk->page);
        pager.release(trunk);
    }
    return s;
}

static constexpr auto is_freelist_type(PointerMap::Type type) -> bool
{
    return type == PointerMap::kFreelistTrunk ||
           type == PointerMap::kFreelistLeaf;
}

auto Tree::vacuum(Schema &schema) -> Status
{
    auto db_size = m_pager->page_count();
    if (db_size == 0) {
        return Status::ok();
    }

    Status s;
    auto &root = m_pager->get_root();

    U32 free_size;
    const auto free_head = FileHdr::get_freelist_head(root.page);
    // Count the number of pages in the freelist, since we don't keep this information stored
    // anywhere. This involves traversing the list of freelist trunk pages. Luckily, these pages
    // are likely to be accessed again soon, so it may not hurt have them in the pager cache.
    s = determine_freelist_size(*m_pager, free_head, free_size);
    // Determine what the last page in the file should be after this vacuum is run to completion.
    const auto end_page = vacuum_end_page(db_size, free_size);
    for (; s.is_ok() && db_size > end_page.value; --db_size) {
        const Id last_page_id(db_size);
        if (!PointerMap::is_map(last_page_id)) {
            PointerMap::Entry entry;
            s = PointerMap::read_entry(*m_pager, last_page_id, entry);
            if (!s.is_ok()) {
                break;
            }
            if (!is_freelist_type(entry.type)) {
                PageRef *free = nullptr;
                // Find an unused page that will exist after the vacuum. Copy the last occupied
                // page into it. Once there are no more such unoccupied pages, the vacuum is
                // finished and all occupied pages are tightly packed at the start of the file.
                while (s.is_ok()) {
                    m_pager->release(free);
                    s = m_pager->allocate(free);
                    if (s.is_ok()) {
                        if (free->page_id <= end_page) {
                            s = vacuum_step(*free, entry, schema, last_page_id);
                            m_pager->release(free);
                            break;
                        }
                    }
                }
            }
        }
    }
    if (s.is_ok() && db_size != end_page.value) {
        std::string msg;
        append_fmt_string(
            msg, "unexpected page count %u (expected %u pages)",
            db_size, end_page.value);
        s = Status::corruption(msg);
    }
    if (s.is_ok()) {
        s = schema.vacuum_finish();
    }
    if (s.is_ok() && db_size < m_pager->page_count()) {
        m_pager->mark_dirty(root);
        FileHdr::put_freelist_head(root.page, Id::null());
        m_pager->set_page_count(db_size);
    }
    return s;
}

auto Tree::destroy_impl(Node node) -> Status
{
    Status s;
    for (U32 i = 0, n = NodeHdr::get_cell_count(node.hdr()); i <= n; ++i) {
        if (i < NodeHdr::get_cell_count(node.hdr())) {
            Cell cell;
            if (node.read(i, cell)) {
                s = corrupted_page(node.ref->page_id);
            }
            if (s.is_ok() && cell.local_pl_size != cell.total_pl_size) {
                s = free_overflow(read_overflow_id(cell));
            }
            if (!s.is_ok()) {
                break;
            }
        }
        if (!node.is_leaf()) {
            const auto save_id = node.ref->page_id;
            const auto next_id = node.read_child_id(i);
            release(std::move(node));

            Node next;
            s = acquire(next_id, false, next);
            if (s.is_ok()) {
                s = destroy_impl(std::move(next));
            }
            if (s.is_ok()) {
                s = acquire(save_id, false, node);
            }
            if (!s.is_ok()) {
                // Just return early: `node` has already been released.
                return s;
            }
        }
    }
    if (s.is_ok() && !node.ref->page_id.is_root()) {
        return free(std::move(node));
    }

    release(std::move(node));
    return s;
}

auto Tree::destroy(Tree &tree) -> Status
{
    Node root;
    auto s = tree.acquire(tree.root(), false, root);
    if (s.is_ok()) {
        s = tree.destroy_impl(std::move(root));
    }
    return s;
}

#if CALICODB_TEST

#define CHECK_OK(expr)                                           \
    do {                                                         \
        if (const auto check_s = (expr); !check_s.is_ok()) {     \
            std::fprintf(stderr, "error(line %d): %s\n",         \
                         __LINE__, check_s.to_string().c_str()); \
            std::abort();                                        \
        }                                                        \
    } while (0)

#define CHECK_TRUE(expr)                                                                   \
    do {                                                                                   \
        if (!(expr)) {                                                                     \
            std::fprintf(stderr, "error: \"%s\" was false on line %d\n", #expr, __LINE__); \
            std::abort();                                                                  \
        }                                                                                  \
    } while (0)

#define CHECK_EQ(lhs, rhs)                                                                         \
    do {                                                                                           \
        if ((lhs) != (rhs)) {                                                                      \
            std::fprintf(stderr, "error: \"" #lhs " != " #rhs "\" failed on line %d\n", __LINE__); \
            std::abort();                                                                          \
        }                                                                                          \
    } while (0)

class TreeValidator
{
    using NodeCallback = std::function<void(Node &, std::size_t)>;
    using PageCallback = std::function<void(PageRef *&)>;

    struct PrinterData {
        std::vector<std::string> levels;
        std::vector<std::size_t> spaces;
    };

    static auto traverse_inorder_helper(const Tree &tree, Node node, const NodeCallback &callback) -> void
    {
        for (std::size_t index = 0; index <= NodeHdr::get_cell_count(node.hdr()); ++index) {
            if (!node.is_leaf()) {
                const auto saved_id = node.ref->page_id;
                const auto next_id = node.read_child_id(index);

                // "node" must be released while we traverse, otherwise we are limited in how long of a traversal we can
                // perform by the number of pager frames.
                tree.release(std::move(node));

                Node next;
                CHECK_OK(tree.acquire(next_id, false, next));
                traverse_inorder_helper(tree, std::move(next), callback);
                CHECK_OK(tree.acquire(saved_id, false, node));
            }
            if (index < NodeHdr::get_cell_count(node.hdr())) {
                callback(node, index);
            }
        }
        tree.release(std::move(node));
    }

    static auto traverse_inorder(const Tree &tree, const NodeCallback &callback) -> void
    {
        Node root;
        CHECK_OK(tree.acquire(tree.root(), false, root));
        traverse_inorder_helper(tree, std::move(root), callback);
    }

    static auto traverse_chain(Pager &pager, PageRef *page, const PageCallback &callback) -> void
    {
        for (;;) {
            callback(page);

            const auto next_id = read_next_id(*page);
            pager.release(page);
            if (next_id.is_null()) {
                break;
            }
            CHECK_OK(pager.acquire(next_id, page));
        }
    }

    static auto add_to_level(PrinterData &data, const std::string &message, std::size_t target) -> void
    {
        // If target is equal to levels.size(), add spaces to all levels.
        CHECK_TRUE(target <= data.levels.size());
        std::size_t i = 0;

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
            ++L_itr;
            ++s_itr;
        }
    }

    static auto ensure_level_exists(PrinterData &data, std::size_t level) -> void
    {
        while (level >= data.levels.size()) {
            data.levels.emplace_back();
            data.spaces.emplace_back();
        }
        CHECK_TRUE(data.levels.size() > level);
        CHECK_TRUE(data.levels.size() == data.spaces.size());
    }

    static auto collect_levels(const Tree &tree, PrinterData &data, Node &node, std::size_t level) -> void
    {
        ensure_level_exists(data, level);
        const auto cell_count = NodeHdr::get_cell_count(node.hdr());
        for (std::size_t cid = 0; cid < cell_count; ++cid) {
            const auto is_first = cid == 0;
            const auto not_last = cid + 1 < cell_count;
            Cell cell;
            CHECK_EQ(0, node.read(cid, cell));

            if (!node.is_leaf()) {
                Node next;
                CHECK_OK(tree.acquire(read_child_id(cell), false, next));
                collect_levels(tree, data, next, level + 1);
            }

            if (is_first) {
                add_to_level(data, std::to_string(node.ref->page_id.value) + ":[", level);
            }
            std::string key;
            CHECK_OK(tree.read_key(node, cid, key, nullptr));
//            const auto ikey = std::to_string(std::stoi(key));
            const auto ikey = escape_string(key.substr(0, std::min(key.size(), 3UL)));
            add_to_level(data, ikey, level);
            if (cell.local_pl_size != cell.total_pl_size) {
                add_to_level(data, "(" + number_to_string(read_overflow_id(cell).value) + ")", level);
            }

            if (not_last) {
                add_to_level(data, ",", level);
            } else {
                add_to_level(data, "]", level);
            }
        }
        if (!node.is_leaf()) {
            Node next;
            CHECK_OK(tree.acquire(NodeHdr::get_next_id(node.hdr()), false, next));
            collect_levels(tree, data, next, level + 1);
        }

        tree.release(std::move(node));
    }

    [[nodiscard]] static auto get_readable_content(const PageRef &page, U32 size_limit) -> Slice
    {
        return Slice(page.page, kPageSize).range(kLinkContentOffset, std::min(size_limit, kLinkContentSize));
    }

public:
    static auto validate_tree(const Tree &tree) -> void
    {
        auto check_parent_child = [&tree](auto &node, auto index) -> void {
            Node child;
            CHECK_OK(tree.acquire(node.read_child_id(index), false, child));

            Id parent_id;
            CHECK_OK(tree.find_parent_id(child.ref->page_id, parent_id));
            CHECK_TRUE(parent_id == node.ref->page_id);

            tree.release(std::move(child));
        };
        traverse_inorder(tree, [f = std::move(check_parent_child)](const auto &node, auto index) {
            const auto count = NodeHdr::get_cell_count(node.hdr());
            CHECK_TRUE(index < count);

            if (!node.is_leaf()) {
                f(node, index);
                // Rightmost child.
                if (index + 1 == count) {
                    f(node, index + 1);
                }
            }
        });

        traverse_inorder(tree, [&tree](auto &node, auto index) {
            Cell cell;
            CHECK_EQ(0, node.read(index, cell));

            auto accumulated = cell.local_pl_size;
            auto requested = cell.key_size;
            if (node.is_leaf()) {
                U32 value_size = 0;
                CHECK_TRUE(decode_varint(cell.ptr, node.ref->page + kPageSize, value_size));
                requested += value_size;
            }

            if (cell.local_pl_size != cell.total_pl_size) {
                const auto overflow_id = read_overflow_id(cell);
                PageRef *head;
                CHECK_OK(tree.m_pager->acquire(overflow_id, head));
                traverse_chain(*tree.m_pager, head, [&](auto &page) {
                    CHECK_TRUE(requested > accumulated);
                    const auto size_limit = std::min(static_cast<U32>(kPageSize), requested - accumulated);
                    accumulated += U32(get_readable_content(*page, size_limit).size());
                });
                CHECK_EQ(requested, accumulated);
            }

            if (index == 0) {
                CHECK_TRUE(node.assert_state());

                if (node.is_leaf() && !NodeHdr::get_next_id(node.hdr()).is_null()) {
                    Node next;
                    CHECK_OK(tree.acquire(NodeHdr::get_next_id(node.hdr()), false, next));

                    tree.release(std::move(next));
                }
            }
        });

        // Find the leftmost external node.
        Node node;
        CHECK_OK(tree.acquire(tree.root(), false, node));
        while (!node.is_leaf()) {
            const auto id = node.read_child_id(0);
            tree.release(std::move(node));
            CHECK_OK(tree.acquire(id, false, node));
        }
        while (!NodeHdr::get_next_id(node.hdr()).is_null()) {
            Node right;
            CHECK_OK(tree.acquire(NodeHdr::get_next_id(node.hdr()), false, right));
            std::string lhs_buffer, rhs_buffer;
            CHECK_OK(const_cast<Tree &>(tree).read_key(node, 0, lhs_buffer, nullptr));
            CHECK_OK(const_cast<Tree &>(tree).read_key(right, 0, rhs_buffer, nullptr));
            CHECK_TRUE(lhs_buffer < rhs_buffer);
            CHECK_EQ(NodeHdr::get_prev_id(right.hdr()), node.ref->page_id);
            tree.release(std::move(node));
            node = std::move(right);
        }
        tree.release(std::move(node));
    }

    [[nodiscard]] static auto to_string(const Tree &tree) -> std::string
    {
        std::string repr;
        PrinterData data;

        Node root;
        CHECK_OK(tree.acquire(tree.root(), false, root));
        collect_levels(tree, data, root, 0);
        for (const auto &level : data.levels) {
            repr.append(level + '\n');
        }
        return repr;
    }
};

auto Tree::TEST_validate() const -> void
{
    TreeValidator::validate_tree(*this);
}

auto Tree::TEST_to_string() const -> std::string
{
    return TreeValidator::to_string(*this);
}

#undef CHECK_TRUE
#undef CHECK_EQ
#undef CHECK_OK

#else

auto Node::TEST_validate() -> void
{
}

auto Tree::TEST_to_string() const -> std::string
{
    return "";
}

auto Tree::TEST_validate() const -> void
{
}

#endif // CALICODB_TEST

Tree::InternalCursor::InternalCursor(Tree &tree)
    : m_tree(&tree)
{
}

Tree::InternalCursor::~InternalCursor()
{
    clear();
}

auto Tree::InternalCursor::clear() -> void
{
    m_tree->release(std::move(m_node));
    m_status = Status::ok();
}

auto Tree::InternalCursor::seek_root() -> void
{
    clear();
    std::memset(history, 0, sizeof(history));
    history[0].page_id = m_tree->root();
    level = 0;

    m_status = m_tree->acquire(m_tree->root(), false, m_node);
}

auto Tree::InternalCursor::seek(const Slice &key) -> bool
{
    CALICODB_EXPECT_TRUE(is_valid());

    auto exact = false;
    auto upper = NodeHdr::get_cell_count(m_node.hdr());
    U32 lower = 0;
    while (lower < upper) {
        Slice rhs;
        const auto mid = (lower + upper) / 2;
        // This call to Tree::read_key() may return a partial key, if the whole key wasn't
        // needed for the comparison. We read at most 1 byte more than is present in `key`
        // so we still have necessary length information to break ties. This lets us avoid
        // reading overflow chains if it isn't really necessary.
        m_status = m_tree->read_key(m_node, mid, m_buffer,
                                    &rhs, key.size() + 1);
        if (!m_status.is_ok()) {
            break;
        }
        const auto cmp = key.compare(rhs);
        if (cmp <= 0) {
            exact = cmp == 0;
            upper = mid;
        } else if (cmp > 0) {
            lower = mid + 1;
        }
    }

    const U32 shift = exact * !m_node.is_leaf();
    history[level].index = lower + shift;
    return exact;
}

auto Tree::InternalCursor::move_down(Id child_id) -> void
{
    CALICODB_EXPECT_TRUE(is_valid());
    clear();
    history[++level] = {child_id, 0};
    m_status = m_tree->acquire(child_id, false, m_node);
}

Cursor::Cursor() = default;

Cursor::~Cursor() = default;

CursorImpl::~CursorImpl()
{
    clear();
    if (m_count_ptr) {
        --*m_count_ptr;
    }
}

auto CursorImpl::fetch_payload(Node &node, std::size_t index) -> Status
{
    m_key.clear();
    m_val.clear();

    Cell cell;
    if (node.read(index, cell)) {
        return m_tree->corrupted_page(node.ref->page_id);
    }

    auto s = m_tree->read_key(cell, m_key_buf, &m_key);
    if (s.is_ok()) {
        s = m_tree->read_value(cell, m_val_buf, &m_val);
    }
    return s;
}

auto CursorImpl::seek_first() -> void
{
    clear();

    Node lowest;
    m_status = m_tree->find_lowest(lowest);
    if (m_status.is_ok()) {
        seek_to(std::move(lowest), 0);
    }
}

auto CursorImpl::seek_last() -> void
{
    clear();

    Node highest;
    m_status = m_tree->find_highest(highest);
    if (!m_status.is_ok()) {
        return;
    }
    if (NodeHdr::get_cell_count(highest.hdr()) > 0) {
        const auto idx = NodeHdr::get_cell_count(highest.hdr());
        seek_to(std::move(highest), idx - 1);
    } else {
        m_tree->release(std::move(highest));
    }
}

auto CursorImpl::next() -> void
{
    CALICODB_EXPECT_TRUE(is_valid());
    if (++m_index < NodeHdr::get_cell_count(m_node.hdr())) {
        auto s = fetch_payload(m_node, m_index);
        if (!s.is_ok()) {
            clear(s);
        }
        return;
    }
    const auto next_id = NodeHdr::get_next_id(m_node.hdr());
    clear();

    if (next_id.is_null()) {
        return;
    }
    Node node;
    m_status = m_tree->acquire(next_id, false, node);
    if (m_status.is_ok()) {
        seek_to(std::move(node), 0);
    }
}

auto CursorImpl::previous() -> void
{
    CALICODB_EXPECT_TRUE(is_valid());
    if (m_index) {
        auto s = fetch_payload(m_node, --m_index);
        if (!s.is_ok()) {
            clear(s);
        }
        return;
    }
    const auto prev_id = NodeHdr::get_prev_id(m_node.hdr());
    clear();

    if (prev_id.is_null()) {
        return;
    }
    Node node;
    m_status = m_tree->acquire(prev_id, false, node);
    if (m_status.is_ok()) {
        // node should never be empty. TODO: Report corruption
        const auto idx = NodeHdr::get_cell_count(node.hdr());
        seek_to(std::move(node), std::max(1U, idx) - 1);
    }
}

auto CursorImpl::seek_to(Node node, std::size_t index) -> void
{
    CALICODB_EXPECT_EQ(nullptr, m_node.ref);
    CALICODB_EXPECT_TRUE(m_status.is_ok());
    CALICODB_EXPECT_TRUE(node.is_leaf());

    if (index == NodeHdr::get_cell_count(node.hdr()) && !NodeHdr::get_next_id(node.hdr()).is_null()) {
        const auto next_id = NodeHdr::get_next_id(node.hdr());
        m_tree->release(std::move(node));
        auto s = m_tree->acquire(next_id, false, node);
        if (!s.is_ok()) {
            m_status = s;
            return;
        }
        index = 0;
    }
    if (index < NodeHdr::get_cell_count(node.hdr())) {
        m_status = fetch_payload(node, index);
        if (m_status.is_ok()) {
            m_node = std::move(node);
            m_index = static_cast<U32>(index);
            return;
        }
    }
    m_tree->release(std::move(node));
}

auto CursorImpl::seek(const Slice &key) -> void
{
    clear();

    bool unused;
    auto s = m_tree->find_external(key, unused);
    if (s.is_ok()) {
        const auto idx = m_tree->m_c.index();
        seek_to(std::move(m_tree->m_c.node()), idx);
        m_tree->m_c.clear();
    } else {
        m_status = s;
    }
}

auto CursorImpl::clear(Status s) -> void
{
    m_tree->release(std::move(m_node));
    m_status = std::move(s);
}

} // namespace calicodb

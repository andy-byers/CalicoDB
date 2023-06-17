// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "tree.h"
#include "db_impl.h"
#include "encoding.h"
#include "freelist.h"
#include "logging.h"
#include "pager.h"
#include "schema.h"
#include "scope_guard.h"
#include "utils.h"
#include <array>
#include <functional>
#include <numeric>

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

static auto detach_cell(Cell &cell, char *backing)
{
    if (backing && cell.ptr != backing) {
        std::memcpy(backing, cell.ptr, cell.footprint);
        const auto diff = cell.key - cell.ptr;
        cell.ptr = backing;
        cell.key = backing + diff;
    }
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
    auto memory_size = kPageSize - cell_start;
    auto *memory = root.ref->page + cell_start;
    std::memcpy(memory, child.ref->page + cell_start, memory_size);

    // Copy the header and cell pointers.
    memory_size = NodeHdr::get_cell_count(child.hdr()) * kCellPtrSize;
    memory = root.ref->page + cell_slots_offset(root);
    std::memcpy(memory, child.ref->page + cell_slots_offset(child), memory_size);
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
    static auto promote(Pager &pager, char *scratch, Cell &cell, Id parent_id) -> Status
    {
        detach_cell(cell, scratch);

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
    CALICODB_TRY(PointerMap::read_entry(*m_pager, page_id, entry));
    out = entry.back_ptr;
    return Status::ok();
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
    const auto rc = node.write(index, cell, m_cell_scratch);
    if (rc < 0) {
        return corrupted_page(node.ref->page_id);
    } else if (rc == 0) {
        m_ovfl_cell = cell;
        m_has_ovfl = true;
    }
    if (!node.is_leaf()) {
        CALICODB_TRY(fix_parent_id(read_child_id(cell), node.ref->page_id, PointerMap::kTreeNode));
    }
    return maybe_fix_overflow_chain(cell, node.ref->page_id);
}

auto Tree::remove_cell(Node &node, std::size_t index) -> Status
{
    Cell cell;
    if (node.read(index, cell)) {
        return corrupted_page(node.ref->page_id);
    }
    if (cell.local_pl_size != cell.total_pl_size) {
        CALICODB_TRY(free_overflow(read_overflow_id(cell)));
    }
    if (node.erase(index, cell.footprint)) {
        return corrupted_page(node.ref->page_id);
    }
    return Status::ok();
}

auto Tree::free_overflow(Id head_id) -> Status
{
    while (!head_id.is_null()) {
        PageRef *page;
        CALICODB_TRY(m_pager->acquire(head_id, page));
        head_id = read_next_id(*page);
        CALICODB_TRY(m_pager->destroy(page));
    }
    return Status::ok();
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
    for (std::size_t index = 0; index < NodeHdr::get_cell_count(node.hdr()); ++index) {
        Cell cell;
        if (node.read(index, cell)) {
            return corrupted_page(node.ref->page_id);
        }
        // Fix the back pointer for the head of an overflow chain rooted at `node`.
        CALICODB_TRY(maybe_fix_overflow_chain(cell, parent_id));
        if (!node.is_leaf()) {
            // Fix the parent pointer for the current child node.
            CALICODB_TRY(fix_parent_id(read_child_id(cell), parent_id, PointerMap::kTreeNode));
        }
    }
    if (!node.is_leaf()) {
        CALICODB_TRY(fix_parent_id(NodeHdr::get_next_id(node.hdr()), parent_id, PointerMap::kTreeNode));
    }
    if (m_has_ovfl) {
        CALICODB_TRY(maybe_fix_overflow_chain(m_ovfl_cell, parent_id));
        if (!node.is_leaf()) {
            CALICODB_TRY(fix_parent_id(read_child_id(m_ovfl_cell), parent_id, PointerMap::kTreeNode));
        }
    }
    return Status::ok();
}

auto Tree::allocate(bool is_external, Node &node) -> Status
{
    PageRef *ref;
    auto s = m_pager->allocate(ref);
    if (s.is_ok()) {
        CALICODB_EXPECT_FALSE(PointerMap::is_map(node.ref->page_id));
        node = Node::from_new_page(*ref, is_external);
    }
    return s;
}

auto Tree::acquire(Id page_id, bool write, Node &node) const -> Status
{
    // The internal cursor should use this method instead of acquire(), since it has a dedicated Node
    // object that is populated instead of one of the working set node slots.
    CALICODB_EXPECT_FALSE(PointerMap::is_map(page_id));
    
    PageRef *ref;
    auto s = m_pager->acquire(page_id, ref);
    if (s.is_ok()) {
        if (Node::from_existing_page(*ref, node)) {
            return corrupted_page(page_id);
        }
        if (write) {
            upgrade(node);
        }
    }
    return s;
}

auto Tree::free(Node &node) -> Status
{
    return m_pager->destroy(node.ref);
}

auto Tree::upgrade(Node &node) const -> void
{
    m_pager->mark_dirty(*node.ref);
}

auto Tree::release(Node node) const -> void
{
    if (node.ref && m_pager->mode() == Pager::kDirty) {
        // If the pager is in kWrite mode and a page is marked dirty, it immediately
        // transitions to kDirty mode. So, if this node is dirty, then the pager must
        // be in kDirty mode (unless there was an error).
        if (NodeHdr::get_frag_count(node.hdr()) > 0x80) {
            // Fragment count is too large. Defragment the node to get rid of all fragments.
            if (node.defrag(m_node_scratch)) {
                // Sets the pager error status.
                (void)corrupted_page(node.ref->page_id);
            }
        }
    }
    // Pager::release() NULLs out the page reference.
    m_pager->release(node.ref);
}

auto Tree::advance_cursor(Node node, int diff) const -> void
{
    // InternalCursor::move_to() takes ownership of the page reference in `node`. When the working set
    // is cleared below, this reference is not released.
    m_c.move_to(std::move(node), diff);
}

auto Tree::finish_operation() const -> void
{
    m_c.clear();
}

auto Tree::resolve_overflow() -> Status
{
    CALICODB_EXPECT_TRUE(m_c.is_valid());

    Status s;
    while (m_has_ovfl) {
        if (m_c.node().ref->page_id == root()) {
            s = split_root();
        } else {
            s = split_nonroot();
        }
        if (s.is_ok()) {
            ++m_stats.stats[kStatSMOCount];
        } else {
            break;
        }
    }
    m_c.clear();
    return s;
}

auto Tree::split_root() -> Status
{
    auto &root = m_c.node();
    CALICODB_EXPECT_EQ(Tree::root(), root.ref->page_id);

    Node child;
    auto s = allocate(root.is_leaf(), child);
    if (s.is_ok()) {
        // Copy the cell content area.
        const auto after_root_headers = cell_area_offset(root);
        auto area_size = kPageSize - after_root_headers;
        auto *area = child.ref->page + after_root_headers;
        std::memcpy(area, root.ref->page + after_root_headers, area_size);

        // Copy the header and cell pointers. Doesn't copy the page LSN.
        area_size = NodeHdr::get_cell_count(root.hdr()) * kCellPtrSize;
        area = child.ref->page + cell_slots_offset(child);
        std::memcpy(area, root.ref->page + cell_slots_offset(root), area_size);
        std::memcpy(child.hdr(), root.hdr(), NodeHdr::kSize);

        CALICODB_EXPECT_TRUE(m_has_ovfl);
        child.gap_size = root.gap_size;
        if (root.ref->page_id.is_root()) {
            child.gap_size += FileHdr::kSize;
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
        m_c.history[0].index = 0;
        advance_cursor(std::move(child), 1);
    }
    return s;
}

auto Tree::transfer_left(Node &left, Node &right) -> Status
{
    CALICODB_EXPECT_EQ(left.is_leaf(), right.is_leaf());
    Cell cell;
    if (right.read(0, cell)) {
        return corrupted_page(right.ref->page_id);
    }
    auto s = insert_cell(left, NodeHdr::get_cell_count(left.hdr()), cell);
    if (s.is_ok()) {
        CALICODB_EXPECT_FALSE(m_has_ovfl);
        if (right.erase(0, cell.footprint)) {
            s = corrupted_page(right.ref->page_id);
        }
    }
    return s;
}

auto Tree::split_nonroot() -> Status
{
    auto &node = m_c.node();
    CALICODB_EXPECT_NE(node.ref->page_id, root());
    CALICODB_EXPECT_TRUE(m_has_ovfl);

    CALICODB_EXPECT_LT(0, m_c.level);
    const auto last_loc = m_c.history[m_c.level - 1];
    const auto parent_id = last_loc.page_id;
    CALICODB_EXPECT_FALSE(parent_id.is_null());

    Node parent, left;
    U32 cell_count;
    U32 overflow_index;
    
    auto s = acquire(parent_id, true, parent);
    if (s.is_ok()) {
        s = allocate(node.is_leaf(), left);
    }
    if (!s.is_ok()) {
        goto cleanup;
    }

    cell_count = NodeHdr::get_cell_count(node.hdr());
    m_has_ovfl = false;

    if (m_c.index() == cell_count) {
        // Note the reversal of the "left" and "right" parameters. We are splitting the other way.
        // This can greatly improve the performance of sequential writes.
        return split_nonroot_fast(
            std::move(parent),
            std::move(left));
    }

    // Fix the overflow. The overflow cell should fit in either "left" or "right". This routine
    // works by transferring cells, one-by-one, from "right" to "left", and trying to insert the
    // overflow cell. Where the overflow cell is written depends on how many cells we have already
    // transferred. If "overflow_index" is 0, we definitely have enough room in "left". Otherwise,
    // we transfer a cell and try to write the overflow cell to "right". If this isn't possible,
    // then the left node must have enough room, since the maximum cell size is limited to roughly
    // 1/4 of a page. If "right" is more than 3/4 full, then "left" must be less than 1/4 full, so
    // it must be able to accept the overflow cell without overflowing.
    for (U32 i = 0; s.is_ok() && i < cell_count; ++i) {
        if (i == m_c.index()) {
            s = insert_cell(left, NodeHdr::get_cell_count(left.hdr()), m_ovfl_cell);
            break;
        }
        s = transfer_left(left, node);
        if (!s.is_ok()) {
            break;
        }

        if (node.usable_space >= m_ovfl_cell.footprint + kCellPtrSize) {
            s = insert_cell(node, m_c.index() - i - 1, m_ovfl_cell);
            break;
        }
        CALICODB_EXPECT_NE(i + 1, cell_count);
    }
    CALICODB_EXPECT_FALSE(m_has_ovfl);
    if (!s.is_ok()) {
        goto cleanup;
    }

    Cell separator;
    if (node.read(0, separator)) {
        s = corrupted_page(node.ref->page_id);
        goto cleanup;
    }
    detach_cell(separator, cell_scratch());

    if (node.is_leaf()) {
        const auto prev_id = NodeHdr::get_prev_id(node.hdr());
        if (!prev_id.is_null()) {
            Node left_sibling;
            s = acquire(prev_id, true, left_sibling);
            if (!s.is_ok()) {
                goto cleanup;
            }
            NodeHdr::put_next_id(left_sibling.hdr(), left.ref->page_id);
            NodeHdr::put_prev_id(left.hdr(), left_sibling.ref->page_id);
            release(std::move(left_sibling));
        }
        NodeHdr::put_prev_id(node.hdr(), left.ref->page_id);
        NodeHdr::put_next_id(left.hdr(), node.ref->page_id);
        s = PayloadManager::promote(
            *m_pager,
            nullptr,
            separator,
            parent_id);
    } else {
        const auto next_id = read_child_id(separator);
        NodeHdr::put_next_id(left.hdr(), next_id);
        s = fix_parent_id(next_id, left.ref->page_id, PointerMap::kTreeNode);
        if (s.is_ok() && node.erase(0, separator.footprint)) {
            s = corrupted_page(node.ref->page_id);
        }
    }
    if (s.is_ok()) {
        // Post the separator into the parent node. This call will fix the sibling's parent pointer.
        write_child_id(separator, left.ref->page_id);
        s = insert_cell(parent, last_loc.index, separator);
    }

cleanup:
    advance_cursor(std::move(parent), -1);
    release(std::move(left));
    return s;
}

auto Tree::split_nonroot_fast(Node parent, Node right) -> Status
{
    auto &left = m_c.node();
    auto s = insert_cell(right, 0, m_ovfl_cell);
    CALICODB_EXPECT_FALSE(m_has_ovfl);
    InternalCursor::Location last_loc;

    Cell separator;
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

        if (right.read(0, separator)) {
            s = corrupted_page(right.ref->page_id);
            goto cleanup;
        }
        s = PayloadManager::promote(
            *m_pager,
            cell_scratch(),
            separator,
            parent.ref->page_id);
    } else {
        auto cell_count = NodeHdr::get_cell_count(left.hdr());
        if (left.read(cell_count - 1, separator)) {
            s = corrupted_page(left.ref->page_id);
            goto cleanup;
        }
        detach_cell(separator, cell_scratch());
        left.erase(cell_count - 1, separator.footprint);

        NodeHdr::put_next_id(right.hdr(), NodeHdr::get_next_id(left.hdr()));
        NodeHdr::put_next_id(left.hdr(), read_child_id(separator));
        s = fix_parent_id(NodeHdr::get_next_id(right.hdr()), right.ref->page_id, PointerMap::kTreeNode);
        if (s.is_ok()) {
            s = fix_parent_id(NodeHdr::get_next_id(left.hdr()), left.ref->page_id, PointerMap::kTreeNode);
        }
        if (!s.is_ok()) {
            goto cleanup;
        }
    }

    CALICODB_EXPECT_LT(0, m_c.level);
    last_loc = m_c.history[m_c.level - 1];

    // Post the separator into the parent node. This call will fix the sibling's parent pointer.
    write_child_id(separator, left.ref->page_id);
    s = insert_cell(parent, last_loc.index, separator);
    if (s.is_ok()) {
        parent.write_child_id(last_loc.index + !m_has_ovfl, right.ref->page_id);
        s = fix_parent_id(right.ref->page_id, parent.ref->page_id, PointerMap::kTreeNode);
    }

cleanup:
    advance_cursor(std::move(parent), -1);
    release(std::move(right));
    return s;
}

auto Tree::resolve_underflow() -> Status
{
    Status s;
    while (s.is_ok() && m_c.is_valid() && is_underflowing(m_c.node())) {
        if (m_c.node().ref->page_id == root()) {
            return fix_root();
        }
        CALICODB_EXPECT_LT(0, m_c.level);

        Node parent;
        const auto last_loc = m_c.history[m_c.level - 1];
        s = acquire(last_loc.page_id, true, parent);
        if (s.is_ok()) {
            s = fix_nonroot(std::move(parent), last_loc.index);
        }

        ++m_stats.stats[kStatSMOCount];
    }
    return s;
}

auto Tree::merge_left(Node &left, Node &right, Node &parent, std::size_t index) -> Status
{
    CALICODB_EXPECT_FALSE(parent.is_leaf());
    CALICODB_EXPECT_TRUE(is_underflowing(left));

    if (left.is_leaf()) {
        CALICODB_EXPECT_TRUE(right.is_leaf());
        const auto next_id = NodeHdr::get_next_id(right.hdr());
        NodeHdr::put_next_id(left.hdr(), next_id);
        CALICODB_TRY(remove_cell(parent, index));

        while (NodeHdr::get_cell_count(right.hdr())) {
            CALICODB_TRY(transfer_left(left, right));
        }
        parent.write_child_id(index, left.ref->page_id);

        if (!next_id.is_null()) {
            Node right_sibling;
            CALICODB_TRY(acquire(next_id, true, right_sibling));
            NodeHdr::put_prev_id(right_sibling.hdr(), left.ref->page_id);
            release(std::move(right_sibling));
        }
    } else {
        CALICODB_EXPECT_FALSE(right.is_leaf());
        Cell separator;
        const auto is_corrupted = 
            parent.read(index, separator) || 
            left.write(NodeHdr::get_cell_count(left.hdr()), separator, m_node_scratch) < 0;
        if (is_corrupted) {
            return corrupted_page(parent.ref->page_id);
        }
        left.write_child_id(NodeHdr::get_cell_count(left.hdr()) - 1, NodeHdr::get_next_id(left.hdr()));
        CALICODB_TRY(fix_parent_id(NodeHdr::get_next_id(left.hdr()), left.ref->page_id, PointerMap::kTreeNode));
        CALICODB_TRY(maybe_fix_overflow_chain(separator, left.ref->page_id));
        parent.erase(index, separator.footprint);

        while (NodeHdr::get_cell_count(right.hdr())) {
            CALICODB_TRY(transfer_left(left, right));
        }
        NodeHdr::put_next_id(left.hdr(), NodeHdr::get_next_id(right.hdr()));
        parent.write_child_id(index, left.ref->page_id);
    }
    CALICODB_TRY(fix_links(left));
    return free(right);
}

auto Tree::merge_right(Node &left, Node &right, Node &parent, std::size_t index) -> Status
{
    CALICODB_EXPECT_FALSE(parent.is_leaf());
    CALICODB_EXPECT_TRUE(is_underflowing(right));
    if (left.is_leaf()) {
        CALICODB_EXPECT_TRUE(right.is_leaf());
        const auto next_id = NodeHdr::get_next_id(right.hdr());
        NodeHdr::put_next_id(left.hdr(), next_id);
        CALICODB_EXPECT_EQ(parent.read_child_id(index + 1), right.ref->page_id);
        parent.write_child_id(index + 1, left.ref->page_id);
        CALICODB_TRY(remove_cell(parent, index));

        while (NodeHdr::get_cell_count(right.hdr())) {
            CALICODB_TRY(transfer_left(left, right));
        }
        if (!next_id.is_null()) {
            Node right_sibling;
            CALICODB_TRY(acquire(next_id, true, right_sibling));
            NodeHdr::put_prev_id(right_sibling.hdr(), left.ref->page_id);
            release(std::move(right_sibling));
        }
    } else {
        CALICODB_EXPECT_FALSE(right.is_leaf());

        Cell separator;
        const auto is_corrupted = 
            parent.read(index, separator) ||
            left.write(NodeHdr::get_cell_count(left.hdr()), separator, m_node_scratch) < 0;
        if (is_corrupted) {
            return corrupted_page(parent.ref->page_id);
        }
        left.write_child_id(NodeHdr::get_cell_count(left.hdr()) - 1, NodeHdr::get_next_id(left.hdr()));
        CALICODB_TRY(fix_parent_id(NodeHdr::get_next_id(left.hdr()), left.ref->page_id, PointerMap::kTreeNode));
        CALICODB_TRY(maybe_fix_overflow_chain(separator, left.ref->page_id));
        NodeHdr::put_next_id(left.hdr(), NodeHdr::get_next_id(right.hdr()));

        CALICODB_EXPECT_EQ(parent.read_child_id(index + 1), right.ref->page_id);
        parent.write_child_id(index + 1, left.ref->page_id);
        parent.erase(index, separator.footprint);

        // Transfer the rest of the cells. left shouldn't overflow.
        while (NodeHdr::get_cell_count(right.hdr())) {
            CALICODB_TRY(transfer_left(left, right));
        }
    }
    CALICODB_TRY(fix_links(left));
    return free(right);
}

auto Tree::fix_nonroot(Node parent, std::size_t index) -> Status
{
    auto &node = m_c.node();
    CALICODB_EXPECT_NE(node.ref->page_id, root());
    CALICODB_EXPECT_TRUE(is_underflowing(node));
    CALICODB_EXPECT_FALSE(m_has_ovfl);
    Node sibling;
    Status s;

    if (index > 0) {
        s = acquire(parent.read_child_id(index - 1), true, sibling);
        if (!s.is_ok()) {
            goto cleanup;
        }
        if (NodeHdr::get_cell_count(sibling.hdr()) == 1) {
            s = merge_right(sibling, m_c.node(), parent, index - 1);
            CALICODB_EXPECT_FALSE(m_has_ovfl);
            goto cleanup;
        }
        s = rotate_right(parent, sibling, node, index - 1);
    } else {
        s = acquire(parent.read_child_id(index + 1), true, sibling);
        if (!s.is_ok()) {
            goto cleanup;
        }
        if (NodeHdr::get_cell_count(sibling.hdr()) == 1) {
            s = merge_left(node, sibling, parent, index);
            CALICODB_EXPECT_FALSE(m_has_ovfl);
            goto cleanup;
        }
        s = rotate_left(parent, node, sibling, index);
    }

    CALICODB_EXPECT_FALSE(m_has_ovfl);
    
cleanup:
    advance_cursor(std::move(parent), -1);
    if (s.is_ok() && m_has_ovfl) {
        return resolve_overflow();
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
            } else {
                m_ovfl_cell = cell;
                detach_cell(m_ovfl_cell, cell_scratch());
                child.erase(m_c.index(), cell.footprint);
                advance_cursor(std::move(child), 0);
                s = split_nonroot();
            }
        } else {
            if (merge_root(node, child, m_node_scratch)) {
                s = corrupted_page(node.ref->page_id);
            } else {
                s = free(child);
            }
            if (s.is_ok()) {
                s = fix_links(node);
            }
        }
        release(std::move(child));
    }
    return s;
}

auto Tree::rotate_left(Node &parent, Node &left, Node &right, std::size_t index) -> Status
{
    CALICODB_EXPECT_FALSE(parent.is_leaf());
    CALICODB_EXPECT_GT(NodeHdr::get_cell_count(parent.hdr()), 0);
    CALICODB_EXPECT_GT(NodeHdr::get_cell_count(right.hdr()), 1);
    if (left.is_leaf()) {
        CALICODB_EXPECT_TRUE(right.is_leaf());

        Cell lowest;
        if (right.read(0, lowest)) {
            return corrupted_page(right.ref->page_id);
        }
        CALICODB_TRY(insert_cell(left, NodeHdr::get_cell_count(left.hdr()), lowest));
        CALICODB_EXPECT_FALSE(m_has_ovfl);
        right.erase(0, lowest.footprint);

        Cell separator;
        if (right.read(0, separator)) {
            return corrupted_page(right.ref->page_id);
        }
        CALICODB_TRY(PayloadManager::promote(
            *m_pager,
            cell_scratch(),
            separator,
            parent.ref->page_id));
        write_child_id(separator, left.ref->page_id);

        CALICODB_TRY(remove_cell(parent, index));
        return insert_cell(parent, index, separator);
    } else {
        CALICODB_EXPECT_FALSE(right.is_leaf());

        Node child;
        CALICODB_TRY(acquire(right.read_child_id(0), true, child));
        const auto saved_id = NodeHdr::get_next_id(left.hdr());
        NodeHdr::put_next_id(left.hdr(), child.ref->page_id);
        CALICODB_TRY(fix_parent_id(child.ref->page_id, left.ref->page_id, PointerMap::kTreeNode));

        Cell separator;
        if (parent.read(index, separator)) {
            return corrupted_page(parent.ref->page_id);
        }
        CALICODB_TRY(insert_cell(left, NodeHdr::get_cell_count(left.hdr()), separator));
        CALICODB_EXPECT_FALSE(m_has_ovfl);
        left.write_child_id(NodeHdr::get_cell_count(left.hdr()) - 1, saved_id);
        parent.erase(index, separator.footprint);

        Cell lowest;
        if (right.read(0, lowest)) {
            return corrupted_page(right.ref->page_id);
        }
        detach_cell(lowest, cell_scratch());
        right.erase(0, lowest.footprint);
        write_child_id(lowest, left.ref->page_id);
        return insert_cell(parent, index, lowest);
    }
}

auto Tree::rotate_right(Node &parent, Node &left, Node &right, std::size_t index) -> Status
{
    CALICODB_EXPECT_FALSE(parent.is_leaf());
    CALICODB_EXPECT_GT(NodeHdr::get_cell_count(parent.hdr()), 0);
    CALICODB_EXPECT_GT(NodeHdr::get_cell_count(left.hdr()), 1);

    if (left.is_leaf()) {
        CALICODB_EXPECT_TRUE(right.is_leaf());

        Cell highest;
        if (left.read(NodeHdr::get_cell_count(left.hdr()) - 1, highest)) {
            return corrupted_page(left.ref->page_id);
        }
        CALICODB_TRY(insert_cell(right, 0, highest));
        CALICODB_EXPECT_FALSE(m_has_ovfl);

        auto separator = highest;
        CALICODB_TRY(PayloadManager::promote(
            *m_pager,
            cell_scratch(),
            separator,
            parent.ref->page_id));
        write_child_id(separator, left.ref->page_id);

        // Don't erase the cell until it has been detached.
        left.erase(NodeHdr::get_cell_count(left.hdr()) - 1, highest.footprint);

        CALICODB_TRY(remove_cell(parent, index));
        CALICODB_TRY(insert_cell(parent, index, separator));
    } else {
        CALICODB_EXPECT_FALSE(right.is_leaf());

        Node child;
        CALICODB_TRY(acquire(NodeHdr::get_next_id(left.hdr()), true, child));
        const auto child_id = child.ref->page_id;
        CALICODB_TRY(fix_parent_id(child.ref->page_id, right.ref->page_id, PointerMap::kTreeNode));
        NodeHdr::put_next_id(left.hdr(), left.read_child_id(NodeHdr::get_cell_count(left.hdr()) - 1));

        Cell separator;
        if (parent.read(index, separator)) {
            return corrupted_page(parent.ref->page_id);
        }
        CALICODB_TRY(insert_cell(right, 0, separator));
        CALICODB_EXPECT_FALSE(m_has_ovfl);
        right.write_child_id(0, child_id);
        parent.erase(index, separator.footprint);

        Cell highest;
        if (left.read(NodeHdr::get_cell_count(left.hdr()) - 1, highest)) {
            return corrupted_page(left.ref->page_id);
        }
        detach_cell(highest, cell_scratch());
        write_child_id(highest, left.ref->page_id);
        left.erase(NodeHdr::get_cell_count(left.hdr()) - 1, highest.footprint);
        CALICODB_TRY(insert_cell(parent, index, highest));
    }
    return Status::ok();
}

Tree::Tree(Pager &pager, char *scratch, const Id *root_id)
    : m_c(*this),
      m_node_scratch(scratch),
      m_cell_scratch(scratch + kPageSize),
      m_pager(&pager),
      m_root_id(root_id)
{
}

auto Tree::cell_scratch() -> char *
{
    // Leave space for a child ID. We need the maximum difference between the size of a varint and
    // an Id. When a cell is promoted (i.e. made into an internal cell, so it can be posted to the
    // parent node) it loses a varint (the value size), but gains an Id (the left child pointer).
    // We should be able to write any external cell to this location, and still have room to write
    // the left child ID before the key size field, regardless of the number of bytes occupied by
    // the varint value size.
    return m_cell_scratch + sizeof(U32) - 1;
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
            // Create a cell representing the `key` and `value`. This routine also populates any
            // overflow pages necessary to hold a `key` and/or `value` that won't fit on a single
            // node page. If the cell cannot fit in `node`, it will be written to scratch memory.
            s = emplace(m_c.node(), key, value, m_c.index(), overflow);

            if (s.is_ok()) {
                if (overflow) {
                    // There wasn't enough room in `node` to hold the cell. Get the node back and
                    // perform a split.
                    Cell ovfl;
                    if (m_c.node().parser(cell_scratch(), cell_scratch() + kPageSize, &ovfl)) {
                        s = corrupted_page(m_c.node().ref->page_id);
                    } else {
                        m_ovfl_cell = ovfl;
                        m_has_ovfl = true;
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
    const auto hdr_size = std::uintptr_t(ptr - header);
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
            for (std::size_t i = 0; s.is_ok() && i < NodeHdr::get_cell_count(parent.hdr()); ++i) {
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
                for (std::size_t i = 0; !found && i <= NodeHdr::get_cell_count(parent.hdr()); ++i) {
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

// NOTE: `node` is not part of the working set.
auto Tree::destroy_impl(Node &node) -> Status
{
    ScopeGuard guard = [this, &node] {
        release(std::move(node));
    };
    for (std::size_t i = 0; i <= NodeHdr::get_cell_count(node.hdr()); ++i) {
        if (i < NodeHdr::get_cell_count(node.hdr())) {
            Cell cell;
            if (node.read(i, cell)) {
                return corrupted_page(node.ref->page_id);
            }
            if (cell.local_pl_size != cell.total_pl_size) {
                CALICODB_TRY(free_overflow(read_overflow_id(cell)));
            }
        }
        if (!node.is_leaf()) {
            const auto save_id = node.ref->page_id;
            const auto next_id = node.read_child_id(i);
            release(std::move(node));

            Node next;
            CALICODB_TRY(acquire(next_id, false, next));
            CALICODB_TRY(destroy_impl(next));
            CALICODB_TRY(acquire(save_id, false, node));
        }
    }
    if (!node.ref->page_id.is_root()) {
        std::move(guard).cancel();
        return free(node);
    }
    return Status::ok();
}

auto Tree::destroy(Tree &tree) -> Status
{
    Node root;
    CALICODB_TRY(tree.acquire(tree.root(), false, root));
    return tree.destroy_impl(root);
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

            const auto key = Slice(cell.key, std::min<std::size_t>(3, cell.key_size)).to_string();
            add_to_level(data, escape_string(key), level);
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
    level = 0;
    history[0] = {m_tree->root(), 0};
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
        seek_to(lowest, 0);
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
        seek_to(highest, NodeHdr::get_cell_count(highest.hdr()) - 1);
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
        seek_to(node, 0);
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
        seek_to(node, std::max<U32>(1, NodeHdr::get_cell_count(node.hdr())) - 1);
    }
}

auto CursorImpl::seek_to(Node &node, std::size_t index) -> void
{
    CALICODB_EXPECT_EQ(nullptr, m_node.ref);
    CALICODB_EXPECT_TRUE(m_status.is_ok());
    CALICODB_EXPECT_TRUE(node.is_leaf());

    if (index == NodeHdr::get_cell_count(node.hdr()) && !NodeHdr::get_next_id(node.hdr()).is_null()) {
        m_tree->release(std::move(node));
        auto s = m_tree->acquire(NodeHdr::get_next_id(node.hdr()), false, node);
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
        // On success, seek_to() transfers ownership of the internal cursor's page reference
        // to this cursor. Otherwise, Tree::InternalCursor::clear() will release it below.
        seek_to(m_tree->m_c.node(), m_tree->m_c.index());
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

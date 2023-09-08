// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "tree.h"
#include "cursor_impl.h"
#include "encoding.h"
#include "logging.h"
#include "mem.h"
#include "pager.h"
#include "schema.h"
#include "stat.h"
#include "utils.h"

#ifdef CALICODB_TEST
// Used for debug printing the tree structure.
#include <vector>
#endif // CALICODB_TEST

namespace calicodb
{

namespace
{

constexpr uint32_t kCellPtrSize = sizeof(uint16_t);

[[nodiscard]] auto corrupted_page(Id page_id, PointerMap::Type page_type = PointerMap::kEmpty) -> Status
{
    const char *type_name;
    switch (page_type) {
        case PointerMap::kTreeNode:
        case PointerMap::kTreeRoot:
            type_name = "tree node";
            break;
        case PointerMap::kOverflowHead:
        case PointerMap::kOverflowLink:
            type_name = "overflow page";
            break;
        case PointerMap::kFreelistPage:
            type_name = "freelist page";
            break;
        default:
            type_name = "page";
    }
    return StatusBuilder::corruption("corruption detected on %s with ID %u",
                                     type_name, page_id.value);
}

[[nodiscard]] auto cell_slots_offset(const Node &node) -> uint32_t
{
    return page_offset(node.ref->page_id) + NodeHdr::kSize;
}

[[nodiscard]] auto cell_area_offset(const Node &node) -> uint32_t
{
    return cell_slots_offset(node) + NodeHdr::get_cell_count(node.hdr()) * kCellPtrSize;
}

[[nodiscard]] auto read_next_id(const PageRef &page) -> Id
{
    return Id(get_u32(page.data + page_offset(page.page_id)));
}

auto write_next_id(PageRef &page, Id next_id) -> void
{
    put_u32(page.data + page_offset(page.page_id), next_id.value);
}

[[nodiscard]] auto read_child_id(const Cell &cell)
{
    return Id(get_u32(cell.ptr));
}

[[nodiscard]] auto read_overflow_id(const Cell &cell)
{
    return Id(get_u32(cell.key + cell.local_pl_size));
}

auto write_overflow_id(Cell &cell, Id overflow_id)
{
    put_u32(cell.key + cell.local_pl_size, overflow_id.value);
}

auto write_child_id(Cell &cell, Id child_id)
{
    put_u32(cell.ptr, child_id.value);
}

[[nodiscard]] auto merge_root(Node &root, Node &child, uint32_t page_size)
{
    CALICODB_EXPECT_EQ(NodeHdr::get_next_id(root.hdr()), child.ref->page_id);
    if (NodeHdr::get_free_start(child.hdr()) > 0) {
        if (child.defrag()) {
            return -1;
        }
    }

    // Copy the cell content area.
    const auto cell_start = NodeHdr::get_cell_start(child.hdr());
    CALICODB_EXPECT_GE(cell_start, cell_slots_offset(root));
    auto area_size = page_size - cell_start;
    auto *area = root.ref->data + cell_start;
    std::memcpy(area, child.ref->data + cell_start, area_size);

    // Copy the header and cell pointers.
    area_size = NodeHdr::get_cell_count(child.hdr()) * kCellPtrSize;
    area = root.ref->data + cell_slots_offset(root);
    std::memcpy(area, child.ref->data + cell_slots_offset(child), area_size);
    std::memcpy(root.hdr(), child.hdr(), NodeHdr::kSize);
    root.parser = child.parser;
    return 0;
}

[[nodiscard]] auto is_underflowing(const Node &node)
{
    return NodeHdr::get_cell_count(node.hdr()) == 0;
}

constexpr uint32_t kLinkContentOffset = sizeof(uint32_t);

struct PayloadManager {
    static auto access(
        Pager &pager,
        const Cell &cell,   // The `cell` containing the payload being accessed
        uint32_t offset,    // `offset` within the payload being accessed
        uint32_t length,    // Number of bytes to access
        const char *in_buf, // Write buffer of size at least `length` bytes, or nullptr if not a write
        char *out_buf       // Read buffer of size at least `length` bytes, or nullptr if not a read
        ) -> Status
    {
        const auto ovfl_content_max = static_cast<uint32_t>(pager.page_size() - kLinkContentOffset);
        CALICODB_EXPECT_TRUE(in_buf || out_buf);
        if (offset <= cell.local_pl_size) { // TODO: change to "<", no change in outcome but is less confusing
            const auto n = minval(length, cell.local_pl_size - offset);
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
                if (pgno.as_index() >= pager.page_count()) {
                    // Page ID is past the end of the file. Also note that if pgno.is_null(), pager.acquire()
                    // will return a Status::corruption() (this happens if the end of the chain is reached
                    // before `length` reaches 0).
                    return corrupted_page(pgno, PointerMap::kOverflowLink);
                }
                PageRef *ovfl;
                s = pager.acquire(pgno, ovfl);
                if (!s.is_ok()) {
                    break;
                } else if (in_buf) {
                    pager.mark_dirty(*ovfl);
                }
                uint32_t len;
                if (offset >= ovfl_content_max) {
                    offset -= ovfl_content_max;
                    len = 0;
                } else {
                    len = minval(length, ovfl_content_max - offset);
                    if (in_buf) {
                        std::memcpy(ovfl->data + kLinkContentOffset + offset, in_buf, len);
                        in_buf += len;
                    } else {
                        std::memcpy(out_buf, ovfl->data + kLinkContentOffset + offset, len);
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
        if (s.is_ok() && length) {
            return StatusBuilder::corruption("missing %u bytes from overflow record", length);
        }
        return s;
    }
};

auto detach_cell(Cell &cell, char *backing) -> void
{
    CALICODB_EXPECT_NE(backing, nullptr);
    if (cell.ptr != backing) {
        std::memcpy(backing, cell.ptr, cell.footprint);
        const auto diff = cell.key - cell.ptr;
        cell.ptr = backing;
        cell.key = backing + diff;
    }
}

// Determine what the last page number should be after a vacuum operation completes on a database with the
// given number of pages `db_size` and number of freelist (trunk + leaf) pages `free_size`. This computation
// was taken from SQLite (src/btree.c:finalDbSize()).
auto vacuum_end_page(uint32_t page_size, uint32_t db_size, uint32_t free_size) -> Id
{
    // Number of entries that can fit on a pointer map page.
    const auto entries_per_map = page_size / 5;
    // Page ID of the most-recent pointer map page (the page that holds the back pointer for the last page
    // in the database file).
    const auto pm_page = PointerMap::lookup(Id(db_size), page_size);
    // Number of pointer map pages between the current last page and the after-vacuum last page.
    const auto pm_size = (free_size + pm_page.value + entries_per_map - db_size) / entries_per_map;

    auto end_page = Id(db_size - free_size - pm_size);
    end_page.value -= PointerMap::is_map(end_page, page_size);
    return end_page;
}

class InorderTraversal
{
public:
    struct TraversalInfo {
        uint32_t idx;
        uint32_t ncells;
        uint32_t level;
    };

    // Call the callback for every record and pivot in the tree, in sort order, plus once when the node
    // is no longer required for determining the rest of the traversal
    template <class Callback>
    static auto traverse(Tree &tree, const Callback &cb) -> Status
    {
        Node root;
        auto s = tree.acquire(tree.root(), root);
        if (s.is_ok()) {
            s = traverse_impl(tree, move(root), cb, 0);
        }
        return s;
    }

private:
    template <class Callback>
    static auto traverse_impl(Tree &tree, Node node, const Callback &cb, uint32_t level) -> Status
    {
        Status s;
        for (uint32_t i = 0, n = NodeHdr::get_cell_count(node.hdr()); s.is_ok() && i <= n; ++i) {
            if (!node.is_leaf()) {
                const auto save_id = node.ref->page_id;
                const auto next_id = node.read_child_id(i);
                tree.release(move(node));

                Node next;
                s = tree.acquire(next_id, next);
                if (s.is_ok()) {
                    s = traverse_impl(tree, move(next), cb, level + 1);
                }
                if (s.is_ok()) {
                    s = tree.acquire(save_id, node);
                }
            }
            if (s.is_ok()) {
                s = cb(node, TraversalInfo{i, n, level});
            }
        }
        tree.release(move(node));
        return s;
    }
};

[[nodiscard]] constexpr auto is_overflow_type(PointerMap::Type type) -> bool
{
    return type == PointerMap::kOverflowHead ||
           type == PointerMap::kOverflowLink;
}

} // namespace

auto Tree::corrupted_node(Id page_id) const -> Status
{
    auto s = corrupted_page(page_id, PointerMap::kTreeNode);
    if (m_pager->mode() >= Pager::kWrite) {
        // Pager status should never be set unless a rollback is needed.
        m_pager->set_status(s);
    }
    return s;
}

auto Tree::get_tree(CursorImpl &c) -> Tree *
{
    return c.m_tree;
}

auto Tree::save_all_cursors() const -> void
{
    manage_cursors(nullptr, kInitSaveCursors);
}

auto Tree::create(Id *root_id_out) -> Status
{
    // Determine the next root page. This is the lowest-numbered page that is
    // not already a root, and not a pointer map page.
    auto &database_root = m_pager->get_root();
    auto target = FileHdr::get_largest_root(database_root.data);
    for (++target.value; PointerMap::is_map(target, m_page_size);) {
        ++target.value; // Skip pointer map pages.
    }

    PageRef *page;
    // Attempt to allocate the page that needs to become the next root. This
    // is only possible if it is on the freelist, or it is past the end of the
    // database file.
    auto s = allocate(kAllocateExact, target, page);
    if (!s.is_ok()) {
        return s;
    }
    const auto found = page->page_id;
    if (found != target) {
        PointerMap::Entry entry;
        s = PointerMap::read_entry(*m_pager, target, entry);
        if (s.is_ok()) {
            s = relocate_page(page, entry, target);
        }
        m_pager->release(page);
        if (s.is_ok()) {
            s = m_pager->acquire(target, page);
        }
        if (s.is_ok()) {
            m_pager->mark_dirty(*page);
        }
    }

    if (s.is_ok()) {
        auto *hdr = page->data + page_offset(page->page_id);
        std::memset(hdr, 0, NodeHdr::kSize);
        NodeHdr::put_type(hdr, true);
        NodeHdr::put_cell_start(hdr, m_page_size);
        s = PointerMap::write_entry(*m_pager, target,
                                    {Id::null(), PointerMap::kTreeRoot});
    }

    if (s.is_ok()) {
        m_pager->mark_dirty(database_root);
        FileHdr::put_largest_root(database_root.data, target);
        *root_id_out = target;
    }
    m_pager->release(page);
    return s;
}

auto Tree::destroy(Tree &tree, Reroot &rr) -> Status
{
    CALICODB_EXPECT_FALSE(tree.root().is_root());
    rr.after = tree.root();

    // Push all pages belonging to `tree` onto the freelist, except for the root page.
    auto s = InorderTraversal::traverse(
        tree, [&tree](auto &node, const auto &info) {
            if (info.idx == info.ncells) {
                if (node.ref->page_id == tree.root()) {
                    return Status::ok();
                }
                return Freelist::add(*tree.m_pager, node.ref);
            }
            Cell cell;
            if (node.read(info.idx, cell)) {
                return tree.corrupted_node(node.ref->page_id);
            }
            if (cell.local_pl_size < cell.total_pl_size) {
                return tree.free_overflow(read_overflow_id(cell));
            }
            return Status::ok();
        });
    if (!s.is_ok()) {
        return s;
    }

    auto &database_root = m_pager->get_root();
    rr.before = FileHdr::get_largest_root(database_root.data);
    if (rr.before != rr.after) {
        // Replace the destroyed tree's root page with the highest-numbered root page.
        PageRef *unused_page;
        s = m_pager->acquire(rr.after, unused_page);
        if (s.is_ok()) {
            CALICODB_EXPECT_EQ(rr.after, unused_page->page_id);
            const PointerMap::Entry root_info = {Id::null(), PointerMap::kTreeRoot};
            s = relocate_page(unused_page, root_info, rr.before);
            m_pager->release(unused_page);
        }
    }
    if (s.is_ok()) {
        PageRef *largest_root;
        s = m_pager->acquire(rr.before, largest_root);
        if (s.is_ok()) {
            s = Freelist::add(*m_pager, largest_root);
        }
    }
    if (!s.is_ok()) {
        return s;
    }

    // Update the "largest root" file header field.
    auto largest = rr.before;
    for (--largest.value; PointerMap::is_map(largest, m_page_size);) {
        // Skip pointer map pages.
        if (--largest.value == Id::kRoot) {
            break;
        }
    }
    m_pager->mark_dirty(database_root);
    FileHdr::put_largest_root(database_root.data, largest);
    return s;
}

auto Tree::extract_key(Node &node, uint32_t index, KeyScratch &scratch, Slice &key_out, uint32_t limit) const -> Status
{
    Cell cell;
    if (node.read(index, cell)) {
        return corrupted_node(node.ref->page_id);
    }
    return extract_key(cell, scratch, key_out, limit);
}

auto Tree::extract_key(const Cell &cell, KeyScratch &scratch, Slice &key_out, uint32_t limit) const -> Status
{
    if (limit == 0 || limit > cell.key_size) {
        limit = cell.key_size;
    }
    if (limit <= cell.local_pl_size) {
        key_out = Slice(cell.key, limit);
        return Status::ok();
    }
    if (limit > scratch.len) {
        auto *buf = Mem::reallocate(
            scratch.buf, limit);
        if (buf) {
            scratch.buf = static_cast<char *>(buf);
            scratch.len = limit;
        } else {
            return Status::no_memory();
        }
    }
    return read_key(cell, scratch.buf, &key_out, limit);
}

auto Tree::read_key(const Cell &cell, char *scratch, Slice *key_out, uint32_t limit) const -> Status
{
    if (limit == 0 || limit > cell.key_size) {
        limit = cell.key_size;
    }
    auto s = PayloadManager::access(*m_pager, cell, 0, limit, nullptr, scratch);
    if (s.is_ok() && key_out) {
        *key_out = Slice(scratch, limit);
    }
    return s;
}

auto Tree::read_value(Node &node, uint32_t index, char *scratch, Slice *value_out) const -> Status
{
    Cell cell;
    if (node.read(index, cell)) {
        return corrupted_node(node.ref->page_id);
    }
    return read_value(cell, scratch, value_out);
}

auto Tree::read_value(const Cell &cell, char *scratch, Slice *value_out) const -> Status
{
    const auto value_size = cell.total_pl_size - cell.key_size;
    auto s = PayloadManager::access(*m_pager, cell, cell.key_size, value_size, nullptr, scratch);
    if (s.is_ok() && value_out) {
        *value_out = Slice(scratch, value_size);
    }
    return s;
}

auto Tree::overwrite_value(const Cell &cell, const Slice &value) -> Status
{
    return PayloadManager::access(*m_pager, cell, cell.key_size,
                                  static_cast<uint32_t>(value.size()),
                                  value.data(), nullptr);
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

auto Tree::fix_parent_id(Id page_id, Id parent_id, PointerMap::Type type, Status &s) -> void
{
    if (s.is_ok()) {
        s = PointerMap::write_entry(*m_pager, page_id, {parent_id, type});
    }
}

auto Tree::maybe_fix_overflow_chain(const Cell &cell, Id parent_id, Status &s) -> void
{
    if (s.is_ok() && cell.local_pl_size != cell.total_pl_size) {
        fix_parent_id(read_overflow_id(cell), parent_id, PointerMap::kOverflowHead, s);
    }
}

auto Tree::make_pivot(const PivotOptions &opt, Cell &pivot_out) -> Status
{
    Slice keys[2];
    for (size_t i = 0; i < 2; ++i) {
        const auto local_key_size = minval(
            opt.cells[i]->key_size,
            opt.cells[i]->local_pl_size);
        keys[i] = Slice(opt.cells[i]->key, local_key_size);
    }
    if (keys[0] >= keys[1]) {
        // The left key must be less than the right key. If this cannot be seen in the local
        // keys, then 1 of the 2 must be overflowing. The nonlocal part is needed to perform
        // suffix truncation.
        for (size_t i = 0; i < 2; ++i) {
            if (opt.cells[i]->key_size > keys[i].size()) {
                // Read just enough of the key to determine the ordering.
                auto s = extract_key(
                    *opt.cells[i],
                    m_key_scratch[i],
                    keys[i],
                    opt.cells[1 - i]->key_size + 1);
                if (!s.is_ok()) {
                    return s;
                }
            }
        }
    }
    Slice prefix;
    if (truncate_suffix(keys[0], keys[1], prefix)) {
        return Status::corruption();
    }
    pivot_out.ptr = opt.scratch;
    pivot_out.total_pl_size = static_cast<uint32_t>(prefix.size());
    auto *ptr = pivot_out.ptr + sizeof(uint32_t); // Skip the left child ID.
    pivot_out.key = encode_varint(ptr, pivot_out.total_pl_size);
    pivot_out.local_pl_size = compute_local_pl_size(prefix.size(), 0, m_page_size);
    pivot_out.footprint = pivot_out.local_pl_size + uint32_t(pivot_out.key - opt.scratch);
    std::memcpy(pivot_out.key, prefix.data(), pivot_out.local_pl_size);
    prefix.advance(pivot_out.local_pl_size);

    Status s;
    if (!prefix.is_empty()) {
        // The pivot is too long to fit on a single page. Transfer the portion that
        // won't fit to an overflow chain.
        PageRef *prev = nullptr;
        auto dst_type = PointerMap::kOverflowHead;
        auto dst_bptr = opt.parent_id;
        while (s.is_ok() && !prefix.is_empty()) {
            PageRef *dst;
            // Allocate a new overflow page.
            s = allocate(kAllocateAny, dst_bptr, dst);
            if (!s.is_ok()) {
                break;
            }
            const auto copy_size = minval<size_t>(
                prefix.size(), m_page_size - kLinkContentOffset);
            std::memcpy(dst->data + kLinkContentOffset,
                        prefix.data(),
                        copy_size);
            prefix.advance(copy_size);

            if (prev) {
                put_u32(prev->data, dst->page_id.value);
                m_pager->release(prev, Pager::kNoCache);
            } else {
                write_overflow_id(pivot_out, dst->page_id);
            }
            s = PointerMap::write_entry(
                *m_pager, dst->page_id, {dst_bptr, dst_type});

            dst_type = PointerMap::kOverflowLink;
            dst_bptr = dst->page_id;
            prev = dst;
        }
        if (s.is_ok()) {
            CALICODB_EXPECT_NE(nullptr, prev);
            put_u32(prev->data, 0);
            pivot_out.footprint += sizeof(uint32_t);
        }
        m_pager->release(prev, Pager::kNoCache);
    }
    return s;
}

auto Tree::post_pivot(Node &parent, uint32_t idx, Cell &pivot, Id child_id) -> Status
{
    const auto rc = parent.write(idx, pivot);
    if (rc > 0) {
        put_u32(parent.ref->data + rc, child_id.value);
    } else if (rc == 0) {
        CALICODB_EXPECT_FALSE(m_ovfl.exists());
        detach_cell(pivot, m_cell_scratch[0]);
        m_ovfl = {pivot, parent.ref->page_id, idx};
        write_child_id(pivot, child_id);
    } else {
        return corrupted_node(parent.ref->page_id);
    }
    Status s;
    fix_parent_id(child_id, parent.ref->page_id, PointerMap::kTreeNode, s);
    maybe_fix_overflow_chain(pivot, parent.ref->page_id, s);
    return s;
}

auto Tree::insert_cell(Node &node, uint32_t idx, const Cell &cell) -> Status
{
    const auto rc = node.write(idx, cell);
    if (rc < 0) {
        return corrupted_node(node.ref->page_id);
    } else if (rc == 0) {
        CALICODB_EXPECT_FALSE(m_ovfl.exists());
        // NOTE: The overflow cell may need to be detached, if the node it is backed by will be released
        //       before it can be written to another node (without that node itself overflowing).
        m_ovfl = {cell, node.ref->page_id, idx};
    }
    Status s;
    if (!node.is_leaf()) {
        fix_parent_id(
            read_child_id(cell),
            node.ref->page_id,
            PointerMap::kTreeNode,
            s);
    }
    maybe_fix_overflow_chain(cell, node.ref->page_id, s);
    return s;
}

auto Tree::remove_cell(Node &node, uint32_t idx) -> Status
{
    Cell cell;
    if (node.read(idx, cell)) {
        return corrupted_node(node.ref->page_id);
    }
    Status s;
    if (cell.local_pl_size != cell.total_pl_size) {
        s = free_overflow(read_overflow_id(cell));
    }
    if (s.is_ok() && node.erase(idx, cell.footprint)) {
        s = corrupted_node(node.ref->page_id);
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
            s = Freelist::add(*m_pager, page);
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
    Status s;
    for (uint32_t i = 0, n = NodeHdr::get_cell_count(node.hdr()); s.is_ok() && i < n; ++i) {
        Cell cell;
        if (node.read(i, cell)) {
            s = corrupted_node(node.ref->page_id);
        }
        // Fix the back pointer for the head of an overflow chain rooted at `node`.
        maybe_fix_overflow_chain(cell, parent_id, s);
        if (!node.is_leaf()) {
            // Fix the parent pointer for the current child node.
            fix_parent_id(
                read_child_id(cell),
                parent_id,
                PointerMap::kTreeNode,
                s);
        }
    }
    if (!node.is_leaf()) {
        fix_parent_id(
            NodeHdr::get_next_id(node.hdr()),
            parent_id,
            PointerMap::kTreeNode,
            s);
    }
    if (m_ovfl.exists()) {
        maybe_fix_overflow_chain(m_ovfl.cell, parent_id, s);
        if (!node.is_leaf()) {
            fix_parent_id(
                read_child_id(m_ovfl.cell),
                parent_id,
                PointerMap::kTreeNode,
                s);
        }
    }
    return s;
}

auto Tree::resolve_overflow(CursorImpl &c) -> Status
{
    Status s;
    while (s.is_ok() && m_ovfl.exists()) {
        if (c.page_id() == root()) {
            s = split_root(c);
        } else {
            s = split_nonroot(c);
        }
        ++m_stat->counters[Stat::kSMOCount];
    }
    c.reset(s);
    return s;
}

auto Tree::split_root(CursorImpl &c) -> Status
{
    CALICODB_EXPECT_EQ(c.m_level, 0);
    auto &root = c.m_node;
    CALICODB_EXPECT_EQ(Tree::root(), root.ref->page_id);

    PageRef *child_page;
    auto s = allocate(kAllocateAny, root.ref->page_id, child_page);
    if (s.is_ok()) {
        auto child = Node::from_new_page(*child_page, m_page_size, m_node_scratch, root.is_leaf());
        // Copy the cell content area. Preserves the indirection vector values.
        const auto after_root_ivec = cell_area_offset(root);
        std::memcpy(child.ref->data + after_root_ivec,
                    root.ref->data + after_root_ivec,
                    m_page_size - after_root_ivec);

        // Copy the header and cell pointers.
        std::memcpy(child.hdr(), root.hdr(), NodeHdr::kSize);
        std::memcpy(child.ref->data + cell_slots_offset(child),
                    root.ref->data + cell_slots_offset(root),
                    NodeHdr::get_cell_count(root.hdr()) * kCellPtrSize);

        CALICODB_EXPECT_TRUE(m_ovfl.exists());
        child.gap_size = root.gap_size;
        child.usable_space = root.usable_space;
        if (root.ref->page_id.is_root()) {
            child.gap_size += FileHdr::kSize;
            child.usable_space += FileHdr::kSize;
        }

        root = Node::from_new_page(*root.ref, m_page_size, m_node_scratch, false);
        NodeHdr::put_next_id(root.hdr(), child.ref->page_id);

        fix_parent_id(
            child.ref->page_id,
            root.ref->page_id,
            PointerMap::kTreeNode,
            s);
        if (s.is_ok()) {
            s = fix_links(child);
        }

        // Overflow cell is now in the child. m_ovfl.idx stays the same.
        m_ovfl.pid = child.ref->page_id;
        c.assign_child(move(child));
        c.m_idx_path[1] = c.m_idx_path[0];
        c.m_idx_path[0] = 0;
    }
    return s;
}

auto Tree::split_nonroot(CursorImpl &c) -> Status
{
    auto &node = c.m_node;
    CALICODB_EXPECT_TRUE(m_ovfl.exists());
    CALICODB_EXPECT_GT(c.m_level, 0);

    Node left;
    auto &parent = c.m_node_path[c.m_level - 1];
    auto s = allocate(kAllocateAny, c.page_id(), left.ref);
    const auto pivot_idx = c.m_idx_path[c.m_level - 1];

    if (s.is_ok()) {
        left = Node::from_new_page(*left.ref, m_page_size, m_node_scratch, node.is_leaf());
        const auto ncells = NodeHdr::get_cell_count(node.hdr());
        if (m_ovfl.idx >= ncells && c.on_last_node()) {
            return split_nonroot_fast(c, parent, move(left));
        }
        s = redistribute_cells(left, node, parent, pivot_idx);
    }

    release(move(left));
    c.move_to_parent();
    return s;
}

auto Tree::split_nonroot_fast(CursorImpl &c, Node &parent, Node right) -> Status
{
    auto &left = c.m_node;
    CALICODB_EXPECT_TRUE(m_ovfl.exists());
    const auto ovfl = m_ovfl.cell;
    m_ovfl.clear();

    auto s = insert_cell(right, 0, ovfl);
    CALICODB_EXPECT_FALSE(m_ovfl.exists());
    upgrade(parent);

    Cell pivot;
    if (left.is_leaf()) {
        Cell right_cell;
        if (right.read(0, right_cell)) {
            s = corrupted_node(right.ref->page_id);
            goto cleanup;
        }

        Cell left_cell;
        if (left.read(NodeHdr::get_cell_count(left.hdr()) - 1, left_cell)) {
            s = corrupted_node(right.ref->page_id);
            goto cleanup;
        }
        const PivotOptions opt = {
            {&left_cell,
             &right_cell},
            m_cell_scratch[0],
            parent.ref->page_id,
        };
        s = make_pivot(opt, pivot);
    } else {
        auto cell_count = NodeHdr::get_cell_count(left.hdr());
        if (left.read(cell_count - 1, pivot)) {
            s = corrupted_node(left.ref->page_id);
            goto cleanup;
        }
        NodeHdr::put_next_id(right.hdr(), NodeHdr::get_next_id(left.hdr()));
        NodeHdr::put_next_id(left.hdr(), read_child_id(pivot));

        // NOTE: The pivot doesn't need to be detached, since only the child ID is overwritten by erase().
        left.erase(cell_count - 1, pivot.footprint);

        fix_parent_id(NodeHdr::get_next_id(right.hdr()), right.ref->page_id,
                      PointerMap::kTreeNode, s);
        fix_parent_id(NodeHdr::get_next_id(left.hdr()), left.ref->page_id,
                      PointerMap::kTreeNode, s);
    }
    if (s.is_ok()) {
        CALICODB_EXPECT_GT(c.m_level, 0);
        const auto pivot_idx = c.m_idx_path[c.m_level - 1];

        // Post the pivot into the parent node. This call will fix the left's parent pointer.
        s = post_pivot(parent, pivot_idx, pivot, left.ref->page_id);
        if (s.is_ok()) {
            CALICODB_EXPECT_EQ(NodeHdr::get_next_id(parent.hdr()), left.ref->page_id);
            NodeHdr::put_next_id(parent.hdr(), right.ref->page_id);
            fix_parent_id(right.ref->page_id, parent.ref->page_id,
                          PointerMap::kTreeNode, s);
        }
    }

cleanup:
    release(move(right));
    c.move_to_parent();
    return s;
}

auto Tree::resolve_underflow(CursorImpl &c) -> Status
{
    Status s;
    while (c.has_node() && s.is_ok() && is_underflowing(c.m_node)) {
        if (c.page_id() == root()) {
            s = fix_root(c);
            break;
        }
        CALICODB_EXPECT_GT(c.m_level, 0);
        auto &parent = c.m_node_path[c.m_level - 1];
        const auto pivot_idx = c.m_idx_path[c.m_level - 1];
        s = fix_nonroot(c, parent, pivot_idx);
        ++m_stat->counters[Stat::kSMOCount];
    }
    c.reset(s);
    return s;
}

// This routine redistributes cells between two siblings, `left` and `right`, and their `parent`
// One of the two siblings must be empty. This code handles rebalancing after both put() and
// erase() operations. When called from put(), there will be an overflow cell in m_ovfl.cell
// which needs to be put in either `left` or `right`, depending on its index and which cell is
// chosen as the new pivot. When called from erase(), the `left` node may be left totally empty,
// in which case, it should be freed.
auto Tree::redistribute_cells(Node &left, Node &right, Node &parent, uint32_t pivot_idx) -> Status
{
    upgrade(parent);

    PageRef *unused;
    auto s = m_pager->get_unused_page(unused);
    if (!s.is_ok()) {
        return s;
    }
    auto tmp = Node::from_new_page(*unused, m_page_size, m_node_scratch, left.is_leaf());

    Node *p_src, *p_left, *p_right;
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
    const auto src_location = p_src->ref->page_id;
    tmp.ref->page_id = src_location;

    if (!p_src->is_leaf()) {
        // The new node is empty, so only the next pointer from p_src is relevant.
        NodeHdr::put_next_id(tmp.hdr(), NodeHdr::get_next_id(p_src->hdr()));
    }
    CALICODB_EXPECT_EQ(0, NodeHdr::get_cell_count(tmp.hdr()));
    CALICODB_EXPECT_EQ(0, NodeHdr::get_free_start(tmp.hdr()));
    CALICODB_EXPECT_EQ(0, NodeHdr::get_frag_count(tmp.hdr()));

    const auto is_split = m_ovfl.exists();
    const auto cell_count = NodeHdr::get_cell_count(p_src->hdr());
    // split_nonroot_fast() handles this case. If the overflow is on the rightmost position, this
    // code path must never be hit, since it doesn't handle that case in particular. This routine
    // also expects that the child pointer in `parent` at `pivot_idx+1` points to `right` There
    // may not be a pointer to `left` in `parent` yet.
    CALICODB_EXPECT_TRUE(!is_split || p_src == &right);

    // Cells that need to be redistributed, in order.
    Buffer<Cell> cell_buffer;
    if (cell_buffer.realloc(cell_count + 2)) {
        m_pager->release(unused);
        return Status::no_memory();
    }
    auto *cells = cell_buffer.ptr() + 1;
    auto *cell_itr = cells;
    uint32_t right_accum = 0;
    Cell cell;

    for (uint32_t i = 0; i <= cell_count;) {
        if (m_ovfl.exists() && i == m_ovfl.idx) {
            right_accum += m_ovfl.cell.footprint;
            // Move the overflow cell backing to an unused scratch buffer. The `parent` may overflow
            // when the pivot is posted (and this cell may not be the pivot). The new overflow cell
            // will use scratch buffer 0, so this cell cannot be stored there.
            detach_cell(m_ovfl.cell, m_cell_scratch[3]);
            *cell_itr++ = m_ovfl.cell;
            m_ovfl.clear();
            continue;
        } else if (i == cell_count) {
            break;
        }
        if (p_src->read(i++, cell)) {
            return corrupted_node(p_src->ref->page_id);
        }
        right_accum += cell.footprint;
        *cell_itr++ = cell;
    }
    const auto accounted_for = right_accum + p_src->usable_space +
                               page_offset(p_src->ref->page_id) +
                               NodeHdr::kSize + cell_count * kCellPtrSize -
                               (is_split ? cells[m_ovfl.idx].footprint : 0);
    if (m_page_size != accounted_for) {
        return corrupted_node(p_src->ref->page_id);
    }

    CALICODB_EXPECT_FALSE(m_ovfl.exists());
    // The pivot cell from `parent` may need to be added to the redistribution set. If a pivot exists
    // at all, it must be removed. If the `left` node already existed, then there must be a pivot
    // separating `left` and `right` (the cell pointing to `left`).
    if (!is_split) {
        if (parent.read(pivot_idx, cell)) {
            return corrupted_node(parent.ref->page_id);
        }
        if (p_src->is_leaf()) {
            if (cell.local_pl_size != cell.total_pl_size) {
                s = free_overflow(read_overflow_id(cell));
                if (!s.is_ok()) {
                    return s;
                }
            }
        } else {
            detach_cell(cell, m_cell_scratch[1]);
            // cell is from the `parent`, so it already has room for a left child ID (`parent` must
            // be internal).
            write_child_id(cell, NodeHdr::get_next_id(p_left->hdr()));
            right_accum += cell.footprint;
            if (p_src == &left) {
                *cell_itr++ = cell;
            } else {
                --cells;
                *cells = cell;
            }
        }
        parent.erase(pivot_idx, cell.footprint);
    }
    const auto ncells = static_cast<int>(cell_itr - cells);
    CALICODB_EXPECT_GE(ncells, is_split ? 4 : 1);

    auto sep = -1;
    for (uint32_t left_accum = 0; right_accum > p_left->usable_space / 2 &&
                                  right_accum > left_accum &&
                                  2 + sep++ < ncells;) {
        left_accum += cells[sep].footprint;
        right_accum -= cells[sep].footprint;
    }
    sep += sep == 0;

    auto idx = ncells - 1;
    for (; idx > sep; --idx) {
        s = insert_cell(*p_right, 0, cells[idx]);
        CALICODB_EXPECT_FALSE(m_ovfl.exists());
        if (!s.is_ok()) {
            return s;
        }
    }

    CALICODB_EXPECT_TRUE(idx > 0 || idx == -1);
    // Post a pivot to the `parent` which links to p_left. If this connection existed before, we would have erased it
    // when parsing cells earlier.
    if (idx > 0) {
        Cell pivot;
        if (p_src->is_leaf()) {
            ++idx; // Backtrack to the last cell written to p_right.
            const PivotOptions opt = {
                {&cells[idx - 1],
                 &cells[idx]},
                m_cell_scratch[2],
                parent.ref->page_id,
            };
            s = make_pivot(opt, pivot);
            cells[idx] = pivot;

        } else {
            const auto next_id = read_child_id(cells[idx]);
            NodeHdr::put_next_id(p_left->hdr(), next_id);
            fix_parent_id(next_id, p_left->ref->page_id, PointerMap::kTreeNode, s);
        }
        if (s.is_ok()) {
            // Post the pivot. This may cause the `parent` to overflow.
            s = post_pivot(parent, pivot_idx, cells[idx], p_left->ref->page_id);
            --idx;
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
    // Replace *p_src with tmp.
    tmp.ref->page_id = Id::null();
    release(move(*p_src), Pager::kDiscard);
    m_pager->move_page(*tmp.ref, src_location);
    *p_src = move(tmp);

    // Only the parent is allowed to overflow. The caller is expected to rebalance the parent in this case.
    CALICODB_EXPECT_TRUE(!m_ovfl.exists() || m_ovfl.pid == parent.ref->page_id);
    return s;
}

auto Tree::fix_nonroot(CursorImpl &c, Node &parent, uint32_t idx) -> Status
{
    auto &node = c.m_node;
    CALICODB_EXPECT_NE(node.ref->page_id, root());
    CALICODB_EXPECT_TRUE(is_underflowing(node));
    CALICODB_EXPECT_FALSE(m_ovfl.exists());

    Status s;
    Node sibling, *p_left, *p_right;
    if (idx > 0) {
        --idx; // Correct the pivot `idx` to point to p_left.
        s = acquire(parent.read_child_id(idx), sibling, true);
        p_left = &sibling;
        p_right = &node;
    } else {
        s = acquire(parent.read_child_id(idx + 1), sibling, true);
        p_left = &node;
        p_right = &sibling;
    }
    if (s.is_ok()) {
        s = redistribute_cells(*p_left, *p_right, parent, idx);
        // NOTE: If this block isn't hit, then (a) sibling is not acquired, and (b) node will be
        //       released when the cursor is advanced.
        release(move(*p_right));
        if (s.is_ok() && 0 == NodeHdr::get_cell_count(p_left->hdr())) {
            // redistribute_cells() performed a merge.
            s = Freelist::add(*m_pager, p_left->ref);
        } else {
            release(move(*p_left));
        }
    }

    c.move_to_parent();
    if (s.is_ok() && m_ovfl.exists()) {
        // The `parent` may have overflowed when the pivot was posted (if redistribute_cells()
        // performed a rotation).
        s = resolve_overflow(c);
    }
    return s;
}

auto Tree::fix_root(CursorImpl &c) -> Status
{
    auto &node = c.m_node;
    CALICODB_EXPECT_EQ(node.ref->page_id, root());
    if (node.is_leaf()) {
        // The whole tree is empty.
        return Status::ok();
    }

    Node child;
    auto s = acquire(NodeHdr::get_next_id(node.hdr()), child, true);
    if (s.is_ok()) {
        // We don't have enough room to transfer the child contents into the root, due to the space occupied by
        // the file header. In this case, we'll just split the child and insert the median cell into the root.
        // Note that the child needs an overflow cell for the split routine to work. We'll just fake it by
        // extracting an arbitrary cell and making it the overflow cell.
        if (node.ref->page_id.is_root() && child.usable_space < FileHdr::kSize) {
            Cell cell;
            c.m_idx = NodeHdr::get_cell_count(child.hdr()) / 2;
            if (child.read(c.m_idx, cell)) {
                s = corrupted_node(node.ref->page_id);
                release(move(child));
            } else {
                m_ovfl.cell = cell;
                detach_cell(m_ovfl.cell, m_cell_scratch[0]);
                child.erase(c.m_idx, cell.footprint);
                c.assign_child(move(child));
                s = split_nonroot(c);
            }
        } else {
            if (merge_root(node, child, m_page_size)) {
                s = corrupted_node(node.ref->page_id);
                release(move(child));
            } else {
                s = Freelist::add(*m_pager, child.ref);
            }
            if (s.is_ok()) {
                s = fix_links(node);
            }
        }
    }
    return s;
}

Tree::Tree(Pager &pager, Stat &stat, char *scratch, Id root_id, String name)
    : list_entry{Slice(name), this, nullptr, nullptr},
      m_stat(&stat),
      m_node_scratch(scratch + pager.page_size()),
      m_cell_scratch{
          scratch,
          scratch + pager.page_size() / kNumCellBuffers,
          scratch + pager.page_size() / kNumCellBuffers * 2,
          scratch + pager.page_size() / kNumCellBuffers * 3,
      },
      m_name(move(name)),
      m_pager(&pager),
      m_root_id(root_id),
      m_page_size(pager.page_size()),
      m_writable(pager.mode() >= Pager::kWrite)
{
    IntrusiveList::initialize(list_entry);
    IntrusiveList::initialize(m_active_list);
    IntrusiveList::initialize(m_inactive_list);

    // Make sure that cells written to scratch memory won't interfere with each other.
    CALICODB_EXPECT_GT(m_page_size / kNumCellBuffers,
                       compute_local_pl_size(m_page_size, 0, m_page_size) + kMaxCellHeaderSize);
}

Tree::~Tree()
{
    for (const auto &scratch : m_key_scratch) {
        Mem::deallocate(scratch.buf);
    }

    // Make sure all cursors are in the inactive list with their nodes released.
    save_all_cursors();

    // Clear the inactive cursors list, which may contain some cursors that the user
    // hasn't yet called delete on. This makes sure they don't try to remove themselves
    // from m_inactive_list, since the sentinel entry will no longer be valid after this
    // destructor returns.
    while (!IntrusiveList::is_empty(m_inactive_list)) {
        auto *entry = m_inactive_list.next_entry;
        IntrusiveList::remove(*entry);
        IntrusiveList::initialize(*entry);
    }
}

auto Tree::allocate(AllocationType type, Id nearby, PageRef *&page_out) -> Status
{
    auto s = Freelist::remove(*m_pager, static_cast<Freelist::RemoveType>(type),
                              nearby, page_out);
    if (s.is_ok() && page_out == nullptr) {
        // Freelist is empty. Allocate a page from the end of the database file.
        s = m_pager->allocate(page_out);
        if (s.is_ok() && PointerMap::is_map(page_out->page_id, m_page_size)) {
            m_pager->release(page_out);
            s = m_pager->allocate(page_out);
        }
    }
    if (s.is_ok()) {
        if (page_out->refs == 1) {
            m_pager->mark_dirty(*page_out);
        } else {
            m_pager->release(page_out);
            s = Status::corruption();
        }
    }
    return s;
}

auto Tree::put(CursorImpl &c, const Slice &key, const Slice &value) -> Status
{
    if (key.size() > kMaxAllocation) {
        return Status::invalid_argument("key is too long");
    } else if (value.size() > kMaxAllocation) {
        return Status::invalid_argument("value is too long");
    }

    manage_cursors(&c, kInitNormal);
    const auto key_exists = c.seek_to_leaf(
        key, CursorImpl::kSeekWriter);
    auto s = c.m_status;
    if (s.is_ok()) {
        upgrade(c.m_node);
        if (key_exists) {
            Cell cell;
            if (c.m_node.read(c.m_idx, cell)) {
                return corrupted_node(c.m_node.ref->page_id);
            }
            const auto value_length = cell.total_pl_size - cell.key_size;
            if (value_length == value.size()) {
                s = overwrite_value(cell, value);
                goto cleanup;
            }
            s = remove_cell(c.m_node, c.m_idx);
        }
        bool overflow;
        if (s.is_ok()) {
            // Attempt to write a cell representing the `key` and `value` directly to the page.
            // This routine also populates any overflow pages necessary to hold a `key` and/or
            // `value` that won't fit on a single node page. If the cell itself cannot fit in
            // `node`, it will be written to m_cell_scratch[0] instead.
            s = emplace(c.m_node, key, value, c.m_idx, overflow);
        }

        if (s.is_ok() && overflow) {
            // There wasn't enough room for the cell in `node`, so it was built in
            // m_cell_scratch[0] instead.
            Cell ovfl;
            if (c.m_node.parser(m_cell_scratch[0], m_cell_scratch[1], m_page_size, &ovfl)) {
                s = corrupted_node(c.page_id());
            } else {
                CALICODB_EXPECT_FALSE(m_ovfl.exists());
                m_ovfl = {ovfl, c.page_id(), c.m_idx};
                s = resolve_overflow(c);
            }
        }
    }

cleanup:
    if (s.is_ok()) {
        if (c.has_key()) {
            // Cursor is already on the correct record.
            s = c.fetch_user_payload();
        } else {
            // There must have been a SMO. The rebalancing routine clears the cursor,
            // since it is left on an internal node. Seek back to where the record
            // was inserted.
            c.seek(key);
            s = c.status();
        }
    }
    if (c.m_status.is_ok()) {
        c.m_status = s;
    }
    return s;
}

auto Tree::emplace(Node &node, const Slice &key, const Slice &value, uint32_t index, bool &overflow) -> Status
{
    CALICODB_EXPECT_TRUE(node.is_leaf());
    auto k = key.size();
    auto v = value.size();
    const auto local_pl_size = compute_local_pl_size(k, v, m_page_size);
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
    ptr = encode_varint(ptr, static_cast<uint32_t>(value.size()));
    ptr = encode_varint(ptr, static_cast<uint32_t>(key.size()));
    const auto hdr_size = static_cast<uintptr_t>(ptr - header);
    const auto pad_size = hdr_size > kMinCellHeaderSize ? 0 : kMinCellHeaderSize - hdr_size;
    const auto cell_size = local_pl_size + hdr_size + pad_size + sizeof(uint32_t) * has_remote;
    // External cell headers are padded out to 4 bytes.
    std::memset(ptr, 0, pad_size);

    // Attempt to allocate space for the cell in the node. If this is not possible,
    // write the cell to scratch memory. allocate_block() should not return an offset
    // that would interfere with the node header/indirection vector or cause an out-of-
    // bounds write (this only happens if the node is corrupted).
    const auto local_offset = node.alloc(
        index, static_cast<uint32_t>(cell_size));
    if (local_offset > 0) {
        ptr = node.ref->data + local_offset;
        overflow = false;
    } else if (local_offset == 0) {
        ptr = m_cell_scratch[0];
        overflow = true;
    } else {
        return corrupted_node(node.ref->page_id);
    }
    // Write the cell header.
    std::memcpy(ptr, header, hdr_size + pad_size);
    ptr += hdr_size + pad_size;

    PageRef *prev = nullptr;
    auto payload_left = key.size() + value.size();
    auto prev_pgno = node.ref->page_id;
    auto prev_type = PointerMap::kOverflowHead;
    auto *next_ptr = ptr + local_pl_size;
    auto len = local_pl_size;
    auto src = key;

    Status s;
    while (s.is_ok()) {
        const auto n = minval(len, static_cast<uint32_t>(src.size()));
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
            s = allocate(kAllocateAny, node.ref->page_id, ovfl);
            if (s.is_ok()) {
                put_u32(next_ptr, ovfl->page_id.value);
                len = m_page_size - kLinkContentOffset;
                ptr = ovfl->data + sizeof(uint32_t);
                next_ptr = ovfl->data;
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
        put_u32(prev->data, 0);
        m_pager->release(prev, Pager::kNoCache);
    }
    return s;
}

auto Tree::erase(CursorImpl &c, const Slice &key) -> Status
{
    manage_cursors(&c, kInitNormal);
    // This call may invalidate `key`, if `key` was returned by `c->key()`. In
    // this case, seek_to_leaf() with kSeekReader should cause `c->key()` to
    // return the same key bytes that `key` formerly held.
    const auto key_exists = c.seek_to_leaf(
        key, CursorImpl::kSeekReader);
    auto s = c.m_status;
    if (s.is_ok() && key_exists) {
        s = erase(c);
    }
    return s;
}

auto Tree::erase(CursorImpl &c) -> Status
{
    if (!c.m_status.is_ok()) {
        return c.status();
    } else if (!c.is_valid()) {
        return Status::invalid_argument();
    }
    manage_cursors(&c, kInitNormal);

    Slice saved_key;
    if (1 == NodeHdr::get_cell_count(c.m_node.hdr())) {
        // This node will underflow when the record is removed. Make sure the key is saved so that
        // the correct position can be found after underflow resolution. The backing buffer for
        // saved_key will not be freed/realloc'd until after the cursor position is found again.
        saved_key = c.key();
    }
    Status s;
    if (c.m_idx < NodeHdr::get_cell_count(c.m_node.hdr())) {
        upgrade(c.m_node);
        s = remove_cell(c.m_node, c.m_idx);
        if (s.is_ok() && is_underflowing(c.m_node)) {
            s = resolve_underflow(c);
        }
    }
    if (s.is_ok()) {
        if (c.has_node()) {
            c.ensure_correct_leaf(true);
        } else {
            c.seek(saved_key);
        }
        s = c.m_status;
    } else if (c.m_status.is_ok()) {
        c.m_status = s;
    }
    return s;
}

auto Tree::relocate_page(PageRef *&free, PointerMap::Entry entry, Id last_id) -> Status
{
    CALICODB_EXPECT_NE(free->page_id, last_id);

    Status s;
    switch (entry.type) {
        case PointerMap::kOverflowLink:
            // Back pointer points to another overflow chain link, or the head of the chain.
            if (!entry.back_ptr.is_null()) {
                PageRef *parent;
                s = m_pager->acquire(entry.back_ptr, parent);
                if (s.is_ok()) {
                    m_pager->mark_dirty(*parent);
                    write_next_id(*parent, free->page_id);
                    m_pager->release(parent, Pager::kNoCache);
                }
            }
            break;
        case PointerMap::kOverflowHead: {
            // Back pointer points to the node that the overflow chain is rooted in. Search through that node's cells
            // for the target overflowing cell.
            Node parent;
            s = acquire(entry.back_ptr, parent, true);
            if (!s.is_ok()) {
                return s;
            }
            bool found = false;
            for (uint32_t i = 0, n = NodeHdr::get_cell_count(parent.hdr()); i < n; ++i) {
                Cell cell;
                if (parent.read(i, cell)) {
                    s = corrupted_node(parent.ref->page_id);
                    break;
                }
                found = cell.local_pl_size < cell.total_pl_size &&
                        read_overflow_id(cell) == last_id;
                if (found) {
                    write_overflow_id(cell, free->page_id);
                    break;
                }
            }
            const auto page_id = parent.ref->page_id;
            release(move(parent));
            if (s.is_ok() && !found) {
                s = corrupted_node(page_id);
            }
            break;
        }
        case PointerMap::kTreeNode: {
            // Back pointer points to another node, i.e. this is not a root. Search through the
            // parent for the target child pointer and overwrite it with the new page ID.
            Node parent;
            s = acquire(entry.back_ptr, parent, true);
            if (!s.is_ok()) {
                return s;
            } else if (parent.is_leaf()) {
                release(move(parent));
                return corrupted_node(entry.back_ptr);
            }
            bool found = false;
            for (uint32_t i = 0, n = NodeHdr::get_cell_count(parent.hdr()); !found && i <= n; ++i) {
                const auto child_id = parent.read_child_id(i);
                found = child_id == last_id;
                if (found) {
                    parent.write_child_id(i, free->page_id);
                }
            }
            if (!found) {
                s = corrupted_node(parent.ref->page_id);
            }
            release(move(parent));
            [[fallthrough]];
        }
        case PointerMap::kTreeRoot: {
            if (!s.is_ok()) {
                return s;
            }
            // Update references.
            Node last;
            s = acquire(last_id, last, true);
            if (!s.is_ok()) {
                return s;
            }
            s = fix_links(last, free->page_id);
            release(move(last));
            break;
        }
        default:
            return corrupted_node(PointerMap::lookup(last_id, m_page_size));
    }

    if (s.is_ok()) {
        s = PointerMap::write_entry(*m_pager, last_id, {});
    }
    if (s.is_ok()) {
        s = PointerMap::write_entry(*m_pager, free->page_id, entry);
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
                        entry.back_ptr = free->page_id;
                        s = PointerMap::write_entry(*m_pager, next_id, entry);
                    }
                }
            }
            const auto new_location = free->page_id;
            m_pager->release(free, Pager::kDiscard);
            if (s.is_ok()) {
                m_pager->move_page(*last, new_location);
                m_pager->release(last);
            }
        }
    }
    return s;
}

auto Tree::vacuum() -> Status
{
    auto db_size = m_pager->page_count();
    if (db_size == 0) {
        return Status::ok();
    }

    Status s;
    auto &root = m_pager->get_root();

    // Count the number of pages in the freelist, since we don't keep this information stored
    // anywhere. This involves traversing the list of freelist trunk pages. Luckily, these pages
    // are likely to be accessed again soon, so it may not hurt have them in the pager cache.
    const auto free_len = FileHdr::get_freelist_length(root.data);
    // Determine what the last page in the file should be after this vacuum is run to completion.
    const auto end_page = vacuum_end_page(m_page_size, db_size, free_len);
    for (; s.is_ok() && db_size > end_page.value; --db_size) {
        const Id last_page_id(db_size);
        if (!PointerMap::is_map(last_page_id, m_page_size)) {
            PointerMap::Entry entry;
            s = PointerMap::read_entry(*m_pager, last_page_id, entry);
            if (!s.is_ok()) {
                break;
            }
            if (entry.type != PointerMap::kFreelistPage) {
                PageRef *free = nullptr;
                // Find an unused page that will exist after the vacuum. Copy the last occupied
                // page into it. Once there are no more such unoccupied pages, the vacuum is
                // finished and all occupied pages are tightly packed at the start of the file.
                while (s.is_ok()) {
                    s = allocate(kAllocateAny, Id::null(), free);
                    if (s.is_ok()) {
                        if (free->page_id <= end_page) {
                            s = relocate_page(free, entry, last_page_id);
                            m_pager->release(free);
                            break;
                        } else {
                            m_pager->release(free);
                        }
                    }
                }
            }
        }
    }
    if (s.is_ok()) {
        if (db_size != end_page.value) {
            s = Status::corruption("invalid page count");
        } else if (db_size < m_pager->page_count()) {
            m_pager->mark_dirty(root);
            FileHdr::put_freelist_head(root.data, Id::null());
            FileHdr::put_freelist_length(root.data, 0);
            m_pager->set_page_count(db_size);
        }
    }
    return s;
}

#if CALICODB_TEST

#define CHECK_OK(expr)                                           \
    do {                                                         \
        if (const auto check_s = (expr); !check_s.is_ok()) {     \
            std::fprintf(stderr, "error(%s:%d): %s\n",           \
                         __FILE__, __LINE__, check_s.message()); \
            std::abort();                                        \
        }                                                        \
    } while (0)

#define CHECK_TRUE(expr)                                             \
    do {                                                             \
        if (!(expr)) {                                               \
            std::fprintf(stderr, "error(%s:%d): \"%s\" was false\n", \
                         __FILE__, __LINE__, #expr);                 \
            std::abort();                                            \
        }                                                            \
    } while (0)

#define CHECK_EQ(lhs, rhs)                                                   \
    do {                                                                     \
        if ((lhs) != (rhs)) {                                                \
            std::fprintf(stderr, "error(%s:%d): \"" #lhs " != " #rhs "\"\n", \
                         __FILE__, __LINE__);                                \
            std::abort();                                                    \
        }                                                                    \
    } while (0)

class TreePrinter
{
    struct StructuralData {
        std::vector<String> levels;
        std::vector<uint32_t> spaces;
    };

    static auto add_to_level(StructuralData &data, const String &message, uint32_t target) -> void
    {
        // If target is equal to levels.size(), add spaces to all levels.
        CHECK_TRUE(target <= data.levels.size());
        uint32_t i = 0;

        auto s_itr = begin(data.spaces);
        auto L_itr = begin(data.levels);
        while (s_itr != end(data.spaces)) {
            CHECK_TRUE(L_itr != end(data.levels));
            if (i++ == target) {
                // Don't leave trailing spaces. Only add them if there will be more text.
                for (size_t j = 0; j < *s_itr; ++j) {
                    CHECK_EQ(append_strings(*L_itr, " "), 0);
                }
                CHECK_EQ(append_strings(*L_itr, message.c_str()), 0);
                *s_itr = 0;
            } else {
                *s_itr += uint32_t(message.length());
            }
            ++L_itr;
            ++s_itr;
        }
    }

    static auto ensure_level_exists(StructuralData &data, uint32_t level) -> void
    {
        while (level >= data.levels.size()) {
            data.levels.emplace_back();
            data.spaces.emplace_back();
        }
        CHECK_TRUE(data.levels.size() > level);
        CHECK_TRUE(data.levels.size() == data.spaces.size());
    }

public:
    static auto print_structure(Tree &tree, String &repr_out) -> Status
    {
        StructuralData data;
        const auto print = [&data](auto &node, const auto &info) {
            StringBuilder msg;
            if (info.idx == info.ncells) {
                if (node.is_leaf()) {
                    msg.append_format("%u]", info.ncells);
                }
            } else {
                if (info.idx == 0) {
                    msg.append_format("%u:[", node.ref->page_id.value);
                    ensure_level_exists(data, info.level);
                }
                if (!node.is_leaf()) {
                    msg.append('*');
                    if (info.idx + 1 == info.ncells) {
                        msg.append(']');
                    }
                }
            }
            String msg_out;
            CHECK_EQ(msg.build(msg_out), 0);
            add_to_level(data, msg_out, info.level);
            return Status::ok();
        };
        StringBuilder builder;
        auto s = InorderTraversal::traverse(tree, print);
        if (s.is_ok()) {
            for (const auto &level : data.levels) {
                builder.append_format("%s\n", level.c_str());
            }
        }
        CHECK_EQ(builder.build(repr_out), 0);
        return s;
    }

    static auto print_nodes(Tree &tree, String &repr_out) -> Status
    {
        const auto print = [&tree, &repr_out](auto &node, const auto &info) {
            if (info.idx == info.ncells) {
                StringBuilder msg;
                msg.append_format("%sternalNode(%u)\n", node.is_leaf() ? "Ex" : "In",
                                  node.ref->page_id.value);
                for (uint32_t i = 0; i < info.ncells; ++i) {
                    Cell cell;
                    if (node.read(i, cell)) {
                        return tree.corrupted_node(node.ref->page_id);
                    }
                    msg.append("  Cell(");
                    if (!node.is_leaf()) {
                        msg.append_format("%u,", read_child_id(cell).value);
                    }
                    const auto key_len = minval(32U, minval(cell.key_size, cell.local_pl_size));
                    msg.append('"');
                    msg.append_escaped(Slice(cell.key, key_len));
                    msg.append('"');
                    if (cell.key_size > key_len) {
                        msg.append_format(" + <%zu bytes>", cell.key_size - key_len);
                    }
                    msg.append(")\n");
                }
                String msg_out;
                CHECK_EQ(msg.build(msg_out), 0);
                CHECK_EQ(append_strings(repr_out, msg_out.c_str()), 0);
            }
            return Status::ok();
        };
        return InorderTraversal::traverse(tree, print);
    }
};

class TreeValidator
{
    template <class PageCallback>
    static auto traverse_chain(Pager &pager, PageRef *page, const PageCallback &cb) -> void
    {
        for (;;) {
            cb(page);

            const auto next_id = read_next_id(*page);
            pager.release(page);
            if (next_id.is_null()) {
                break;
            }
            CHECK_OK(pager.acquire(next_id, page));
        }
    }

    [[nodiscard]] static auto get_readable_content(const PageRef &page, uint32_t page_size, uint32_t size_limit) -> Slice
    {
        return Slice(page.data, page_size).range(kLinkContentOffset, minval(size_limit, page_size - kLinkContentOffset));
    }

public:
    static auto validate(Tree &tree) -> void
    {
        CHECK_OK(InorderTraversal::traverse(tree, [&tree](auto &node, const auto &info) {
            auto check_parent_child = [&tree](auto &node, auto index) -> void {
                Node child;
                CHECK_OK(tree.acquire(node.read_child_id(index), child, false));

                Id parent_id;
                CHECK_OK(tree.find_parent_id(child.ref->page_id, parent_id));
                CHECK_TRUE(parent_id == node.ref->page_id);

                tree.release(move(child));
            };
            if (info.idx == info.ncells) {
                return Status::ok();
            }

            if (!node.is_leaf()) {
                check_parent_child(node, info.idx);
                // Rightmost child.
                if (info.idx + 1 == info.ncells) {
                    check_parent_child(node, info.idx + 1);
                }
            }
            return Status::ok();
        }));

        CHECK_OK(InorderTraversal::traverse(tree, [&tree](auto &node, const auto &info) {
            if (info.idx == info.ncells) {
                CHECK_TRUE(node.assert_state());
                return Status::ok();
            }
            Cell cell;
            CHECK_EQ(0, node.read(info.idx, cell));

            auto accumulated = cell.local_pl_size;
            auto requested = cell.key_size;
            if (node.is_leaf()) {
                uint32_t value_size = 0;
                CHECK_TRUE(decode_varint(cell.ptr, node.ref->data + tree.m_page_size, value_size));
                requested += value_size;
            }

            if (cell.local_pl_size != cell.total_pl_size) {
                const auto overflow_id = read_overflow_id(cell);
                PageRef *head;
                CHECK_OK(tree.m_pager->acquire(overflow_id, head));
                traverse_chain(*tree.m_pager, head, [&](auto *page) {
                    CHECK_TRUE(requested > accumulated);
                    accumulated += static_cast<uint32_t>(get_readable_content(
                                                             *page,
                                                             tree.m_page_size,
                                                             requested - accumulated)
                                                             .size());
                });
                CHECK_EQ(requested, accumulated);
            }
            return Status::ok();
        }));
    }
};

auto Tree::TEST_validate() -> void
{
    TreeValidator::validate(*this);
}

auto Tree::print_structure(String &repr_out) -> Status
{
    return TreePrinter::print_structure(*this, repr_out);
}

auto Tree::print_nodes(String &repr_out) -> Status
{
    return TreePrinter::print_nodes(*this, repr_out);
}

#undef CHECK_TRUE
#undef CHECK_EQ
#undef CHECK_OK

#else

auto Tree::TEST_validate() -> void
{
}

#endif // CALICODB_TEST

auto Tree::manage_cursors(Cursor *c, CursorAction type) const -> void
{
    CALICODB_EXPECT_TRUE(c || type == kInitSaveCursors);
    if (m_writable || type == kInitSaveCursors) {
        // Clear the active cursor list.
        auto *entry = m_active_list.next_entry;
        while (entry != &m_active_list) {
            auto *ptr = entry;
            entry = ptr->next_entry;
            // Skip saving the target cursor `c`, since it may already be on the correct
            // node, and it is about to be used.
            if (c != ptr->cursor) {
                if (ptr->cursor->is_valid()) {
                    ptr->cursor->save_position();
                } else {
                    // Don't get rid of the cursor status: it may contain error info.
                    ptr->cursor->reset(ptr->cursor->m_status);
                }
                IntrusiveList::remove(*ptr);
                IntrusiveList::add_head(*ptr, m_inactive_list);
            }
        }
    }
    if (auto *impl = reinterpret_cast<CursorImpl *>(c)) {
        // Initialize the target cursor.
        impl->ensure_position_loaded();
        IntrusiveList::remove(impl->m_list_entry);
        IntrusiveList::add_head(impl->m_list_entry, m_active_list);
        impl->m_saved = false;
    }
}

} // namespace calicodb

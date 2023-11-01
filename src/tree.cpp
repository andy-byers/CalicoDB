// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "tree.h"
#include "encoding.h"
#include "internal.h"
#include "logging.h"
#include "pager.h"
#include "schema.h"
#include "status_internal.h"

#ifdef CALICODB_TEST
// Used for debug printing the tree structure.
#include <vector>
#endif // CALICODB_TEST

namespace calicodb
{

namespace
{

constexpr uint32_t kCellPtrSize = sizeof(uint16_t);

[[nodiscard]] auto corrupted_page(Id page_id, PageType page_type = kInvalidPage) -> Status
{
    return StatusBuilder::corruption("corruption detected on %s with ID %u",
                                     page_type_name(page_type), page_id.value);
}

[[nodiscard]] auto ivec_offset(Id page_id, bool is_leaf) -> uint32_t
{
    return page_offset(page_id) + NodeHdr::size(is_leaf);
}

[[nodiscard]] auto cell_area_offset(const Node &node) -> uint32_t
{
    return ivec_offset(node.page_id(), node.is_leaf()) + node.cell_count() * kCellPtrSize;
}

[[nodiscard]] auto read_next_id(const PageRef &page) -> Id
{
    return Id(get_u32(page.data + page_offset(page.page_id)));
}

void write_next_id(PageRef &page, Id next_id)
{
    put_u32(page.data + page_offset(page.page_id), next_id.value);
}

[[nodiscard]] auto read_child_id(const Cell &cell)
{
    return Id(get_u32(cell.ptr));
}

[[nodiscard]] auto read_overflow_id(const Cell &cell)
{
    return Id(get_u32(cell.key + cell.local_size));
}

auto write_overflow_id(Cell &cell, Id overflow_id)
{
    put_u32(cell.key + cell.local_size, overflow_id.value);
}

auto write_child_id(Cell &cell, Id child_id)
{
    put_u32(cell.ptr, child_id.value);
}

[[nodiscard]] auto merge_root(Node &root, Node &child, uint32_t page_size)
{
    CALICODB_EXPECT_EQ(NodeHdr::get_next_id(root.hdr()), child.page_id());
    if (NodeHdr::get_free_start(child.hdr()) > 0) {
        if (child.defrag()) {
            return -1;
        }
    }

    // Copy the cell content area.
    const auto cell_start = NodeHdr::get_cell_start(child.hdr());
    CALICODB_EXPECT_GE(cell_start, ivec_offset(root.page_id(), child.is_leaf()));
    auto area_size = page_size - cell_start;
    auto *area = root.ref->data + cell_start;
    std::memcpy(area, child.ref->data + cell_start, area_size);

    // Copy the header and cell pointers.
    area_size = child.cell_count() * kCellPtrSize;
    area = root.ref->data + ivec_offset(root.page_id(), child.is_leaf());
    std::memcpy(area, child.ref->data + ivec_offset(child.page_id(), child.is_leaf()), area_size);
    std::memcpy(root.hdr(), child.hdr(), NodeHdr::size(child.is_leaf()));

    // Transfer/recompute metadata.
    const auto size_difference = root.page_id().is_root() ? FileHdr::kSize : 0U;
    root.usable_space = child.usable_space - size_difference;
    root.gap_size = child.gap_size - size_difference;
    root.min_local = child.min_local;
    root.max_local = child.max_local;
    root.scratch = child.scratch;
    root.parser = child.parser;
    return 0;
}

[[nodiscard]] auto is_underflowing(const Node &node)
{
    return node.cell_count() == 0;
}

constexpr uint32_t kLinkContentOffset = sizeof(uint32_t);

struct PayloadManager {
    PayloadManager() = delete;

    static auto compare(Pager &pager, const Slice &key, const Cell &cell, int &cmp_out) -> Status
    {
        auto rest = key;
        PageRef *page = nullptr;
        auto remaining = cell.key_size;
        Slice rhs(cell.key, minval(remaining, cell.local_size));
        for (int i = 0;; ++i) {
            const auto lhs = rest.range(0, minval(rest.size(), rhs.size()));
            cmp_out = lhs.compare(rhs);
            remaining -= static_cast<uint32_t>(rhs.size());
            rest.advance(lhs.size());
            if (cmp_out) {
                break;
            } else if (rest.is_empty()) {
                cmp_out = remaining ? -1 : 0;
                break;
            } else if (remaining == 0) {
                cmp_out = 1;
                break;
            }
            // Get the next part of the record key to compare.
            const auto next_id = i ? read_next_id(*page)
                                   : read_overflow_id(cell);
            pager.release(page);
            auto s = pager.acquire(next_id, page);
            if (!s.is_ok()) {
                return s;
            }
            rhs = Slice(page->data + kLinkContentOffset,
                        pager.page_size() - kLinkContentOffset);
            rhs.truncate(minval<size_t>(remaining, rhs.size()));
        }
        pager.release(page);
        return Status::ok();
    }

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
        if (offset < cell.local_size) {
            const auto n = minval(length, cell.local_size - offset);
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
            offset -= cell.local_size;
        }

        Status s;
        if (length) {
            auto pgno = read_overflow_id(cell);
            while (!pgno.is_null()) {
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

void detach_cell(Cell &cell, char *backing)
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
        for (uint32_t i = 0, n = node.cell_count(); s.is_ok() && i <= n; ++i) {
            if (!node.is_leaf()) {
                const auto save_id = node.page_id();
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

} // namespace

auto TreeCursor::read_user_key() -> Status
{
    CALICODB_EXPECT_TRUE(has_valid_position(true));
    m_key.clear();
    if (m_tree->m_writable || m_cell.key_size > m_cell.local_size) {
        if (m_key_buf.size() < m_cell.key_size) {
            if (m_key_buf.realloc(m_cell.key_size)) {
                return Status::no_memory();
            }
        }
        if (m_cell.key_size) {
            return m_tree->read_key(m_cell, m_key_buf.data(), &m_key);
        }
    } else {
        m_key = Slice(m_cell.key, m_cell.key_size);
    }
    return Status::ok();
}

auto TreeCursor::read_user_value() -> Status
{
    CALICODB_EXPECT_TRUE(has_valid_position(true));
    m_value.clear();
    if (m_cell.is_bucket) {
        return Status::ok();
    }
    const auto value_size = m_cell.total_size - m_cell.key_size;
    if (m_tree->m_writable || m_cell.total_size > m_cell.local_size) {
        if (m_value_buf.size() < value_size) {
            if (m_value_buf.realloc(value_size)) {
                return Status::no_memory();
            }
        }
        if (value_size) {
            return m_tree->read_value(m_cell, m_value_buf.data(), &m_value);
        }
    } else {
        m_value = Slice(m_cell.key + m_cell.key_size, value_size);
    }
    return Status::ok();
}

void TreeCursor::read_record()
{
    CALICODB_EXPECT_NE(m_state, kSaved);
    if (!has_valid_position(true)) {
        return;
    }
    CALICODB_EXPECT_TRUE(m_node.is_leaf());
    auto s = read_user_key();
    if (s.is_ok()) {
        s = read_user_value();
    }
    if (s.is_ok()) {
        m_state = kHasRecord;
    } else {
        reset(s);
    }
}

void TreeCursor::read_current_cell()
{
    CALICODB_EXPECT_NE(m_state, kSaved);
    CALICODB_EXPECT_TRUE(has_valid_position());
    CALICODB_EXPECT_TRUE(m_node.is_leaf());
    CALICODB_EXPECT_LT(m_idx, m_node.cell_count());
    if (m_node.read(m_idx, m_cell)) {
        reset(m_tree->corrupted_node(page_id()));
    }
}

auto TreeCursor::ensure_position_loaded(bool *changed_type_out) -> bool
{
    if (m_state == kSaved) {
        CALICODB_EXPECT_TRUE(m_tree->m_writable);
        // The only way a cursor can be saved is if save_position() is called while the cursor has
        // m_state equal to kHasRecord. When m_state == kHasRecord, m_key references the internal
        // key buffer (unless the key has 0 length). If m_key were to reference memory on a page,
        // it would be invalidated once we start the traversal in seek_to_leaf().
        CALICODB_EXPECT_TRUE(m_key.is_empty() || m_key.data() == m_key_buf.data());
        const auto was_bucket = m_cell.is_bucket;
        // Seek the cursor back to where it was before.
        if (seek_to_leaf(m_key)) {
            // Record with key m_key was found. Make sure it is still the same type of record. We
            // don't consider the record value.
            if (changed_type_out) {
                *changed_type_out = was_bucket != m_cell.is_bucket;
            }
        } else {
            return true;
        }
    }
    return false;
}

void TreeCursor::ensure_correct_leaf()
{
    if (has_valid_position()) {
        CALICODB_EXPECT_TRUE(m_node.is_leaf());
        if (m_idx == m_node.cell_count()) {
            move_right();
        }
    }
}

void TreeCursor::move_right()
{
    CALICODB_EXPECT_TRUE(has_valid_position()); // May be one past the end
    CALICODB_EXPECT_TRUE(m_node.is_leaf());
    const auto leaf_level = m_level;
    m_state = kFloating;
    for (uint32_t adjust = 0;; adjust = 1) {
        const auto ncells = m_node.cell_count();
        if (++m_idx < ncells + adjust) {
            break;
        } else if (m_level == 0) {
            // Hit the rightmost end of the tree. Invalidate the cursor. This catches
            // the case where the whole tree is empty.
            reset();
            return;
        }
        move_to_parent(false);
    }
    while (!m_node.is_leaf()) {
        move_to_child(m_node.read_child_id(m_idx));
        if (!m_status.is_ok()) {
            return;
        }
        m_idx = 0;
    }
    if (m_level == leaf_level) {
        read_current_cell();
    } else {
        reset(Status::corruption());
    }
}

void TreeCursor::move_left()
{
    CALICODB_EXPECT_TRUE(has_valid_position(true));
    CALICODB_EXPECT_TRUE(m_node.is_leaf());
    const auto leaf_level = m_level;
    m_state = kFloating;
    for (;;) {
        if (m_idx > 0) {
            --m_idx;
            break;
        } else if (m_level == 0) {
            // Hit the leftmost end of the tree.
            reset();
            return;
        }
        move_to_parent(false);
    }
    while (!m_node.is_leaf()) {
        move_to_child(m_node.read_child_id(m_idx));
        if (!m_status.is_ok()) {
            return;
        }
        m_idx = m_node.cell_count() - m_node.is_leaf();
    }
    if (m_level == leaf_level) {
        read_current_cell();
    } else {
        reset(Status::corruption());
    }
}

void TreeCursor::seek_to_root()
{
    reset();
    if (m_tree->m_pager->page_count()) {
        m_status = m_tree->acquire(m_tree->root(), m_node);
    }
}

void TreeCursor::seek_to_last_leaf()
{
    seek_to_root();
    if (!has_valid_position(true)) {
        // The whole tree is empty.
        return;
    }
    do {
        m_idx = m_node.cell_count();
        if (m_node.is_leaf()) {
            CALICODB_EXPECT_GT(m_idx, 0);
            --m_idx;
            read_current_cell();
            break;
        }
        move_to_child(NodeHdr::get_next_id(m_node.hdr()));
    } while (m_status.is_ok());
}

auto TreeCursor::search_node(const Slice &key) -> bool
{
    CALICODB_EXPECT_TRUE(m_status.is_ok());
    CALICODB_EXPECT_NE(m_node.ref, nullptr);

    auto exact = false;
    auto upper = m_node.cell_count();
    uint32_t lower = 0;

    while (lower < upper) {
        Cell cell;
        const auto index = (lower + upper) / 2;
        if (m_node.read(index, cell)) {
            reset(Status::corruption());
            return false;
        }
        int cmp;
        auto s = PayloadManager::compare(*m_tree->m_pager, key, cell, cmp);
        if (!s.is_ok()) {
            reset(s);
            return false;
        }
        if (cmp < 0) {
            m_cell = cell;
            upper = index;
        } else if (cmp > 0) {
            lower = index + 1;
        } else {
            m_cell = cell;
            lower = index;
            exact = true;
            break;
        }
    }
    m_idx = lower;
    return exact;
}

TreeCursor::TreeCursor(Tree &tree)
    : m_list_entry{this, nullptr, nullptr},
      m_tree(&tree),
      m_status(tree.m_pager->status())
{
    IntrusiveList::add_head(m_list_entry, tree.m_inactive_list);
}

TreeCursor::~TreeCursor()
{
    if (!IntrusiveList::is_empty(m_list_entry)) {
        IntrusiveList::remove(m_list_entry);
        reset();
    }
}

void TreeCursor::move_to_parent(bool preserve_path)
{
    CALICODB_EXPECT_GT(m_level, 0);
    if (preserve_path) {
        m_node_path[m_level] = move(m_node);
        m_idx_path[m_level] = m_idx;
    }
    release_nodes(kCurrentLevel);
    --m_level;
    m_idx = m_idx_path[m_level];
    m_node = move(m_node_path[m_level]);
}

void TreeCursor::assign_child(Node child)
{
    CALICODB_EXPECT_TRUE(has_valid_position());
    m_idx_path[m_level] = m_idx;
    m_node_path[m_level] = move(m_node);
    m_node = move(child);
    ++m_level;
}

void TreeCursor::move_to_child(Id child_id)
{
    CALICODB_EXPECT_TRUE(has_valid_position());
    if (m_level < static_cast<int>(kMaxDepth - 1)) {
        Node child;
        auto s = m_tree->acquire(child_id, child);
        if (s.is_ok()) {
            assign_child(move(child));
        } else {
            reset(s);
        }
    } else {
        reset(m_tree->corrupted_node(child_id));
    }
}

auto TreeCursor::on_last_node() const -> bool
{
    CALICODB_EXPECT_TRUE(has_valid_position());
    for (int i = 0; i < m_level; ++i) {
        const auto &node = m_node_path[i];
        if (m_idx_path[i] < node.cell_count()) {
            return false;
        }
    }
    return true;
}

void TreeCursor::reset(const Status &s)
{
    release_nodes(kAllLevels);
    m_state = kFloating;
    m_status = s;
    m_level = 0;
    m_idx = 0;
}

auto TreeCursor::start_write(const Slice &key) -> bool
{
    // Save other cursors open on m_tree. This does not invalidate slices obtained from those
    // cursors.
    m_tree->deactivate_cursors(this);
    activate(false);

    // Seek to where the given key is, if it exists, or should go, if it does not. Don't read
    // the record where the cursor ends up: the payload slices being written might have come
    // from the internal buffers.
    const auto result = seek_to_leaf(key);
    if (has_valid_position()) {
        m_tree->upgrade(m_node);
    }
    return result;
}

auto TreeCursor::start_write() -> Status
{
    bool changed_types;
    m_tree->deactivate_cursors(this);
    const auto moved = activate(true, &changed_types); // Load saved position.
    const auto can_write = !moved && !changed_types;
    if (can_write) {
        CALICODB_EXPECT_TRUE(has_valid_position());
        m_tree->upgrade(m_node);
    } else if (moved) {
        return Status::invalid_argument("record was erased");
    } else if (changed_types) {
        return Status::incompatible_value();
    }
    return Status::ok();
}

void TreeCursor::finish_write(Status &s)
{
    if (!s.is_ok()) {
        reset(s);
        return;
    }
    CALICODB_EXPECT_TRUE(has_valid_position());
    m_idx_path[m_level] = m_idx;
    m_node_path[m_level] = move(m_node);
    while (!m_node_path[m_level].is_leaf()) {
        ++m_level;
    }
    m_node = move(m_node_path[m_level]);
    m_idx = m_idx_path[m_level];

    if (m_idx < m_node.cell_count()) {
        read_current_cell();
    } else {
        // Cursor might have been placed "one past the end" in a leaf node after a
        // record was erased. Move to the first record in the right sibling.
        ensure_correct_leaf();
    }
}

auto TreeCursor::seek_to_leaf(const Slice &key) -> bool
{
    auto on_correct_node = false;
    if (has_valid_position(true)) {
        CALICODB_EXPECT_TRUE(m_node.is_leaf());
        if (m_cell.key_size <= m_cell.local_size) {
            const Slice record_key(m_cell.key, m_cell.key_size);
            const auto ordering = record_key.compare(key);
            if (ordering < 0) {
                on_correct_node = on_last_node();
            } else if (ordering == 0) {
                return true;
            }
        }
    }
    m_state = kFloating;
    if (!on_correct_node) {
        seek_to_root();
    }
    while (has_valid_position()) {
        const auto found_exact_key = search_node(key);
        if (m_status.is_ok()) {
            if (m_node.is_leaf()) {
                return found_exact_key;
            }
            m_idx += found_exact_key;
            move_to_child(m_node.read_child_id(m_idx));
        }
    }
    reset(m_status);
    return false;
}

void TreeCursor::release_nodes(ReleaseType type)
{
    m_tree->release(move(m_node));
    if (type < kAllLevels) {
        return;
    }
    for (auto &node : m_node_path) {
        m_tree->release(move(node));
    }
}

auto TreeCursor::key() const -> Slice
{
    CALICODB_EXPECT_TRUE(is_valid());
    return m_key;
}

auto TreeCursor::value() const -> Slice
{
    CALICODB_EXPECT_TRUE(is_valid());
    return m_value;
}

auto TreeCursor::assert_state() const -> bool
{
#ifndef NDEBUG
    if (m_state == kHasRecord) {
        CALICODB_EXPECT_TRUE(has_valid_position(true));
    } else if (m_state == kSaved) {
        CALICODB_EXPECT_EQ(m_node.ref, nullptr);
    }
    if (has_valid_position(true)) {
        CALICODB_EXPECT_TRUE(m_node.is_leaf());
        for (size_t i = 0; i < ARRAY_SIZE(m_node_path); ++i) {
            if (m_node_path[i].ref == nullptr) {
                continue;
            }
            CALICODB_EXPECT_LE(m_idx_path[i], NodeHdr::get_cell_count(m_node_path[i].hdr()));

            [[maybe_unused]] Node node;
            CALICODB_EXPECT_EQ(0, Node::from_existing_page(
                                      m_tree->node_options,
                                      *m_node_path[i].ref,
                                      node));
            CALICODB_EXPECT_EQ(node.gap_size, m_node_path[i].gap_size);
            CALICODB_EXPECT_EQ(node.usable_space, m_node_path[i].usable_space);
            CALICODB_EXPECT_EQ(node.read_child_id(m_idx_path[i]),
                               m_node_path[i + 1].ref ? m_node_path[i + 1].page_id() : m_node.page_id());
        }
    }
#endif // NDEBUG
    return true;
}

auto Tree::corrupted_node(Id page_id) const -> Status
{
    return corrupted_page(page_id, page_id == root() ? kTreeRoot : kTreeNode);
}

auto Tree::create(Id parent_id, Id &root_id_out) -> Status
{
    // Determine the next root page. This is the lowest-numbered page that is
    // not already a root, and not a pointer map page.
    auto &database_root = m_pager->get_root();
    auto target = FileHdr::get_largest_root(database_root.data);
    for (++target.value; PointerMap::is_map(target, page_size);) {
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
        Node::from_new_page(Node::Options(page_size, nullptr), *page, true);
        fix_parent_id(target, parent_id, kTreeRoot, s);
    }

    if (s.is_ok()) {
        m_pager->mark_dirty(database_root);
        FileHdr::put_largest_root(database_root.data, target);
        root_id_out = target;
    }
    m_pager->release(page);
    return s;
}

auto Tree::destroy(Reroot &rr, Vector<Id> &children) -> Status
{
    if (m_root_id.is_root()) {
        return Status::corruption();
    }
    rr.after = m_root_id;
    const auto push_child = [&children](const auto &cell) {
        return children.push_back(read_bucket_root_id(cell));
    };

    // Push all pages belonging to `tree` onto the freelist, except for the root page.
    // Add nested buckets to the list of children. They will be freed in a subsequent
    // call to destroy().
    auto s = InorderTraversal::traverse(
        *this, [this, &push_child](auto &node, const auto &info) {
            if (info.idx == info.ncells) {
                if (node.page_id() == m_root_id) {
                    return Status::ok();
                }
                return Freelist::add(*m_pager, node.ref);
            }
            Cell cell;
            if (node.read(info.idx, cell)) {
                return corrupted_node(node.page_id());
            }
            if (cell.local_size < cell.total_size) {
                return free_overflow(read_overflow_id(cell));
            }
            return cell.is_bucket && push_child(cell) ? Status::no_memory()
                                                      : Status::ok();
        });

    if (!s.is_ok()) {
        return s;
    }

    auto &database_root = m_pager->get_root();
    rr.before = FileHdr::get_largest_root(database_root.data);
    if (rr.before != rr.after) {
        // Replace the destroyed tree's root page with the highest-numbered root page.
        PageRef *unused_page;
        PointerMap::Entry root_info;
        s = m_pager->acquire(rr.after, unused_page);
        if (s.is_ok()) {
            CALICODB_EXPECT_EQ(rr.after, unused_page->page_id);
            s = find_parent_id(rr.before, root_info.back_ptr);
            root_info.type = kTreeRoot;
        }
        if (s.is_ok()) {
            s = relocate_page(unused_page, root_info, rr.before);
        }
        m_pager->release(unused_page);
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
    for (--largest.value; PointerMap::is_map(largest, page_size);) {
        // Skip pointer map pages.
        if (--largest.value == Id::kRoot) {
            break;
        }
    }
    m_pager->mark_dirty(database_root);
    FileHdr::put_largest_root(database_root.data, largest);
    return s;
}

auto Tree::read_key(const Cell &cell, char *scratch, Slice *key_out, uint32_t limit) const -> Status
{
    if (limit == 0 || limit > cell.key_size) {
        limit = cell.key_size;
    }
    auto s = PayloadManager::access(*m_pager, cell, 0, limit, nullptr, scratch);
    if (key_out) {
        *key_out = s.is_ok() ? Slice(scratch, limit) : "";
    }
    return s;
}

auto Tree::read_value(const Cell &cell, char *scratch, Slice *value_out) const -> Status
{
    const auto value_size = cell.total_size - cell.key_size;
    auto s = PayloadManager::access(*m_pager, cell, cell.key_size, value_size, nullptr, scratch);
    if (value_out) {
        *value_out = s.is_ok() ? Slice(scratch, value_size) : "";
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

void Tree::fix_parent_id(Id page_id, Id parent_id, PageType type, Status &s)
{
    PointerMap::write_entry(*m_pager, page_id, {parent_id, type}, s);
}

void Tree::maybe_fix_overflow_chain(const Cell &cell, Id parent_id, Status &s)
{
    if (s.is_ok() && cell.local_size != cell.total_size) {
        fix_parent_id(read_overflow_id(cell), parent_id, kOverflowHead, s);
    }
}

auto Tree::make_pivot(const PivotOptions &opt, Cell &pivot_out) -> Status
{
    const auto ovfl_local = m_pager->page_size() - kLinkContentOffset;
    const auto [cells, parent, scratch] = opt;

    struct {
        Cell cell;
        Slice chunk;
        PageRef *page;
        uint32_t total;
    } items[2] = {};

    // The keys are processed in multiple chunks, if they are overflowing. The keys need to be
    // tracked separately. If one is a bucket record and the other is not, then the cells will have
    // different local key sizes. Note that we generally only need to consider enough of each key
    // to determine an ordering between the two records. If the first key is a prefix of the second,
    // then we will require an extra char from the second key to distinguish between the 2 child key
    // ranges. For example, if the left key is "abc" and the right key is "abccc", we need to store
    // "abcc" as the pivot.
    items[0].cell = *cells[0];
    items[1].cell = *cells[1];
    items[0].total = minval(cells[0]->key_size, cells[1]->key_size);
    items[1].total = minval(cells[0]->key_size + 1, cells[1]->key_size);
    items[0].chunk = Slice(cells[0]->key, minval(items[0].total, cells[0]->local_size));
    items[1].chunk = Slice(cells[1]->key, minval(items[1].total, cells[1]->local_size));
    const auto original = items[1].total;

    pivot_out.key = scratch + sizeof(uint32_t) + kVarintMaxLength;
    pivot_out.is_bucket = false;

    auto target_local = compute_local_size(m_pager->page_size(), 0, parent->min_local, parent->max_local);
    auto target_bptr = opt.parent->page_id();
    auto target_type = kOverflowHead;
    auto *target = pivot_out.key;

    Status s;
    Id overflow_id;
    PageRef *target_page = nullptr;
    PageRef *target_prev = nullptr;
    for (;;) {
        Slice prefix;
        const auto max_prefix_size = minval<size_t>(target_local, items[0].chunk.size(), items[1].chunk.size());
        const auto lhs = items[0].chunk.range(0, max_prefix_size);
        const auto rhs = items[1].chunk.range(0, max_prefix_size);
        if (max_prefix_size) {
            if (truncate_suffix(lhs, rhs, prefix)) {
                s = Status::corruption("keys are out of order");
                break;
            }
            const auto prefix_size = prefix.size();
            std::memcpy(target, prefix.data(), prefix_size);
            items[0].chunk.advance(prefix_size);
            items[1].chunk.advance(prefix_size);
            items[0].total -= static_cast<uint32_t>(prefix_size);
            items[1].total -= static_cast<uint32_t>(prefix_size);
            target_local -= static_cast<uint32_t>(prefix_size);
            target += prefix_size;
        } else {
            // The left key is a prefix of the right key.
            CALICODB_EXPECT_TRUE(items[0].chunk.is_empty());
            CALICODB_EXPECT_EQ(items[1].chunk.size(), 1);
            target[0] = items[1].chunk[0];
            --items[1].total;
            break;
        }

        const auto last = prefix.size() - 1;
        if (lhs.size() != rhs.size() || lhs[last] != rhs[last]) {
            // Stop early if the pivot key has enough information to distinguish between the left
            // and right key ranges. This is clearly the case when truncate_suffix() has performed
            // suffix truncation. Also, catch the case where the keys are the same length and differ
            // only at the last character.
            break;
        }
        if (items[0].total + items[1].total == 0) {
            // The left and right keys are exhausted. We have to continue until both are finished,
            // otherwise we miss the case where the left key is a prefix of the right key (we need
            // an extra char from the right key).
            break;
        }
        for (auto &[cell, chunk, page, total] : items) {
            if (!chunk.is_empty() || !total) {
                continue;
            }
            const auto next_id = page ? read_next_id(*page)
                                      : read_overflow_id(cell);
            m_pager->release(page, Pager::kNoCache);
            s = m_pager->acquire(next_id, page);
            if (!s.is_ok()) {
                goto cleanup; // Break out of nested loop
            }
            chunk = Slice(page->data + kLinkContentOffset,
                          minval<size_t>(ovfl_local, total));
        }

        if (target_local == 0) {
            s = allocate(kAllocateAny, opt.parent->page_id(), target_page);
            if (!s.is_ok()) {
                break;
            }
            if (target_prev) {
                write_next_id(*target_prev, target_page->page_id);
                m_pager->release(target_prev, Pager::kNoCache);
            } else {
                // Overflow ID is written once we know where to put it. Requires knowing the
                // local key size.
                overflow_id = target_page->page_id;
            }
            fix_parent_id(target_page->page_id, target_bptr, target_type, s);
            target = target_page->data + kLinkContentOffset;
            target_local = ovfl_local;
            target_type = kOverflowLink;
            target_bptr = target_page->page_id;
            target_prev = target_page;
        }
    }

cleanup:
    if (s.is_ok()) {
        const auto prefix_size = original - items[1].total;
        const auto varint_size = varint_length(prefix_size);
        auto *varint_ptr = pivot_out.key - varint_size;
        encode_varint(varint_ptr, prefix_size);
        pivot_out.ptr = varint_ptr - sizeof(uint32_t);
        pivot_out.key_size = prefix_size;
        pivot_out.total_size = prefix_size;
        pivot_out.local_size = compute_local_size(prefix_size, 0, parent->min_local, parent->max_local);
        pivot_out.footprint = static_cast<uint32_t>(pivot_out.key - pivot_out.ptr) + pivot_out.local_size;

        if (target_prev) {
            CALICODB_EXPECT_NE(nullptr, target_prev);
            write_overflow_id(pivot_out, overflow_id);
            pivot_out.footprint += sizeof(uint32_t);
            put_u32(target_prev->data, 0);
        }
    }
    m_pager->release(target_prev, Pager::kNoCache);
    m_pager->release(items[0].page, Pager::kNoCache);
    m_pager->release(items[1].page, Pager::kNoCache);
    return s;
}

auto Tree::post_pivot(Node &parent, uint32_t idx, Cell &pivot, Id child_id) -> Status
{
    Status s;
    const auto rc = parent.insert(idx, pivot);
    if (rc > 0) {
        put_u32(parent.ref->data + rc, child_id.value);
    } else if (rc == 0) {
        CALICODB_EXPECT_FALSE(m_ovfl.exists());
        detach_cell(pivot, m_cell_scratch[0]);
        m_ovfl = {pivot, parent.page_id(), idx};
        write_child_id(pivot, child_id);
    } else {
        s = corrupted_node(parent.page_id());
    }
    fix_parent_id(child_id, parent.page_id(), kTreeNode, s);
    maybe_fix_overflow_chain(pivot, parent.page_id(), s);
    return s;
}

auto Tree::insert_cell(Node &node, uint32_t idx, const Cell &cell) -> Status
{
    Status s;
    const auto rc = node.insert(idx, cell);
    if (rc < 0) {
        return corrupted_node(node.page_id());
    } else if (rc == 0) {
        CALICODB_EXPECT_FALSE(m_ovfl.exists());
        // NOTE: The overflow cell may need to be detached, if the node it is backed by will be released
        //       before it can be written to another node (without that node itself overflowing).
        m_ovfl = {cell, node.page_id(), idx};
    }
    fix_cell(cell, node.is_leaf(), node.page_id(), s);
    return s;
}

auto Tree::remove_cell(Node &node, uint32_t idx) -> Status
{
    Cell cell;
    if (node.read(idx, cell)) {
        return corrupted_node(node.page_id());
    }
    Status s;
    if (cell.local_size != cell.total_size) {
        s = free_overflow(read_overflow_id(cell));
    }
    if (s.is_ok() && node.erase(idx, cell.footprint)) {
        s = corrupted_node(node.page_id());
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

void Tree::fix_cell(const Cell &cell, bool is_leaf, Id parent_id, Status &s)
{
    if (!s.is_ok()) {
        return;
    }
    if (!is_leaf) {
        fix_parent_id(read_child_id(cell), parent_id, kTreeNode, s);
    } else if (cell.is_bucket) {
        fix_parent_id(read_bucket_root_id(cell), parent_id, kTreeRoot, s);
    }
    maybe_fix_overflow_chain(cell, parent_id, s);
}

// It is assumed that the children of and/or overflow chains rooted at `node` have incorrect
// parent pointers. This routine fixes them using the pointer map. Using a pointer map is
// vital here: it allows us to access way fewer pages when updating the parent pointers
// (usually just a few as opposed to the number of children in `node` which can be large).
// Also updates back pointers for nested bucket root pages.
auto Tree::fix_links(Node &node, Id parent_id) -> Status
{
    if (parent_id.is_null()) {
        parent_id = node.page_id();
    }
    Status s;
    auto fix_ovfl_cell = m_ovfl.exists();
    for (uint32_t i = 0, n = node.cell_count(); s.is_ok() && i < n;) {
        Cell cell;
        if (fix_ovfl_cell) {
            cell = m_ovfl.cell;
            fix_ovfl_cell = false;
        } else if (node.read(i++, cell)) {
            s = corrupted_node(node.page_id());
            break;
        }
        fix_cell(cell, node.is_leaf(), parent_id, s);
    }
    if (!node.is_leaf()) {
        fix_parent_id(NodeHdr::get_next_id(node.hdr()), parent_id, kTreeNode, s);
    }
    return s;
}

auto Tree::resolve_overflow(TreeCursor &c) -> Status
{
    Status s;
    do {
        if (c.page_id() == root()) {
            s = split_root(c);
        } else {
            s = split_nonroot(c);
        }
        if (!s.is_ok()) {
            break;
        }
        ++m_stat->tree_smo;
    } while (m_ovfl.exists());
    return s;
}

auto Tree::split_root(TreeCursor &c) -> Status
{
    CALICODB_EXPECT_EQ(c.m_level, 0);
    auto &root = c.m_node;
    CALICODB_EXPECT_EQ(Tree::root(), root.page_id());

    PageRef *child_page;
    auto s = allocate(kAllocateAny, root.page_id(), child_page);
    if (s.is_ok()) {
        auto child = Node::from_new_page(node_options, *child_page, root.is_leaf());
        // Copy the cell content area. Preserves the indirection vector values.
        const auto after_root_ivec = cell_area_offset(root);
        std::memcpy(child.ref->data + after_root_ivec,
                    root.ref->data + after_root_ivec,
                    page_size - after_root_ivec);

        // Copy the header and cell pointers.
        std::memcpy(child.hdr(), root.hdr(), NodeHdr::size(root.is_leaf()));
        std::memcpy(child.ref->data + ivec_offset(child.page_id(), root.is_leaf()),
                    root.ref->data + ivec_offset(root.page_id(), root.is_leaf()),
                    root.cell_count() * kCellPtrSize);

        CALICODB_EXPECT_TRUE(m_ovfl.exists());
        child.gap_size = root.gap_size;
        child.usable_space = root.usable_space;
        if (root.page_id().is_root()) {
            child.gap_size += FileHdr::kSize;
            child.usable_space += FileHdr::kSize;
        }

        root = Node::from_new_page(node_options, *root.ref, false);
        NodeHdr::put_next_id(root.hdr(), child.page_id());

        s = fix_links(child);
        fix_parent_id(child.page_id(), root.page_id(), kTreeNode, s);

        // Overflow cell is now in the child. m_ovfl.idx stays the same.
        m_ovfl.pid = child.page_id();
        c.handle_split_root(move(child));
    }
    return s;
}

auto Tree::split_nonroot(TreeCursor &c) -> Status
{
    auto &node = c.m_node;
    CALICODB_EXPECT_TRUE(m_ovfl.exists());
    CALICODB_EXPECT_GT(c.m_level, 0);

    Node left;
    auto &parent = c.m_node_path[c.m_level - 1];
    auto s = allocate(kAllocateAny, parent.page_id(), left.ref);
    const auto pivot_idx = c.m_idx_path[c.m_level - 1];

    if (s.is_ok()) {
        left = Node::from_new_page(node_options, *left.ref, node.is_leaf());
        const auto ncells = node.cell_count();
        if (m_ovfl.idx >= ncells && c.on_last_node()) {
            return split_nonroot_fast(c, parent, move(left));
        }
        s = redistribute_cells(left, node, parent, pivot_idx);
    }
    if (s.is_ok()) {
        // Correct the cursor position. It may not be correct after the split.
        const auto before = left.cell_count() + !left.is_leaf();
        if (c.m_idx < before) {
            auto temp = exchange(node, move(left));
            left = move(temp);
        } else {
            ++c.m_idx_path[c.m_level - 1];
            c.m_idx -= before;
        }
    }

    release(move(left));
    c.move_to_parent(true);
    return s;
}

auto Tree::split_nonroot_fast(TreeCursor &c, Node &parent, Node right) -> Status
{
    auto &left = c.m_node;
    CALICODB_EXPECT_TRUE(m_ovfl.exists());
    const auto ovfl = m_ovfl.cell;
    m_ovfl.clear();

    auto s = insert_cell(right, 0, ovfl);
    CALICODB_EXPECT_FALSE(m_ovfl.exists());
    fix_parent_id(right.page_id(), parent.page_id(), kTreeNode, s);

    Cell pivot;
    upgrade(parent);
    if (left.is_leaf()) {
        Cell left_cell;
        if (left.read(left.cell_count() - 1, left_cell)) {
            s = corrupted_node(right.page_id());
            goto cleanup;
        }
        // ovfl may be in m_cell_scratch[0], use m_cell_scratch[1].
        const PivotOptions opt = {
            {&left_cell,
             &ovfl},
            &parent,
            m_cell_scratch[1],
        };
        s = make_pivot(opt, pivot);
    } else {
        auto cell_count = left.cell_count();
        if (left.read(cell_count - 1, pivot)) {
            s = corrupted_node(left.page_id());
            goto cleanup;
        }
        NodeHdr::put_next_id(right.hdr(), NodeHdr::get_next_id(left.hdr()));
        NodeHdr::put_next_id(left.hdr(), read_child_id(pivot));

        // NOTE: The pivot doesn't need to be detached, since only the child ID is overwritten by erase().
        left.erase(cell_count - 1, pivot.footprint);

        fix_parent_id(NodeHdr::get_next_id(right.hdr()), right.page_id(), kTreeNode, s);
        fix_parent_id(NodeHdr::get_next_id(left.hdr()), left.page_id(), kTreeNode, s);
    }
    if (s.is_ok()) {
        CALICODB_EXPECT_GT(c.m_level, 0);
        const auto pivot_idx = c.m_idx_path[c.m_level - 1];

        // Post the pivot into the parent node. This call will fix left's parent pointer.
        s = post_pivot(parent, pivot_idx, pivot, left.page_id());
        if (s.is_ok()) {
            CALICODB_EXPECT_EQ(NodeHdr::get_next_id(parent.hdr()), left.page_id());
            NodeHdr::put_next_id(parent.hdr(), right.page_id());
        }
    }

cleanup:
    const auto before =
        left.cell_count() + // # cells moved to left
        !left.is_leaf();    // 1 pivot cell posted to parent
    if (c.m_idx >= before) {
        ++c.m_idx_path[c.m_level - 1];
        c.m_idx -= before;
        release(move(left));
        c.m_node = move(right);
    } else {
        release(move(right));
    }
    c.move_to_parent(true);
    return s;
}

auto Tree::resolve_underflow(TreeCursor &c) -> Status
{
    Status s;
    while (s.is_ok() && is_underflowing(c.m_node)) {
        if (c.page_id() == root()) {
            s = fix_root(c);
            break;
        }
        CALICODB_EXPECT_GT(c.m_level, 0);
        auto &parent = c.m_node_path[c.m_level - 1];
        const auto pivot_idx = c.m_idx_path[c.m_level - 1];
        s = fix_nonroot(c, parent, pivot_idx);
        ++m_stat->tree_smo;
    }
    return s;
}

// This routine redistributes cells between two siblings, `left` and `right`, and their `parent`
// One of the two siblings must be empty. This code handles rebalancing after both put() and
// erase() operations. When called from put(), there will be an overflow cell in m_ovfl.cell
// which needs to be put in either `left` or `right`, depending on its index and which cell is
// chosen as the new pivot.
auto Tree::redistribute_cells(Node &left, Node &right, Node &parent, uint32_t pivot_idx) -> Status
{
    upgrade(parent);

    PageRef *unused;
    auto s = m_pager->get_unused_page(unused);
    if (!s.is_ok()) {
        return s;
    }
    auto tmp = Node::from_new_page(node_options, *unused, left.is_leaf());
    const auto merge_threshold = tmp.usable_space;
    const auto is_leaf_level = tmp.is_leaf();

    Node *p_src, *p_left, *p_right;
    if (0 < left.cell_count()) {
        CALICODB_EXPECT_EQ(0, right.cell_count());
        p_src = &left;
        p_left = &tmp;
        p_right = &right;
    } else {
        CALICODB_EXPECT_LT(0, right.cell_count());
        p_src = &right;
        p_left = &left;
        p_right = &tmp;
    }
    const auto src_location = p_src->page_id();
    tmp.ref->page_id = src_location;

    if (!is_leaf_level) {
        // The new node is empty, so only the next pointer from p_src is relevant.
        NodeHdr::put_next_id(tmp.hdr(), NodeHdr::get_next_id(p_src->hdr()));
    }
    CALICODB_EXPECT_EQ(0, tmp.cell_count());
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
    Vector<Cell> cell_buffer;
    if (cell_buffer.reserve(cell_count + 2)) {
        m_pager->release(unused);
        return Status::no_memory();
    }
    int ncells, idx;
    int sep = -1;
    auto *cells = cell_buffer.data() + 1;
    auto *cell_itr = cells;
    uint32_t left_accum = 0;
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
            s = corrupted_node(p_src->page_id());
            goto cleanup;
        }
        right_accum += cell.footprint;
        *cell_itr++ = cell;
    }
    if (page_size != right_accum + p_src->usable_space +
                         page_offset(p_src->page_id()) +
                         NodeHdr::size(is_leaf_level) + cell_count * kCellPtrSize -
                         (is_split ? cells[m_ovfl.idx].footprint : 0)) {
        s = corrupted_node(p_src->page_id());
        goto cleanup;
    }
    CALICODB_EXPECT_FALSE(m_ovfl.exists());
    // The pivot cell from `parent` may need to be added to the redistribution set. If a pivot exists
    // at all, it must be removed. If the `left` node already existed, then there must be a pivot
    // separating `left` and `right` (the cell pointing to `left`). If this is a split, then `left`
    // is a freshly-allocated node (doesn't exist in the tree, so there is no pivot to consider).
    if (!is_split) {
        if (parent.read(pivot_idx, cell)) {
            s = corrupted_node(parent.page_id());
            goto cleanup;
        }
        if (is_leaf_level) {
            if (cell.local_size != cell.total_size) {
                s = free_overflow(read_overflow_id(cell));
                if (!s.is_ok()) {
                    goto cleanup;
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
    ncells = static_cast<int>(cell_itr - cells);
    right_accum += static_cast<uint32_t>(ncells) * kCellPtrSize;

    // Determine if this operation is to be a split, a rotation, or a merge. If is_split is true, then
    // we have no choice but to split. If p_left and p_right are branch nodes, then sep indicates the
    // index of the pivot cell in the cell array. Otherwise, it is the index of the last cell moved to
    // p_left.
    if (is_split || right_accum > merge_threshold) {
        // Determine sep, the pivot index. sep must be placed such that neither p_left, nor p_right,
        // are overflowing. There will always be such an index. Cell sizes are limited so that it
        // only requires 1 additional page to rebalance an overflowing node, even in the worst case.
        do {
            ++sep;
            left_accum += cells[sep].footprint + kCellPtrSize;
            right_accum -= cells[sep].footprint + kCellPtrSize;
            if (left_accum > merge_threshold) {
                // Left node will overflow if the children are leaves. If the children are branches,
                // the cell that p_left has no room for will be posted to the parent instead.
                sep -= is_leaf_level;
                break;
            } else if (right_accum == 0) {
                // This can happen if the rightmost cell is larger than all other cells combined, and
                // this method is called right after a call to split_root() that splits the database
                // root page. The space occupied by the file header can prevent left_accum from
                // exceeding the merge_threshold.
                CALICODB_EXPECT_GT(left_accum, 0);
                --sep;
            }
        } while (left_accum < right_accum);
    }

    idx = ncells - 1;
    for (; idx > sep; --idx) {
        s = insert_cell(*p_right, 0, cells[idx]);
        CALICODB_EXPECT_FALSE(m_ovfl.exists());
        if (!s.is_ok()) {
            goto cleanup;
        }
    }

    // Post a pivot to the `parent` which links to p_left. If this connection existed before, we would have erased it
    // when parsing cells earlier.
    if (idx + is_leaf_level > 0) {
        Cell pivot;
        if (is_leaf_level) {
            ++idx; // Backtrack to the last cell written to p_right.
            const PivotOptions opt = {
                {&cells[idx - 1],
                 &cells[idx]},
                &parent,
                m_cell_scratch[2],
            };
            s = make_pivot(opt, pivot);
            cells[idx] = pivot;

        } else {
            const auto next_id = read_child_id(cells[idx]);
            NodeHdr::put_next_id(p_left->hdr(), next_id);
            fix_parent_id(next_id, p_left->page_id(), kTreeNode, s);
        }
        if (s.is_ok()) {
            // Post the pivot. This may cause the `parent` to overflow.
            s = post_pivot(parent, pivot_idx, cells[idx], p_left->page_id());
            --idx;
        }
    }
    if (!s.is_ok()) {
        goto cleanup;
    }

    // Write the rest of the cells to p_left.
    for (; idx >= 0; --idx) {
        s = insert_cell(*p_left, 0, cells[idx]);
        if (!s.is_ok()) {
            goto cleanup;
        }
    }
    // Replace *p_src with tmp.
    tmp.ref->page_id = Id::null();
    release(move(*p_src), Pager::kDiscard);
    m_pager->move_page(*tmp.ref, src_location);
    *p_src = move(tmp);
    unused = nullptr;

    // Only the parent is allowed to overflow. Must be rebalanced by the caller.
    CALICODB_EXPECT_TRUE(!m_ovfl.exists() || m_ovfl.pid == parent.page_id());

cleanup:
    m_pager->release(unused, Pager::kDiscard);
    return s;
}

auto Tree::fix_nonroot(TreeCursor &c, Node &parent, uint32_t idx) -> Status
{
    auto current = move(c.m_node);
    CALICODB_EXPECT_NE(current.page_id(), root());
    CALICODB_EXPECT_TRUE(is_underflowing(current));
    CALICODB_EXPECT_FALSE(m_ovfl.exists());

    Status s;
    Node sibling, *p_left, *p_right;
    if (idx > 0) {
        --idx; // Correct the pivot `idx` to point to p_left.
        s = acquire(parent.read_child_id(idx), sibling, true);
        p_left = &sibling;
        p_right = &current;
    } else {
        s = acquire(parent.read_child_id(idx + 1), sibling, true);
        p_left = &current;
        p_right = &sibling;
    }
    if (s.is_ok()) {
        // NOTE: p_right is filled up first. If there are not enough cells in the sibling node,
        //       then p_left will be empty after this call.
        s = redistribute_cells(*p_left, *p_right, parent, idx);
        if (s.is_ok()) {
            const auto was_left = p_left == &current;
            // Fix the cursor history path based on what happened in redistribute_cells().
            if (NodeHdr::get_cell_count(p_left->hdr()) == 0) {
                // There was a merge.
                c.m_node = move(*p_right);
                s = Freelist::add(*m_pager, p_left->ref);
                // The parent lost a cell, so correct the path if needed.
                auto &parent_index = c.m_idx_path[c.m_level - 1];
                parent_index -= !was_left;
            } else {
                // There was a rotation.
                c.m_node = move(current);
            }
            if (was_left) {
                c.m_idx = 0;
            } else {
                c.m_idx = c.m_node.cell_count();
            }
        }
    }

    release(move(current));
    release(move(sibling));
    c.move_to_parent(true);
    if (s.is_ok() && m_ovfl.exists()) {
        // The `parent` may have overflowed when the pivot was posted (if redistribute_cells()
        // performed a rotation).
        s = resolve_overflow(c);
    }
    return s;
}

auto Tree::fix_root(TreeCursor &c) -> Status
{
    CALICODB_EXPECT_EQ(c.page_id(), root());
    CALICODB_EXPECT_EQ(c.m_level, 0);
    CALICODB_EXPECT_EQ(c.m_idx, 0);
    if (c.m_node.is_leaf()) {
        // The whole tree is empty.
        return Status::ok();
    }

    Status s;
    auto child = move(c.m_node_path[1]);
    if (c.page_id().is_root() && child.usable_space < FileHdr::kSize) {
        // There is not enough room for the child contents to be moved to the root node. Split the
        // child instead.
        Cell cell;
        const auto path_loc = c.m_idx_path[1];
        const auto cell_count = child.cell_count();
        const auto split_loc = minval(path_loc, cell_count - 1);
        if (child.read(split_loc, cell)) {
            s = corrupted_node(c.page_id());
            release(move(child));
        } else {
            // Note that it is possible for path_loc != split_loc.
            detach_cell(cell, m_cell_scratch[0]);
            child.erase(split_loc, cell.footprint);
            c.assign_child(move(child));
            c.m_idx = path_loc;
            m_ovfl = {cell, c.page_id(), split_loc};
            s = split_nonroot(c);
        }
    } else {
        if (merge_root(c.m_node, child, page_size)) {
            s = corrupted_node(c.page_id());
            release(move(child));
        } else {
            s = Freelist::add(*m_pager, child.ref);
        }
        if (s.is_ok()) {
            s = fix_links(c.m_node);
        }
        c.handle_merge_root();
    }
    return s;
}

Tree::Tree(Pager &pager, Stats &stat, Id root_id)
    : list_entry{this, nullptr, nullptr},
      page_size(pager.page_size()),
      node_options(page_size, pager.scratch() + page_size * 2),
      m_stat(&stat),
      m_cell_scratch{
          pager.scratch(),
          pager.scratch() + page_size / 2,
          pager.scratch() + page_size,
          pager.scratch() + page_size * 3 / 2,
      },
      m_pager(&pager),
      m_root_id(root_id),
      m_writable(pager.mode() >= Pager::kWrite)
{
    IntrusiveList::initialize(list_entry);
    IntrusiveList::initialize(m_active_list);
    IntrusiveList::initialize(m_inactive_list);
}

Tree::~Tree()
{
    // BucketImpl makes sure there is only 1 reference to this tree before calling
    // delete (m_refcount is decremented beforehand, so it should be 0).
    CALICODB_EXPECT_EQ(m_refcount, 0);

    // Make sure all cursors are in the inactive list with their nodes released.
    deactivate_cursors(nullptr);

    // Clear the inactive cursors list, which may contain some cursors that the user
    // hasn't yet called delete on. This makes sure they don't try to remove themselves
    // from m_inactive_list, since the sentinel entry will no longer be valid after this
    // destructor returns.
    while (!IntrusiveList::is_empty(m_inactive_list)) {
        auto *entry = m_inactive_list.next_entry;
        entry->cursor->reset(); // Invalidate
        IntrusiveList::remove(*entry);
        IntrusiveList::initialize(*entry);
    }

    // Remove this tree from the list of open trees.
    IntrusiveList::remove(list_entry);
}

auto Tree::allocate(AllocationType type, Id nearby, PageRef *&page_out) -> Status
{
    auto s = Freelist::remove(*m_pager, static_cast<Freelist::RemoveType>(type),
                              nearby, page_out);
    if (s.is_ok() && page_out == nullptr) {
        // Freelist is empty. Allocate a page from the end of the database file.
        s = m_pager->allocate(page_out);
        if (s.is_ok() && PointerMap::is_map(page_out->page_id, page_size)) {
            m_pager->release(page_out);
            s = m_pager->allocate(page_out);
        }
    }
    if (s.is_ok() && page_out->refs != 1) {
        m_pager->release(page_out);
        s = Status::corruption();
    }
    return s;
}

auto Tree::insert(TreeCursor &c, const Slice &key, const Slice &value, bool is_bucket) -> Status
{
    const auto key_exists = c.start_write(key);
    auto s = c.status();
    if (s.is_ok()) {
        CALICODB_EXPECT_TRUE(c.has_valid_position());
        CALICODB_EXPECT_TRUE(c.assert_state());
        s = write_record(c, key, value, is_bucket, key_exists);
    }
    c.finish_write(s);
    return s;
}

auto Tree::modify(TreeCursor &c, const Slice &value) -> Status
{
    Status s;
    CALICODB_EXPECT_TRUE(c.is_valid());
    if (c.is_bucket()) {
        s = Status::incompatible_value();
    } else {
        s = c.start_write();
    }
    if (s.is_ok()) {
        CALICODB_EXPECT_TRUE(c.has_valid_position(true));
        CALICODB_EXPECT_TRUE(c.assert_state());
        s = write_record(c, c.key(), value, false, true);
    }
    c.finish_write(s);
    return s;
}

auto Tree::write_record(TreeCursor &c, const Slice &key, const Slice &value, bool is_bucket, bool overwrite) -> Status
{
    if (key.size() > kMaxAllocation) {
        return Status::invalid_argument("key is too long");
    } else if (value.size() > kMaxAllocation) {
        return Status::invalid_argument("value is too long");
    }

    Status s;
    CALICODB_EXPECT_TRUE(c.assert_state());
    if (overwrite) {
        CALICODB_EXPECT_FALSE(is_bucket);
        if (c.is_bucket()) {
            return Status::incompatible_value();
        }
        const auto value_size = c.m_cell.total_size - c.m_cell.key_size;
        if (value_size == value.size()) {
            return overwrite_value(c.m_cell, value);
        }
        s = remove_cell(c.m_node, c.m_idx);
    }
    bool overflow;
    if (s.is_ok()) {
        // Attempt to write a cell representing the `key` and `value` directly to the page.
        // This routine also populates any overflow pages necessary to hold a `key` and/or
        // `value` that won't fit on a single node page. If the cell itself cannot fit in
        // `node`, it will be written to m_cell_scratch[0] instead.
        s = emplace(c.m_node, key, value, is_bucket, c.m_idx, overflow);
    }

    if (s.is_ok() && overflow) {
        // There wasn't enough room for the cell in `node`, so it was built in
        // m_cell_scratch[0] instead.
        Cell ovfl;
        if (c.m_node.parser(m_cell_scratch[0], m_cell_scratch[1],
                            c.m_node.min_local, c.m_node.max_local, ovfl)) {
            s = corrupted_node(c.page_id());
        } else {
            CALICODB_EXPECT_FALSE(m_ovfl.exists());
            m_ovfl = {ovfl, c.page_id(), c.m_idx};
            s = resolve_overflow(c);
        }
    }
    return s;
}

auto Tree::emplace(Node &node, Slice key, Slice value, bool flag, uint32_t index, bool &overflow) -> Status
{
    CALICODB_EXPECT_TRUE(node.is_leaf());
    // Serialize the cell header for the external cell and determine the number
    // of bytes needed for the cell.
    char header[kMaxCellHeaderSize];
    const auto key_size = static_cast<uint32_t>(key.size());
    const auto value_size = static_cast<uint32_t>(value.size());
    const auto [k, v, o] = describe_leaf_payload(key_size, value_size, flag,
                                                 node.min_local, node.max_local);
    Id bucket_root_id;
    char *ptr;
    if (flag) {
        ptr = prepare_bucket_cell_hdr(header, key_size);
        write_bucket_root_id(ptr, value);
        bucket_root_id.value = get_u32(value);
        value.clear();
    } else {
        ptr = encode_leaf_record_cell_hdr(header, key_size, value_size);
    }
    const auto hdr_size = static_cast<uintptr_t>(ptr - header);
    const auto cell_size = hdr_size + k + v + o;

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
        return corrupted_node(node.page_id());
    }
    // Write the cell header.
    std::memcpy(ptr, header, hdr_size);
    ptr += hdr_size;

    auto src = key;
    auto len = k + v;
    auto payload_left = key.size() + value.size();
    auto prev_pgno = node.page_id();
    auto prev_type = kOverflowHead;
    auto *next_ptr = ptr + len;
    PageRef *prev = nullptr;

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
            s = allocate(kAllocateAny, node.page_id(), ovfl);
            if (s.is_ok()) {
                put_u32(next_ptr, ovfl->page_id.value);
                len = page_size - kLinkContentOffset;
                ptr = ovfl->data + sizeof(uint32_t);
                next_ptr = ovfl->data;
                if (prev) {
                    m_pager->release(prev, Pager::kNoCache);
                }
                fix_parent_id(ovfl->page_id, prev_pgno, prev_type, s);
                prev_type = kOverflowLink;
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
    if (flag) {
        // Write the back pointer for the new bucket root page.
        fix_parent_id(bucket_root_id, node.page_id(), kTreeRoot, s);
    }
    return s;
}

auto Tree::erase(TreeCursor &c, bool is_bucket) -> Status
{
    Status s;
    CALICODB_EXPECT_TRUE(c.is_valid());
    if (c.is_bucket() != is_bucket) {
        // Incompatible record type. TreeCursor::start_write() will return true if the record type has changed.
        // This check is to make sure that if the cursor was saved, it is saved on the expected record type.
        s = Status::incompatible_value();
    } else {
        s = c.start_write();
    }
    if (s.is_ok()) {
        CALICODB_EXPECT_TRUE(c.has_valid_position(true));
        CALICODB_EXPECT_TRUE(c.assert_state());
        s = remove_cell(c.m_node, c.m_idx);
    }
    if (s.is_ok()) {
        s = resolve_underflow(c);
    }
    c.finish_write(s);
    return s;
}

auto Tree::relocate_page(PageRef *&free, PointerMap::Entry entry, Id last_id) -> Status
{
    CALICODB_EXPECT_NE(free->page_id, last_id);

    Status s;
    switch (entry.type) {
        case kOverflowLink:
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
        case kOverflowHead: {
            // Back pointer points to the node that the overflow chain is rooted in. Search through that node's cells
            // for the target overflowing cell.
            Node parent;
            s = acquire(entry.back_ptr, parent, true);
            if (!s.is_ok()) {
                return s;
            }
            bool found = false;
            for (uint32_t i = 0, n = parent.cell_count(); i < n; ++i) {
                Cell cell;
                if (parent.read(i, cell)) {
                    s = corrupted_node(parent.page_id());
                    break;
                }
                found = cell.local_size < cell.total_size &&
                        read_overflow_id(cell) == last_id;
                if (found) {
                    write_overflow_id(cell, free->page_id);
                    break;
                }
            }
            const auto page_id = parent.page_id();
            release(move(parent));
            if (s.is_ok() && !found) {
                s = corrupted_node(page_id);
            }
            break;
        }
        case kTreeNode: {
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
            for (uint32_t i = 0, n = parent.cell_count(); !found && i <= n; ++i) {
                const auto child_id = parent.read_child_id(i);
                found = child_id == last_id;
                if (found) {
                    parent.write_child_id(i, free->page_id);
                }
            }
            if (!found) {
                s = corrupted_node(parent.page_id());
            }
            release(move(parent));
            [[fallthrough]];
        }
        case kTreeRoot: {
            if (!s.is_ok()) {
                return s;
            }
            // Update references.
            Node node;
            s = acquire(last_id, node, true);
            if (!s.is_ok()) {
                return s;
            }
            s = fix_links(node, free->page_id);
            release(move(node));

            if (s.is_ok() && entry.type == kTreeRoot) {
                // There is a reference to this root node in the leaf node referred to by
                // entry.back_ptr. Find that reference and update it to point to the new
                // location.
                auto found_ref = false;
                s = acquire(entry.back_ptr, node, true);
                for (uint32_t i = 0; s.is_ok() && i < node.cell_count(); ++i) {
                    Cell cell;
                    if (node.read(i, cell)) {
                        s = corrupted_node(node.page_id());
                    } else if (cell.is_bucket) {
                        const auto root_id = read_bucket_root_id(cell);
                        if (root_id == last_id) {
                            write_bucket_root_id(cell, free->page_id);
                            found_ref = true;
                            break;
                        }
                    }
                }
                if (s.is_ok() && !found_ref) {
                    s = StatusBuilder::corruption("missing bucket root reference %u in node %u",
                                                  last_id.value, node.page_id().value);
                }
                release(move(node));
            }
            break;
        }
        default:
            return corrupted_node(PointerMap::lookup(last_id, page_size));
    }

    fix_parent_id(last_id, Id::null(), kInvalidPage, s);
    fix_parent_id(free->page_id, entry.back_ptr, entry.type, s);

    if (s.is_ok()) {
        PageRef *last;
        s = m_pager->acquire(last_id, last);
        if (s.is_ok()) {
            if (entry.type == kOverflowHead || entry.type == kOverflowLink) {
                const auto next_id = read_next_id(*last);
                if (!next_id.is_null()) {
                    s = PointerMap::read_entry(*m_pager, next_id, entry);
                    fix_parent_id(next_id, free->page_id, entry.type, s);
                }
            }
            const auto new_location = free->page_id;
            m_pager->release(free, Pager::kDiscard);
            if (s.is_ok()) {
                m_pager->move_page(*last, new_location);
            }
            m_pager->release(last);
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
    const auto end_page = vacuum_end_page(page_size, db_size, free_len);
    for (; s.is_ok() && db_size > end_page.value; --db_size) {
        const Id last_page_id(db_size);
        if (!PointerMap::is_map(last_page_id, page_size)) {
            PointerMap::Entry entry;
            s = PointerMap::read_entry(*m_pager, last_page_id, entry);
            if (!s.is_ok()) {
                break;
            }
            if (entry.type != kFreelistPage) {
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

class TreeValidator
{
    template <class PageCallback>
    static auto traverse_chain(Pager &pager, PageRef *page, const PageCallback &cb) -> Status
    {
        Status s;
        do {
            s = cb(page);
            if (!s.is_ok()) {
                break;
            }
            const auto next_id = read_next_id(*page);
            pager.release(page);
            if (next_id.is_null()) {
                break;
            }
            s = pager.acquire(next_id, page);
        } while (s.is_ok());
        return s;
    }

public:
    static auto validate(Tree &tree) -> Status
    {
        auto s = InorderTraversal::traverse(tree, [&tree](auto &node, const auto &info) {
            auto check_parent_child = [&tree](auto &node, auto index) {
                Node child;
                auto s = tree.acquire(node.read_child_id(index), child, false);
                if (s.is_ok()) {
                    Id parent_id;
                    s = tree.find_parent_id(child.page_id(), parent_id);
                    if (s.is_ok() && parent_id != node.page_id()) {
                        s = StatusBuilder::corruption("expected parent page %u but found %u",
                                                      parent_id.value, node.page_id().value);
                    }
                    tree.release(move(child));
                }
                return s;
            };
            if (info.idx == info.ncells) {
                return Status::ok();
            }

            Status s;
            if (!node.is_leaf()) {
                s = check_parent_child(node, info.idx);
                // Rightmost child.
                if (s.is_ok() && info.idx + 1 == info.ncells) {
                    s = check_parent_child(node, info.idx + 1);
                }
            }
            return s;
        });
        if (!s.is_ok()) {
            return s;
        }

        s = InorderTraversal::traverse(tree, [&tree](auto &node, const auto &info) {
            if (info.idx == info.ncells) {
                return node.check_integrity();
            }
            Cell cell;
            if (node.read(info.idx, cell)) {
                return StatusBuilder::corruption("corrupted detected in cell %u from tree node %u",
                                                 info.idx, node.page_id().value);
            }

            auto accumulated = cell.local_size;
            auto requested = cell.total_size;
            if (accumulated != requested) {
                const auto overflow_id = read_overflow_id(cell);
                PageRef *head;
                auto s = tree.m_pager->acquire(overflow_id, head);
                if (!s.is_ok()) {
                    return s;
                }
                s = traverse_chain(*tree.m_pager, head, [&](auto *) {
                    if (requested <= accumulated) {
                        return StatusBuilder::corruption(
                            "overflow chain is too long (expected %u but accumulated %u)",
                            requested, accumulated);
                    }
                    accumulated += minval(tree.page_size - kLinkContentOffset,
                                          requested - accumulated);
                    return Status::ok();
                });
                if (!s.is_ok()) {
                    return s;
                }
                if (requested != accumulated) {
                    return StatusBuilder::corruption(
                        "overflow chain is wrong length (expected %u but accumulated %u)",
                        requested, accumulated);
                }
            }
            return Status::ok();
        });
        if (!s.is_ok()) {
            return s;
        }
        return Status::ok();
    }
};

auto Tree::check_integrity() -> Status
{
    return TreeValidator::validate(*this);
}

#if CALICODB_TEST

class TreePrinter
{
    struct StructuralData {
        std::vector<String> levels;
        std::vector<uint32_t> spaces;
    };

    static void add_to_level(StructuralData &data, const String &message, uint32_t target)
    {
        // If target is equal to levels.size(), add spaces to all levels.
        CALICODB_EXPECT_TRUE(target <= data.levels.size());
        uint32_t i = 0;

        [[maybe_unused]] int rc;
        auto s_itr = begin(data.spaces);
        auto L_itr = begin(data.levels);
        while (s_itr != end(data.spaces)) {
            CALICODB_EXPECT_NE(L_itr, end(data.levels));
            if (i++ == target) {
                // Don't leave trailing spaces. Only add them if there will be more text.
                for (size_t j = 0; j < *s_itr; ++j) {
                    rc = append_strings(*L_itr, " ");
                    CALICODB_EXPECT_EQ(rc, 0);
                }
                rc = append_strings(*L_itr, message.c_str());
                *s_itr = 0;
            } else {
                *s_itr += uint32_t(message.size());
            }
            ++L_itr;
            ++s_itr;
        }
    }

    static void ensure_level_exists(StructuralData &data, uint32_t level)
    {
        while (level >= data.levels.size()) {
            data.levels.emplace_back();
            data.spaces.emplace_back();
        }
        CALICODB_EXPECT_TRUE(data.levels.size() > level);
        CALICODB_EXPECT_TRUE(data.levels.size() == data.spaces.size());
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
                    msg.append_format("%u:[", node.page_id().value);
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
            if (msg.build(msg_out)) {
                return Status::no_memory();
            }
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
        if (s.is_ok() && builder.build(repr_out)) {
            return Status::no_memory();
        }
        return s;
    }

    static auto print_nodes(Tree &tree, String &repr_out) -> Status
    {
        const auto print = [&tree, &repr_out](auto &node, const auto &info) {
            if (info.idx == info.ncells) {
                StringBuilder msg;
                msg.append_format("%sternalNode(%u)\n", node.is_leaf() ? "Ex" : "In",
                                  node.page_id().value);
                for (uint32_t i = 0; i < info.ncells; ++i) {
                    Cell cell;
                    if (node.read(i, cell)) {
                        return tree.corrupted_node(node.page_id());
                    }
                    msg.append("  Cell(key=");
                    const auto local_key_size = minval(cell.key_size, cell.local_size);
                    const auto short_key_size = minval(32U, local_key_size);
                    msg.append('"');
                    msg.append_escaped(Slice(cell.key, short_key_size));
                    msg.append('"');
                    if (cell.key_size > short_key_size) {
                        msg.append("...");
                        msg.append_format(" (%u bytes total)", cell.key_size);
                    }
                    if (!node.is_leaf()) {
                        msg.append_format(", child_id=%u", read_child_id(cell).value);
                    } else if (cell.is_bucket) {
                        msg.append_format(", bucket_id=%u", read_bucket_root_id(cell).value);
                    } else {
                        msg.append(", value=");
                        const auto total_value_size = cell.total_size - cell.key_size;
                        const auto local_value_size = cell.local_size - local_key_size;
                        const auto short_value_size = minval(32U, total_value_size, local_value_size);
                        if (short_value_size) {
                            msg.append('"');
                            msg.append_escaped(Slice(cell.key + local_key_size, short_value_size));
                            msg.append('"');
                        }
                        if (short_value_size < total_value_size) {
                            msg.append("...");
                            msg.append_format(" (%u bytes total)", total_value_size);
                        }
                    }
                    msg.append(")\n");
                }
                String msg_out;
                if (msg.build(msg_out) ||
                    append_strings(repr_out, msg_out.c_str())) {
                    return Status::no_memory();
                }
            }
            return Status::ok();
        };
        return InorderTraversal::traverse(tree, print);
    }
};

auto Tree::print_structure(String &repr_out) -> Status
{
    return TreePrinter::print_structure(*this, repr_out);
}

auto Tree::print_nodes(String &repr_out) -> Status
{
    return TreePrinter::print_nodes(*this, repr_out);
}

#else

void Tree::TEST_validate()
{
}

auto Tree::print_structure(String &) -> Status
{
    return Status::ok();
}

auto Tree::print_nodes(String &) -> Status
{
    return Status::ok();
}

#endif // CALICODB_TEST

void Tree::activate_cursor(TreeCursor &target) const
{
    IntrusiveList::remove(target.m_list_entry);
    IntrusiveList::add_head(target.m_list_entry, m_active_list);
}

void Tree::deactivate_cursors(TreeCursor *exclude) const
{
    // Clear the active cursor list.
    auto *entry = m_active_list.next_entry;
    while (entry != &m_active_list) {
        auto *ptr = entry;
        entry = ptr->next_entry;
        CALICODB_EXPECT_NE(ptr->cursor, nullptr);
        if (ptr->cursor != exclude) {
            ptr->cursor->save_position();
            IntrusiveList::remove(*ptr);
            IntrusiveList::add_head(*ptr, m_inactive_list);
        }
    }
}

} // namespace calicodb

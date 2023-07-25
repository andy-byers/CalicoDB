// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "tree.h"
#include "encoding.h"
#include "logging.h"
#include "pager.h"
#include "schema.h"
#include "stat.h"
#include "utils.h"
#include <functional>

namespace calicodb
{

// Helper for traversing the tree structure
class TreeCursor
{
    friend class InorderTraversal;
    friend class Tree;
    friend class TreeValidator;
    friend class UserCursor;

    Tree *const m_tree;
    Status m_status;

    Node m_node;
    U32 m_idx = 0;

    // *_path members are used to track the path taken from the tree's root to the current
    // position. At any given time, the elements with indices less than the current level
    // are valid.
    static constexpr std::size_t kMaxDepth = 17 + 1;
    Node m_node_path[kMaxDepth - 1];
    U32 m_idx_path[kMaxDepth - 1];
    int m_level = 0;

    auto move_to_right_sibling() -> void
    {
        CALICODB_EXPECT_TRUE(has_node());
        CALICODB_EXPECT_TRUE(m_node.is_leaf());
        for (U32 adjust = 0;; adjust = 1) {
            const auto ncells = NodeHdr::get_cell_count(m_node.hdr());
            if (++m_idx < ncells + adjust) {
                break;
            } else if (m_level == 0) {
                reset();
                return;
            }
            move_to_parent();
            CALICODB_EXPECT_FALSE(m_node.is_leaf());
        }
        while (!m_node.is_leaf()) {
            move_to_child(m_node.read_child_id(m_idx));
            if (!m_status.is_ok()) {
                return;
            }
            m_idx = 0;
        }
    }

    auto move_to_left_sibling() -> void
    {
        CALICODB_EXPECT_TRUE(has_node());
        CALICODB_EXPECT_TRUE(m_node.is_leaf());
        for (;;) {
            if (m_idx > 0) {
                --m_idx;
                break;
            } else if (m_level == 0) {
                reset();
                return;
            }
            move_to_parent();
            CALICODB_EXPECT_FALSE(m_node.is_leaf());
        }
        while (!m_node.is_leaf()) {
            move_to_child(m_node.read_child_id(m_idx));
            if (!m_status.is_ok()) {
                return;
            }
            m_idx = NodeHdr::get_cell_count(m_node.hdr()) - m_node.is_leaf();
        }
    }

    auto seek_to_first_leaf() -> void
    {
        seek_to_leaf("");
    }

    auto seek_to_last_leaf() -> void
    {
        reset();
        m_status = m_tree->acquire(m_tree->root(), m_node);
        while (m_status.is_ok()) {
            m_idx = NodeHdr::get_cell_count(m_node.hdr());
            if (m_node.is_leaf()) {
                m_idx -= m_idx > 0;
                break;
            }
            move_to_child(NodeHdr::get_next_id(m_node.hdr()));
        }
    }

    auto search_node(const Slice &key) -> bool
    {
        CALICODB_EXPECT_TRUE(m_status.is_ok());
        CALICODB_EXPECT_NE(m_node.ref, nullptr);

        std::string key_buffer;
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
            m_status = m_tree->read_key(m_node, mid, key_buffer, &rhs,
                                        static_cast<U32>(key.size() + 1));
            if (!m_status.is_ok()) {
                break;
            }
            const auto cmp = key.compare(rhs);
            if (cmp <= 0) {
                exact = cmp == 0;
                upper = mid;
            } else {
                lower = mid + 1;
            }
        }

        m_idx = lower + exact * !m_node.is_leaf();
        return exact;
    }

    explicit TreeCursor(Tree &tree)
        : m_tree(&tree)
    {
    }

public:
    // Return true if the cursor is positioned on a valid node, false otherwise
    [[nodiscard]] auto has_node() const -> bool
    {
        return m_status.is_ok() && m_node.ref != nullptr;
    }

    // Return true if the cursor is positioned on a valid key, false otherwise
    [[nodiscard]] auto has_key() const -> bool
    {
        return has_node() && m_idx < NodeHdr::get_cell_count(m_node.hdr());
    }

    [[nodiscard]] auto page_id() const -> Id
    {
        CALICODB_EXPECT_TRUE(has_node());
        return m_node.ref->page_id;
    }

    // Initialize the cursor for a new traversal
    auto reset(const Status &s = Status::ok()) -> void
    {
        release_nodes(kAllLevels);
        m_status = s;
        m_level = 0;
    }

    auto move_to_parent() -> void
    {
        CALICODB_EXPECT_GT(m_level, 0);
        release_nodes(kCurrentLevel);
        --m_level;
        m_idx = m_idx_path[m_level];
        m_node = std::move(m_node_path[m_level]);
    }

    auto assign_child(Node child) -> void
    {
        CALICODB_EXPECT_TRUE(has_node());
        m_idx_path[m_level] = m_idx;
        m_node_path[m_level] = std::move(m_node);
        m_node = std::move(child);
        ++m_level;
    }

    auto move_to_child(Id child_id) -> void
    {
        CALICODB_EXPECT_TRUE(has_node());
        if (m_level < static_cast<int>(kMaxDepth - 1)) {
            Node child;
            m_status = m_tree->acquire(child_id, child);
            if (m_status.is_ok()) {
                assign_child(std::move(child));
            }
        } else {
            m_status = m_tree->corrupted_node(child_id);
        }
    }

    auto correct_leaf() -> void
    {
        CALICODB_EXPECT_TRUE(has_node());
        CALICODB_EXPECT_TRUE(m_node.is_leaf());
        if (m_idx == NodeHdr::get_cell_count(m_node.hdr())) {
            move_to_right_sibling();
        }
    }

    auto seek_to_leaf(const Slice &key) -> bool
    {
        if (!m_status.is_corruption()) {
            reset();
            m_status = m_tree->acquire(m_tree->root(), m_node);
        }
        while (m_status.is_ok()) {
            const auto found = search_node(key);
            if (m_status.is_ok()) {
                if (m_node.is_leaf()) {
                    return found;
                }
                move_to_child(m_node.read_child_id(m_idx));
            }
        }
        return false;
    }

    enum ReleaseType {
        kCurrentLevel,
        kAllLevels,
    };
    auto release_nodes(ReleaseType type) -> void
    {
        m_tree->release(std::move(m_node));
        if (type < kAllLevels) {
            return;
        }
        for (int i = 0; i < m_level; ++i) {
            m_tree->release(std::move(m_node_path[i]));
        }
    }
};

Cursor::Cursor() = default;

Cursor::~Cursor() = default;

class UserCursor : public Cursor
{
    friend class Tree;

    TreeCursor m_c;

    std::string m_key_buffer;
    std::string m_value_buffer;
    Slice m_key;
    Slice m_value;
    bool m_saved = false;

    auto fetch_payload() -> Status
    {
        CALICODB_EXPECT_TRUE(m_c.has_key());

        m_key.clear();
        m_value.clear();

        Cell cell;
        if (m_c.m_node.read(m_c.m_idx, cell)) {
            return m_c.m_tree->corrupted_node(m_c.page_id());
        }
        auto s = m_c.m_tree->read_key(cell, m_key_buffer, &m_key);
        if (s.is_ok()) {
            s = m_c.m_tree->read_value(cell, m_value_buffer, &m_value);
        }
        if (!s.is_ok()) {
            m_key_buffer.clear();
            m_value_buffer.clear();
            m_key.clear();
            m_value.clear();
        }
        return s;
    }

    auto prepare() -> void
    {
        m_c.m_tree->use_cursor(this);
    }

    auto save_position() -> void
    {
        CALICODB_EXPECT_TRUE(m_c.has_key());
        m_c.release_nodes(TreeCursor::kAllLevels);
        m_saved = m_c.m_status.is_ok();
    }

    auto ensure_position_loaded() -> void
    {
        if (m_saved) {
            m_saved = false;
            m_c.seek_to_leaf(m_key);
            ensure_correct_leaf();
        }
    }

    auto ensure_correct_leaf() -> void
    {
        if (m_c.has_node()) {
            m_c.correct_leaf();
        }
        if (m_c.has_key()) {
            m_c.m_status = fetch_payload();
        }
    }

    explicit UserCursor(TreeCursor c)
        : m_c(std::move(c))
    {
    }

public:
    ~UserCursor() override
    {
        m_c.reset();
        // The TreeCursor contained within this class is about to be destroyed. Make sure the
        // tree doesn't attempt to access its contents when switching cursors.
        if (m_c.m_tree->m_last_c == this) {
            m_c.m_tree->m_last_c = nullptr;
        }
    }

    [[nodiscard]] auto key() const -> Slice override
    {
        CALICODB_EXPECT_TRUE(is_valid());
        return m_key;
    }

    [[nodiscard]] auto value() const -> Slice override
    {
        CALICODB_EXPECT_TRUE(is_valid());
        return m_value;
    }

    [[nodiscard]] auto is_valid() const -> bool override
    {
        return m_c.has_key() || m_saved;
    }

    auto status() const -> Status override
    {
        return m_c.m_status;
    }

    auto seek_first() -> void override
    {
        prepare();
        m_saved = false;
        m_c.seek_to_first_leaf();
        if (m_c.has_key()) {
            m_c.m_status = fetch_payload();
        }
    }

    auto seek_last() -> void override
    {
        prepare();
        m_saved = false;
        m_c.seek_to_last_leaf();
        if (m_c.has_key()) {
            m_c.m_status = fetch_payload();
        }
    }

    auto next() -> void override
    {
        CALICODB_EXPECT_TRUE(is_valid());
        prepare();

        // NOTE: Loading the cursor position involves seeking back to the saved key. If the saved key
        //       was erased, then this will place the cursor on the first record with a key that
        //       compares greater than it.
        ensure_position_loaded();
        if (!m_c.has_key()) {
            return;
        }
        m_c.move_to_right_sibling();
        if (m_c.has_key()) {
            m_c.m_status = fetch_payload();
        }
    }

    auto previous() -> void override
    {
        CALICODB_EXPECT_TRUE(is_valid());
        prepare();

        ensure_position_loaded();
        if (!m_c.has_key()) {
            return;
        }
        m_c.move_to_left_sibling();
        if (m_c.has_key()) {
            m_c.m_status = fetch_payload();
        }
    }

    auto seek(const Slice &key) -> void override
    {
        // The cursor position is not reset prior to the call to seek_to_leaf(). seek_to_leaf() may
        // try to avoid performing a full root-to-leaf traversal.
        prepare();
        m_saved = false;
        m_c.seek_to_leaf(key);
        ensure_correct_leaf();
    }
};

static constexpr U32 kCellPtrSize = sizeof(U16);

[[nodiscard]] static auto corrupted_page(Id page_id, PointerMap::Type page_type = PointerMap::kEmpty) -> Status
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
        case PointerMap::kFreelistTrunk:
        case PointerMap::kFreelistLeaf:
            type_name = "freelist page";
            break;
        default:
            type_name = "page";
    }
    std::string message;
    append_fmt_string(message, "corruption detected on %s with ID %u",
                      type_name, page_id.value);
    return Status::corruption(message);
}

auto Tree::corrupted_node(Id page_id) const -> Status
{
    auto s = corrupted_page(page_id, PointerMap::kTreeNode);
    if (m_pager->mode() >= Pager::kWrite) {
        // Pager status should never be set unless a rollback is needed.
        m_pager->set_status(s);
    }
    return s;
}

auto Tree::get_tree(Cursor &c) -> Tree *
{
    return static_cast<UserCursor *>(c.token())->m_c.m_tree;
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

[[nodiscard]] static auto merge_root(Node &root, Node &child)
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
    static auto access(
        Pager &pager,
        const Cell &cell,   // The `cell` containing the payload being accessed
        U32 offset,         // `offset` within the payload being accessed
        U32 length,         // Number of bytes to access
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
                }
                U32 len;
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
        if (s.is_ok() && length) {
            std::string message;
            const auto repr_len = std::min(cell.key_size, 4U);
            append_fmt_string(message, R"(missing %u bytes from record "%s"%s)",
                              length, escape_string(Slice(cell.key, repr_len)).c_str(),
                              repr_len == cell.key_size ? "" : "...");
            return Status::corruption(message);
        }
        return s;
    }
};

auto Tree::release_nodes() const -> void
{
    use_cursor(nullptr);
    m_cursor->reset();
}

auto Tree::create(Pager &pager, Id *root_id_out) -> Status
{
    PageRef *page;
    auto s = pager.allocate(page);
    if (s.is_ok()) {
        auto *hdr = page->page + page_offset(page->page_id);
        //        Node::from_new_page(*page, true);
        std::memset(hdr, 0, NodeHdr::kSize);
        NodeHdr::put_type(hdr, true);
        NodeHdr::put_cell_start(hdr, kPageSize);

        s = PointerMap::write_entry(pager, page->page_id,
                                    {Id::null(), PointerMap::kTreeRoot});
        if (root_id_out) {
            *root_id_out = s.is_ok() ? page->page_id : Id::null();
        }
        pager.release(page);
    }
    return s;
}

auto Tree::read_key(Node &node, U32 index, std::string &scratch, Slice *key_out, U32 limit) const -> Status
{
    Cell cell;
    if (node.read(index, cell)) {
        return corrupted_node(node.ref->page_id);
    }
    return read_key(cell, scratch, key_out, limit);
}
auto Tree::read_key(const Cell &cell, std::string &scratch, Slice *key_out, U32 limit) const -> Status
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

auto Tree::read_value(Node &node, U32 index, std::string &scratch, Slice *value_out) const -> Status
{
    Cell cell;
    if (node.read(index, cell)) {
        return corrupted_node(node.ref->page_id);
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

auto Tree::write_key(Node &node, U32 index, const Slice &key) -> Status
{
    Cell cell;
    if (node.read(index, cell)) {
        return corrupted_node(node.ref->page_id);
    }
    return PayloadManager::access(*m_pager, cell, 0,
                                  static_cast<U32>(key.size()),
                                  key.data(), nullptr);
}

auto Tree::write_value(Node &node, U32 index, const Slice &value) -> Status
{
    Cell cell;
    if (node.read(index, cell)) {
        return corrupted_node(node.ref->page_id);
    }
    return PayloadManager::access(*m_pager, cell, cell.key_size,
                                  static_cast<U32>(value.size()),
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

auto Tree::fix_parent_id(Id page_id, Id parent_id, PointerMap::Type type) -> Status
{
    return PointerMap::write_entry(*m_pager, page_id, {parent_id, type});
}

auto Tree::maybe_fix_overflow_chain(const Cell &cell, Id parent_id) -> Status
{
    if (cell.local_pl_size != cell.total_pl_size) {
        return fix_parent_id(read_overflow_id(cell), parent_id, PointerMap::kOverflowHead);
    }
    return Status::ok();
}

auto Tree::make_pivot(const PivotOptions &opt, Cell &pivot_out) -> Status
{
    std::string buffers[2];
    Slice keys[2];
    for (std::size_t i = 0; i < 2; ++i) {
        const auto local_key_size = std::min(
            opt.cells[i]->key_size,
            opt.cells[i]->local_pl_size);
        keys[i] = Slice(opt.cells[i]->key, local_key_size);
    }
    if (keys[0] >= keys[1]) {
        // The left key must be less than the right key. If this cannot be seen in the local
        // keys, then 1 of the 2 must be overflowing. The nonlocal part is needed to perform
        // suffix truncation.
        for (std::size_t i = 0; i < 2; ++i) {
            if (opt.cells[i]->key_size > keys[i].size()) {
                // Read just enough of the key to determine the ordering.
                auto s = read_key(
                    *opt.cells[i],
                    buffers[i],
                    &keys[i],
                    opt.cells[1 - i]->key_size + 1);
                if (!s.is_ok()) {
                    return s;
                }
            }
        }
    }
    auto *ptr = opt.scratch + sizeof(U32); // Skip the left child ID.
    auto prefix = truncate_suffix(keys[0], keys[1]);
    pivot_out.ptr = opt.scratch;
    pivot_out.total_pl_size = static_cast<U32>(prefix.size());
    pivot_out.key = encode_varint(ptr, pivot_out.total_pl_size);
    pivot_out.local_pl_size = compute_local_pl_size(prefix.size(), 0);
    pivot_out.footprint = pivot_out.local_pl_size + U32(pivot_out.key - opt.scratch);
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
            s = m_pager->allocate(dst);
            if (!s.is_ok()) {
                break;
            }
            const auto copy_size = std::min<std::size_t>(
                prefix.size(), kLinkContentSize);
            std::memcpy(dst->page + kLinkContentOffset,
                        prefix.data(),
                        copy_size);
            prefix.advance(copy_size);

            if (prev) {
                put_u32(prev->page, dst->page_id.value);
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
            put_u32(prev->page, 0);
            pivot_out.footprint += sizeof(U32);
        }
        m_pager->release(prev, Pager::kNoCache);
    }
    return s;
}

static auto detach_cell(Cell &cell, char *backing) -> void
{
    CALICODB_EXPECT_NE(backing, nullptr);
    if (cell.ptr != backing) {
        std::memcpy(backing, cell.ptr, cell.footprint);
        const auto diff = cell.key - cell.ptr;
        cell.ptr = backing;
        cell.key = backing + diff;
    }
}

auto Tree::post_pivot(Node &parent, U32 idx, Cell &pivot, Id child_id) -> Status
{
    const auto rc = parent.write(idx, pivot);
    if (rc > 0) {
        put_u32(parent.ref->page + rc, child_id.value);
    } else if (rc == 0) {
        CALICODB_EXPECT_FALSE(m_ovfl.exists());
        detach_cell(pivot, m_cell_scratch[0]);
        m_ovfl = {pivot, parent.ref->page_id, idx};
        write_child_id(pivot, child_id);
    } else {
        return corrupted_node(parent.ref->page_id);
    }
    auto s = fix_parent_id(
        child_id,
        parent.ref->page_id,
        PointerMap::kTreeNode);
    if (s.is_ok()) {
        s = maybe_fix_overflow_chain(pivot, parent.ref->page_id);
    }
    return s;
}

auto Tree::insert_cell(Node &node, U32 idx, const Cell &cell) -> Status
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

auto Tree::remove_cell(Node &node, U32 idx) -> Status
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
    for (U32 i = 0, n = NodeHdr::get_cell_count(node.hdr()); i < n; ++i) {
        Cell cell;
        if (node.read(i, cell)) {
            return corrupted_node(node.ref->page_id);
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

auto Tree::resolve_overflow(TreeCursor &c) -> Status
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

auto Tree::split_root(TreeCursor &c) -> Status
{
    CALICODB_EXPECT_EQ(c.m_level, 0);
    auto &root = c.m_node;
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

        root = Node::from_new_page(*root.ref, m_node_scratch, false);
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
        c.assign_child(std::move(child));
        c.m_idx_path[1] = c.m_idx_path[0];
        c.m_idx_path[0] = 0;
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
    auto s = allocate(node.is_leaf(), left);
    const auto pivot_idx = c.m_idx_path[c.m_level - 1];

    if (s.is_ok()) {
        if (m_ovfl.idx == NodeHdr::get_cell_count(node.hdr())) {
            return split_nonroot_fast(c, parent, std::move(left));
        }
        s = redistribute_cells(left, node, parent, pivot_idx);
    }

    release(std::move(left));
    c.move_to_parent();
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
        CALICODB_EXPECT_GT(c.m_level, 0);
        const auto pivot_idx = c.m_idx_path[c.m_level - 1];

        // Post the pivot into the parent node. This call will fix the sibling's parent pointer.
        s = post_pivot(parent, pivot_idx, pivot, left.ref->page_id);
        if (s.is_ok()) {
            parent.write_child_id(
                pivot_idx + !m_ovfl.exists(),
                right.ref->page_id);
            s = fix_parent_id(
                right.ref->page_id,
                parent.ref->page_id,
                PointerMap::kTreeNode);
        }
    }

cleanup:
    release(std::move(right));
    c.move_to_parent();
    return s;
}

auto Tree::resolve_underflow(TreeCursor &c) -> Status
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
auto Tree::redistribute_cells(Node &left, Node &right, Node &parent, U32 pivot_idx) -> Status
{
    upgrade(parent);

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
    // Create a temporary node that uses a scratch buffer as its backing buffer. This is where the
    // new copy of the nonempty sibling node will be built.
    auto ref = *p_src->ref;
    ref.page = m_split_scratch;
    tmp = Node::from_new_page(ref, m_node_scratch, p_src->is_leaf());
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
    // also expects that the child pointer in `parent` at `pivor_idx+1` points to `right` There
    // may not be a pointer to `left` in `parent` yet.
    CALICODB_EXPECT_TRUE(!is_split || (m_ovfl.idx < cell_count && p_src == &right));

    // Cells that need to be redistributed, in order.
    std::vector<Cell> cells(cell_count + m_ovfl.exists());
    auto cell_itr = begin(cells);
    U32 right_accum = 0;
    Cell cell;

    for (U32 i = 0; i < cell_count;) {
        if (m_ovfl.exists() && i == m_ovfl.idx) {
            right_accum += m_ovfl.cell.footprint;
            // Move the overflow cell backing to an unused scratch buffer. The `parent` may overflow
            // when the pivot is posted (and this cell may not be the pivot). The new overflow cell
            // will use scratch buffer 0, so this cell cannot be stored there.
            detach_cell(m_ovfl.cell, m_cell_scratch[3]);
            *cell_itr++ = m_ovfl.cell;
            m_ovfl.clear();
            continue;
        }
        if (p_src->read(i++, cell)) {
            return corrupted_node(p_src->ref->page_id);
        }
        right_accum += cell.footprint;
        *cell_itr++ = cell;
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
                auto s = free_overflow(read_overflow_id(cell));
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
                cells.emplace_back(cell);
            } else {
                cells.insert(begin(cells), cell);
            }
        }
        parent.erase(pivot_idx, cell.footprint);
    }
    CALICODB_EXPECT_GE(cells.size(), is_split ? 4 : 1);

    auto sep = -1;
    for (U32 left_accum = 0; right_accum > p_left->usable_space / 2 &&
                             right_accum > left_accum &&
                             2 + sep++ < int(cells.size());) {
        left_accum += cells[U32(sep)].footprint;
        right_accum -= cells[U32(sep)].footprint;
    }
    if (sep == 0) {
        sep = 1;
    }

    Status s;
    auto idx = int(cells.size()) - 1;
    for (; idx > sep; --idx) {
        s = insert_cell(*p_right, 0, cells[U32(idx)]);
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
                {&cells[U32(idx) - 1],
                 &cells[U32(idx)]},
                m_cell_scratch[2],
                parent.ref->page_id,
            };
            s = make_pivot(opt, pivot);
            cells[U32(idx)] = pivot;

        } else {
            const auto next_id = read_child_id(cells[U32(idx)]);
            NodeHdr::put_next_id(p_left->hdr(), next_id);
            s = fix_parent_id(next_id, p_left->ref->page_id, PointerMap::kTreeNode);
        }
        if (s.is_ok()) {
            // Post the pivot. This may cause the `parent` to overflow.
            s = post_pivot(parent, pivot_idx, cells[U32(idx)], p_left->ref->page_id);
            --idx;
        }
    }
    if (!s.is_ok()) {
        return s;
    }

    // Write the rest of the cells to p_left.
    for (; idx >= 0; --idx) {
        s = insert_cell(*p_left, 0, cells[U32(idx)]);
        if (!s.is_ok()) {
            return s;
        }
    }

    // Copy the newly-built node back to the initial nonempty sibling. TODO: See above TODO about the pager.
    std::memcpy(p_src->ref->page, tmp.ref->page, kPageSize);
    auto *saved_ref = p_src->ref;
    *p_src = std::move(tmp);
    p_src->ref = saved_ref;

    // Only the parent is allowed to overflow. The caller is expected to rebalance the parent in this case.
    CALICODB_EXPECT_TRUE(!m_ovfl.exists() || m_ovfl.pid == parent.ref->page_id);
    return s;
}

auto Tree::fix_nonroot(TreeCursor &c, Node &parent, U32 idx) -> Status
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
        release(std::move(*p_right));
        if (s.is_ok() && 0 == NodeHdr::get_cell_count(p_left->hdr())) {
            // redistribute_cells() performed a merge.
            s = free(std::move(*p_left));
        } else {
            release(std::move(*p_left));
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

auto Tree::fix_root(TreeCursor &c) -> Status
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
                release(std::move(child));
            } else {
                m_ovfl.cell = cell;
                detach_cell(m_ovfl.cell, m_cell_scratch[0]);
                child.erase(c.m_idx, cell.footprint);
                c.assign_child(std::move(child));
                s = split_nonroot(c);
            }
        } else {
            if (merge_root(node, child)) {
                s = corrupted_node(node.ref->page_id);
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

Tree::Tree(Pager &pager, Stat &stat, char *scratch, const Id *root_id)
    : m_cursor(new TreeCursor(*this)),
      m_stat(&stat),
      m_node_scratch(scratch + kPageSize),
      m_split_scratch(scratch + kPageSize * 2),
      m_cell_scratch{
          scratch,
          scratch + kCellBufferLen,
          scratch + kCellBufferLen * 2,
          scratch + kCellBufferLen * 3,
      },
      m_pager(&pager),
      m_root_id(root_id)
{
    // Make sure that cells written to scratch memory don't interfere with each other.
    static_assert(kCellBufferLen > kMaxCellHeaderSize + compute_local_pl_size(kPageSize, 0));
}

Tree::~Tree()
{
    release_nodes();
    delete m_cursor;
}

auto Tree::get(const Slice &key, std::string *value) const -> Status
{
    Status s;
    use_cursor(nullptr);
    const auto key_exists = m_cursor->seek_to_leaf(key);
    if (!m_cursor->m_status.is_ok()) {
        s = m_cursor->m_status;
    } else if (!key_exists) {
        s = Status::not_found();
    } else if (value) {
        Slice slice;
        s = read_value(m_cursor->m_node, m_cursor->m_idx, *value, &slice);
        value->resize(slice.size());
    }
    return s;
}

auto Tree::put(Cursor &c, const Slice &key, const Slice &value) -> Status
{
    if (!c.status().is_ok()) {
        return c.status();
    }

    auto &uc = reinterpret_cast<UserCursor &>(c);
    use_cursor(&uc);

    auto s = put(uc.m_c, key, value);
    if (s.is_ok()) {
        if (uc.m_c.has_key()) {
            // Cursor is already on the correct record.
            s = uc.fetch_payload();
        } else {
            // There must have been a SMO. The rebalancing routine clears the cursor,
            // since it is left on an internal node. Seek back to where the record
            // was inserted.
            uc.seek(key);
            s = uc.status();
        }
    }
    if (uc.m_c.m_status.is_ok()) {
        uc.m_c.m_status = s;
    }
    return s;
}

auto Tree::put(TreeCursor &c, const Slice &key, const Slice &value) -> Status
{
    static constexpr auto kMaxLength = std::numeric_limits<U32>::max();
    if (key.size() > kMaxLength) {
        return Status::invalid_argument("key is too long");
    } else if (value.size() > kMaxLength) {
        return Status::invalid_argument("value is too long");
    }

    const auto key_exists = c.seek_to_leaf(key);
    auto s = c.m_status;
    if (s.is_ok()) {
        upgrade(c.m_node);
        if (key_exists) {
            s = remove_cell(c.m_node, c.m_idx);
        }
        if (s.is_ok()) {
            bool overflow;
            // Attempt to write a cell representing the `key` and `value` directly to the page.
            // This routine also populates any overflow pages necessary to hold a `key` and/or
            // `value` that won't fit on a single node page. If the cell cannot fit in `node`,
            // it will be written to m_cell_scratch[0] instead.
            s = emplace(c.m_node, key, value, c.m_idx, overflow);

            if (s.is_ok()) {
                if (overflow) {
                    // There wasn't enough room for the cell in `node`, so it was built in
                    // m_cell_scratch[0] instead.
                    Cell ovfl;
                    const auto rc = c.m_node.parser(
                        m_cell_scratch[0],
                        m_cell_scratch[1],
                        &ovfl);
                    if (rc) {
                        s = corrupted_node(c.page_id());
                    } else {
                        CALICODB_EXPECT_FALSE(m_ovfl.exists());
                        m_ovfl = {ovfl, c.page_id(), c.m_idx};
                        s = resolve_overflow(c);
                    }
                }
            }
        }
    }
    return s;
}

auto Tree::put(const Slice &key, const Slice &value) -> Status
{
    use_cursor(nullptr);
    return put(*m_cursor, key, value);
}

auto Tree::emplace(Node &node, const Slice &key, const Slice &value, U32 index, bool &overflow) -> Status
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
    const auto pad_size = hdr_size > kMinCellHeaderSize ? 0 : kMinCellHeaderSize - hdr_size;
    const auto cell_size = local_pl_size + hdr_size + pad_size + sizeof(U32) * has_remote;
    // External cell headers are padded out to 4 bytes.
    std::memset(ptr, 0, pad_size);

    // Attempt to allocate space for the cell in the node. If this is not possible,
    // write the cell to scratch memory. allocate_block() should not return an offset
    // that would interfere with the node header/indirection vector or cause an out-of-
    // bounds write (this only happens if the node is corrupted).
    ptr = m_cell_scratch[0];
    const auto local_offset = node.alloc(
        index, static_cast<U32>(cell_size));
    if (local_offset > 0) {
        ptr = node.ref->page + local_offset;
        overflow = false;
    } else if (local_offset == 0) {
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
    use_cursor(nullptr);
    const auto key_exists = m_cursor->seek_to_leaf(key);
    auto s = m_cursor->m_status;
    if (s.is_ok() && key_exists) {
        s = erase(*m_cursor);
    }
    return s;
}

auto Tree::erase(Cursor &c) -> Status
{
    if (!c.status().is_ok()) {
        return c.status();
    } else if (!c.is_valid()) {
        return Status::invalid_argument();
    }
    Status s;
    auto &uc = reinterpret_cast<UserCursor &>(c);
    auto &tc = uc.m_c;
    use_cursor(&uc);

    std::string saved_key;
    uc.ensure_position_loaded();
    if (1 == NodeHdr::get_cell_count(tc.m_node.hdr())) {
        // This node will underflow when the record is removed. Make sure the key is saved so that
        // the correct position can be found after underflow resolution.
        saved_key = std::move(uc.m_key_buffer);
        saved_key.resize(uc.m_key.size());
    }
    s = erase(tc);
    if (s.is_ok()) {
        if (tc.has_node()) {
            uc.ensure_correct_leaf();
        } else {
            uc.seek(saved_key);
        }
        s = uc.status();
    } else if (tc.m_status.is_ok()) {
        tc.m_status = s;
    }
    return s;
}

auto Tree::erase(TreeCursor &c) -> Status
{
    Status s;
    if (c.m_idx < NodeHdr::get_cell_count(c.m_node.hdr())) {
        upgrade(c.m_node);
        s = remove_cell(c.m_node, c.m_idx);
        if (s.is_ok() && is_underflowing(c.m_node)) {
            s = resolve_underflow(c);
        }
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
            s = acquire(entry.back_ptr, parent, true);
            if (!s.is_ok()) {
                return s;
            }
            bool found = false;
            for (U32 i = 0, n = NodeHdr::get_cell_count(parent.hdr()); i < n; ++i) {
                Cell cell;
                if (parent.read(i, cell)) {
                    s = corrupted_node(parent.ref->page_id);
                    break;
                }
                found = cell.local_pl_size < cell.total_pl_size &&
                        read_overflow_id(cell) == last_id;
                if (found) {
                    write_overflow_id(cell, free.page_id);
                    break;
                }
            }
            const auto page_id = parent.ref->page_id;
            release(std::move(parent));
            if (s.is_ok() && !found) {
                s = corrupted_node(page_id);
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
                s = acquire(entry.back_ptr, parent, true);
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
                    s = corrupted_node(parent.ref->page_id);
                }
                release(std::move(parent));
            }
            if (!s.is_ok()) {
                return s;
            }
            // Update references.
            Node last;
            s = acquire(last_id, last, true);
            if (!s.is_ok()) {
                return s;
            }
            s = fix_links(last, free.page_id);
            release(std::move(last));
            break;
        }
        default:
            return corrupted_node(PointerMap::lookup(last_id));
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
    // Page ID of the most-recent pointer map page (the page that holds the back pointer for the last page
    // in the database file).
    const auto pm_page = PointerMap::lookup(Id(db_size));
    // Number of pointer map pages between the current last page and the after-vacuum last page.
    const auto pm_size = (free_size + pm_page.value + kEntriesPerMap - db_size) / kEntriesPerMap;

    auto end_page = Id(db_size - free_size - pm_size);
    end_page.value -= PointerMap::is_map(end_page);
    return end_page;
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

    // Count the number of pages in the freelist, since we don't keep this information stored
    // anywhere. This involves traversing the list of freelist trunk pages. Luckily, these pages
    // are likely to be accessed again soon, so it may not hurt have them in the pager cache.
    const auto free_len = FileHdr::get_freelist_length(root.page);
    // Determine what the last page in the file should be after this vacuum is run to completion.
    const auto end_page = vacuum_end_page(db_size, free_len);
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
                    s = m_pager->allocate(free);
                    if (s.is_ok()) {
                        if (free->page_id <= end_page) {
                            s = vacuum_step(*free, entry, schema, last_page_id);
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
        FileHdr::put_freelist_length(root.page, 0);
        m_pager->set_page_count(db_size);
    }
    return s;
}

class InorderTraversal
{
public:
    struct TraversalInfo {
        U32 idx;
        U32 ncells;
        U32 level;
    };
    using Callback = std::function<Status(Node &, const TraversalInfo &)>;

    // Call the callback for every record and pivot in the tree, in sort order, plus once when the node
    // is no longer required for determining the rest of the traversal
    static auto traverse(const Tree &tree, const Callback &cb) -> Status
    {
        Node root;
        auto s = tree.acquire(tree.root(), root);
        if (s.is_ok()) {
            s = traverse_impl(tree, std::move(root), cb, 0);
        }
        return s;
    }

private:
    static auto traverse_impl(const Tree &tree, Node node, const Callback &cb, U32 level) -> Status
    {
        Status s;
        for (U32 i = 0, n = NodeHdr::get_cell_count(node.hdr()); s.is_ok() && i <= n; ++i) {
            if (!node.is_leaf()) {
                const auto save_id = node.ref->page_id;
                const auto next_id = node.read_child_id(i);
                tree.release(std::move(node));

                Node next;
                s = tree.acquire(next_id, next);
                if (s.is_ok()) {
                    s = traverse_impl(tree, std::move(next), cb, level + 1);
                }
                if (s.is_ok()) {
                    s = tree.acquire(save_id, node);
                }
            }
            if (s.is_ok()) {
                s = cb(node, {i, n, level});
            }
        }
        tree.release(std::move(node));
        return s;
    }
};

auto Tree::destroy(Tree &tree) -> Status
{
    CALICODB_EXPECT_FALSE(tree.root().is_root());
    const auto cleanup = [&tree](auto &node, const auto &info) {
        if (info.idx == info.ncells) {
            return tree.free(std::move(node));
        }
        Cell cell;
        if (node.read(info.idx, cell)) {
            return tree.corrupted_node(node.ref->page_id);
        }
        if (cell.local_pl_size < cell.total_pl_size) {
            return tree.free_overflow(read_overflow_id(cell));
        }
        return Status::ok();
    };
    return InorderTraversal::traverse(tree, cleanup);
}

#if CALICODB_TEST

#define CHECK_OK(expr)                                       \
    do {                                                     \
        if (const auto check_s = (expr); !check_s.is_ok()) { \
            std::fprintf(stderr, "error(%s:%d): %s\n",       \
                         __FILE__, __LINE__,                 \
                         check_s.to_string().c_str());       \
            std::abort();                                    \
        }                                                    \
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
        std::vector<std::string> levels;
        std::vector<U32> spaces;
    };

    static auto add_to_level(StructuralData &data, const std::string &message, U32 target) -> void
    {
        // If target is equal to levels.size(), add spaces to all levels.
        CHECK_TRUE(target <= data.levels.size());
        U32 i = 0;

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
                *s_itr += U32(message.size());
            }
            ++L_itr;
            ++s_itr;
        }
    }

    static auto ensure_level_exists(StructuralData &data, U32 level) -> void
    {
        while (level >= data.levels.size()) {
            data.levels.emplace_back();
            data.spaces.emplace_back();
        }
        CHECK_TRUE(data.levels.size() > level);
        CHECK_TRUE(data.levels.size() == data.spaces.size());
    }

public:
    static auto print_structure(const Tree &tree, std::string &repr_out) -> Status
    {
        StructuralData data;
        const auto print = [&data](auto &node, const auto &info) {
            std::string msg;
            if (info.idx == info.ncells) {
                if (node.is_leaf()) {
                    append_fmt_string(msg, "%u]", info.ncells);
                }
            } else {
                if (info.idx == 0) {
                    append_fmt_string(msg, "%u:[", node.ref->page_id.value);
                    ensure_level_exists(data, info.level);
                }
                if (!node.is_leaf()) {
                    msg += '*';
                    if (info.idx + 1 == info.ncells) {
                        msg += ']';
                    }
                }
            }
            add_to_level(data, msg, info.level);
            return Status::ok();
        };
        auto s = InorderTraversal::traverse(tree, print);
        if (s.is_ok()) {
            for (const auto &level : data.levels) {
                repr_out.append(level + '\n');
            }
        }
        return s;
    }

    static auto print_nodes(const Tree &tree, std::string &repr_out) -> Status
    {
        const auto print = [&repr_out, &tree](auto &node, const auto &info) {
            if (info.idx == info.ncells) {
                std::string msg;
                append_fmt_string(msg, "%sternalNode(%u)\n", node.is_leaf() ? "Ex" : "In",
                                  node.ref->page_id.value);
                for (U32 i = 0; i < info.ncells; ++i) {
                    Cell cell;
                    if (node.read(i, cell)) {
                        return tree.corrupted_node(node.ref->page_id);
                    }
                    msg.append("  Cell(");
                    if (!node.is_leaf()) {
                        append_fmt_string(msg, "%u,", read_child_id(cell).value);
                    }
                    const auto key_len = std::min(32U, std::min(cell.key_size, cell.local_pl_size));
                    msg.append('"' + escape_string(Slice(cell.key, key_len)) + '"');
                    if (cell.key_size > key_len) {
                        append_fmt_string(msg, " + <%zu bytes>", cell.key_size - key_len);
                    }
                    msg.append(")\n");
                }
                repr_out.append(msg);
            }
            return Status::ok();
        };
        return InorderTraversal::traverse(tree, print);
    }
};

class TreeValidator
{
    using PageCallback = std::function<void(PageRef *&)>;

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

    [[nodiscard]] static auto get_readable_content(const PageRef &page, U32 size_limit) -> Slice
    {
        return Slice(page.page, kPageSize).range(kLinkContentOffset, std::min(size_limit, kLinkContentSize));
    }

public:
    static auto validate(const Tree &tree) -> void
    {
        auto check_parent_child = [&tree](auto &node, auto index) -> void {
            Node child;
            CHECK_OK(tree.acquire(node.read_child_id(index), child, false));

            Id parent_id;
            CHECK_OK(tree.find_parent_id(child.ref->page_id, parent_id));
            CHECK_TRUE(parent_id == node.ref->page_id);

            tree.release(std::move(child));
        };
        CHECK_OK(InorderTraversal::traverse(tree, [f = std::move(check_parent_child)](const auto &node, const auto &info) {
            if (info.idx == info.ncells) {
                return Status::ok();
            }

            if (!node.is_leaf()) {
                f(node, info.idx);
                // Rightmost child.
                if (info.idx + 1 == info.ncells) {
                    f(node, info.idx + 1);
                }
            }
            return Status::ok();
        }));

        CHECK_OK(InorderTraversal::traverse(tree, [&tree](const auto &node, const auto &info) {
            if (info.idx == info.ncells) {
                CHECK_TRUE(node.assert_state());
                return Status::ok();
            }
            Cell cell;
            CHECK_EQ(0, node.read(info.idx, cell));

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
            return Status::ok();
        }));
    }
};

auto Tree::TEST_validate() const -> void
{
    TreeValidator::validate(*this);
}

auto Tree::print_structure(std::string &repr_out) const -> Status
{
    return TreePrinter::print_structure(*this, repr_out);
}

auto Tree::print_nodes(std::string &repr_out) const -> Status
{
    return TreePrinter::print_nodes(*this, repr_out);
}

#undef CHECK_TRUE
#undef CHECK_EQ
#undef CHECK_OK

#else

auto Tree::TEST_validate() const -> void
{
}

#endif // CALICODB_TEST

auto Tree::new_cursor() -> Cursor *
{
    auto *c = new UserCursor(TreeCursor(*this));
    c->m_c.reset(m_pager->status());
    return c;
}

auto Tree::use_cursor(Cursor *c) const -> void
{
    if (m_last_c && c != m_last_c) {
        auto *uc = reinterpret_cast<UserCursor *>(m_last_c);
        if (uc->is_valid()) {
            uc->save_position();
        } else {
            uc->m_c.reset();
        }
    }
    if (c) {
        m_cursor->reset();
    }
    m_last_c = c;
}

} // namespace calicodb

#include "cursor_impl.h"
#include "node.h"
#include "pager/pager.h"
#include "tree.h"

namespace calicodb
{

[[nodiscard]] static auto default_error_status() -> Status
{
    return Status::not_found("cursor is invalid");
}

auto CursorImpl::is_valid() const -> bool
{
    return m_status.is_ok();
}

auto CursorImpl::status() const -> Status
{
    return m_status;
}

auto CursorImpl::fetch_payload() -> Status
{
    CALICO_EXPECT_EQ(m_key_size, 0);
    CALICO_EXPECT_EQ(m_value_size, 0);
    BPlusTreeInternal internal {*m_tree};

    Node node;
    CALICO_TRY(internal.acquire(node, m_loc.pid));

    Slice key, value;
    auto cell = read_cell(node, m_loc.index);
    auto s = m_tree->collect_key(m_key, cell, key);
    m_key_size = key.size();
    if (s.is_ok()) {
        s = m_tree->collect_value(m_value, cell, value);
        m_value_size = value.size();
    }
    internal.release(std::move(node));
    return s;
}

auto CursorImpl::key() const -> Slice
{
    CALICO_EXPECT_TRUE(is_valid());
    return Slice {m_key}.truncate(m_key_size);
}

auto CursorImpl::value() const -> Slice
{
    CALICO_EXPECT_TRUE(is_valid());
    return Slice {m_value}.truncate(m_value_size);
}

auto CursorImpl::seek_first() -> void
{
    m_key_size = 0;
    m_value_size = 0;

    Node lowest;
    auto s = m_tree->lowest(lowest);
    if (!s.is_ok()) {
        m_status = s;
        return;
    }
    if (lowest.header.cell_count) {
        seek_to(std::move(lowest), 0);
    } else {
        BPlusTreeInternal internal {*m_tree};
        internal.release(std::move(lowest));
        m_status = Status::not_found("database is empty");
    }
}

auto CursorImpl::seek_last() -> void
{
    m_key_size = 0;
    m_value_size = 0;

    Node highest;
    auto s = m_tree->highest(highest);
    if (!s.is_ok()) {
        m_status = s;
        return;
    }
    if (const auto count = highest.header.cell_count) {
        seek_to(std::move(highest), count - 1);
    } else {
        BPlusTreeInternal internal {*m_tree};
        internal.release(std::move(highest));
        m_status = Status::not_found("database is empty");
    }
}

auto CursorImpl::next() -> void
{
    CALICO_EXPECT_TRUE(is_valid());
    BPlusTreeInternal internal {*m_tree};
    m_key_size = 0;
    m_value_size = 0;

    Node node;
    auto s = internal.acquire(node, Id {m_loc.pid});
    if (!s.is_ok()) {
        m_status = s;
        return;
    }
    if (++m_loc.index < m_loc.count) {
        seek_to(std::move(node), m_loc.index);
        return;
    }
    const auto next_id = node.header.next_id;
    internal.release(std::move(node));

    if (next_id.is_null()) {
        m_status = default_error_status();
        return;
    }
    s = internal.acquire(node, next_id);
    if (!s.is_ok()) {
        m_status = s;
        return;
    }
    seek_to(std::move(node), 0);
}

auto CursorImpl::previous() -> void
{
    CALICO_EXPECT_TRUE(is_valid());
    BPlusTreeInternal internal {*m_tree};
    m_key_size = 0;
    m_value_size = 0;

    Node node;
    auto s = internal.acquire(node, m_loc.pid);
    if (!s.is_ok()) {
        m_status = s;
        return;
    }
    if (m_loc.index != 0) {
        seek_to(std::move(node), m_loc.index - 1);
        return;
    }
    const auto prev_id = node.header.prev_id;
    internal.release(std::move(node));

    if (prev_id.is_null()) {
        m_status = default_error_status();
        return;
    }
    s = internal.acquire(node, prev_id);
    if (!s.is_ok()) {
        m_status = s;
        return;
    }
    const auto count = node.header.cell_count;
    seek_to(std::move(node), count - 1);
}

auto CursorImpl::seek_to(Node node, Size index) -> void
{
    const auto &header = node.header;
    CALICO_EXPECT_TRUE(header.is_external);

    if (header.cell_count && index < header.cell_count) {
        m_loc.index = static_cast<PageSize>(index);
        m_loc.count = header.cell_count;
        m_loc.pid = node.page.id();
        m_status = fetch_payload();
    } else {
        m_status = default_error_status();
    }
    BPlusTreeInternal internal {*m_tree};
    internal.release(std::move(node));
}

auto CursorImpl::seek(const Slice &key) -> void
{
    m_key_size = 0;
    m_value_size = 0;

    SearchResult slot;
    auto s = m_tree->search(key, slot);
    if (!s.is_ok()) {
        m_status = s;
        return;
    }
    seek_to(std::move(slot.node), slot.index);
}

auto CursorInternal::make_cursor(BPlusTree &tree) -> Cursor *
{
    auto *cursor = new CursorImpl {tree};
    invalidate(*cursor, default_error_status());
    return cursor;
}

auto CursorInternal::invalidate(const Cursor &cursor, Status status) -> void
{
    CALICO_EXPECT_FALSE(status.is_ok());
    reinterpret_cast<const CursorImpl &>(cursor).m_status = std::move(status);
}

} // namespace calicodb

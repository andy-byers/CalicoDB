#include "cursor_impl.h"
#include "node.h"
#include "pager/pager.h"
#include "tree.h"

namespace Calico {

[[nodiscard]]
static auto default_error_status() -> Status
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

auto CursorImpl::fetch_key() const -> Status
{
    CALICO_EXPECT_EQ(m_key_size, 0);

    Node node;
    Calico_Try(m_tree->acquire(node, m_loc.pid));

    Slice key;
    auto cell = read_cell(node, m_loc.index);
    auto s = m_tree->collect_key(m_key, cell, key);

    m_key_size = key.size();
    // Go ahead and read the value if it is entirely local.
    if (m_value.empty() && !cell.has_remote) {
        m_value_size = cell.local_size - cell.key_size;
        if (m_value.size() < m_value_size) {
            m_value.resize(m_value_size);
        }
        const Slice value {cell.key + cell.key_size, m_value_size};
        mem_copy(m_value, value);
    }
    m_tree->release(std::move(node));
    return s;
}

auto CursorImpl::fetch_value() const -> Status
{
    CALICO_EXPECT_EQ(m_value_size, 0);

    Node node;
    Calico_Try(m_tree->acquire(node, m_loc.pid));

    Slice value;
    const auto cell = read_cell(node, m_loc.index);
    auto s = m_tree->collect_value(m_value, cell, value);

    m_value_size = value.size();
    m_tree->release(std::move(node));
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
        m_tree->release(std::move(lowest));
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
        m_tree->release(std::move(highest));
        m_status = Status::not_found("database is empty");
    }
}

auto CursorImpl::next() -> void
{
    CALICO_EXPECT_TRUE(is_valid());
    m_key_size = 0;
    m_value_size = 0;

    Node node;
    auto s = m_tree->acquire(node, Id {m_loc.pid});
    if (!s.is_ok()) {
        m_status = s;
        return;
    }
    if (++m_loc.index < m_loc.count) {
        seek_to(std::move(node), m_loc.index);
        return;
    }
    const auto next_id = node.header.next_id;
    m_tree->release(std::move(node));

    if (next_id.is_null()) {
        m_status = default_error_status();
        return;
    }
    s = m_tree->acquire(node, next_id);
    if (!s.is_ok()) {
        m_status = s;
        return;
    }
    seek_to(std::move(node), 0);
}

auto CursorImpl::previous() -> void
{
    CALICO_EXPECT_TRUE(is_valid());
    m_key_size = 0;
    m_value_size = 0;

    Node node;
    auto s = m_tree->acquire(node, m_loc.pid);
    if (!s.is_ok()) {
        m_status = s;
        return;
    }
    if (m_loc.index != 0) {
        seek_to(std::move(node), m_loc.index - 1);
        return;
    }
    const auto prev_id = node.header.prev_id;
    m_tree->release(std::move(node));

    if (prev_id.is_null()) {
        m_status = default_error_status();
        return;
    }
    s = m_tree->acquire(node, prev_id);
    if (!s.is_ok()) {
        m_status = s;
        return;
    }
    const auto count = node.header.cell_count;
    seek_to(std::move(node), count - 1);
}

auto CursorImpl::seek_to(Node node, Size index) -> void
{
    CALICO_EXPECT_EQ(m_key_size, 0);
    CALICO_EXPECT_EQ(m_value_size, 0);
    CALICO_EXPECT_TRUE(node.header.is_external);

    const auto pid = node.page.id();
    const auto count = node.header.cell_count;

    if (count && index < count) {
        m_loc.index = static_cast<PageSize>(index);
        m_loc.count = static_cast<PageSize>(count);
        m_loc.pid = pid;

        auto s = fetch_key();
        if (s.is_ok() && m_value_size == 0) {
            s = fetch_value();
        }
        m_status = s;
    } else {
        m_status = default_error_status();
    }
    m_tree->release(std::move(node));
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

auto CursorInternal::invalidate(const Cursor &cursor, const Status &status) -> void
{
    CALICO_EXPECT_FALSE(status.is_ok());
    reinterpret_cast<const CursorImpl &>(cursor).m_status = status;
}

auto CursorInternal::TEST_validate(const Cursor &cursor) -> void
{
    if (cursor.is_valid()) {
        const auto &impl = reinterpret_cast<const CursorImpl &>(cursor);
        Node node;
        auto s = impl.m_tree->acquire(node, impl.m_loc.pid);
        if (!s.is_ok()) {
            std::fputs(s.what().data(), stderr);
            std::abort();
        }
        node.TEST_validate();
        impl.m_tree->release(std::move(node));
    }
}

} // namespace Calico

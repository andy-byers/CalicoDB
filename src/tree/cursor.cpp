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
    if (m_key_size) {
        return Status::ok();
    }
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
    if (m_value_size) {
        return Status::ok();
    }
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
    m_status = fetch_key();
    return Slice {m_key}.truncate(m_key_size);
}

auto CursorImpl::value() const -> Slice
{
    CALICO_EXPECT_TRUE(is_valid());
    m_status = fetch_value();
    return Slice {m_value}.truncate(m_value_size);
}

auto CursorImpl::seek_first() -> void
{
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
    if (++m_loc.index < m_loc.count) {
        return;
    }
    Node node;
    auto s = m_tree->acquire(node, Id {m_loc.pid});
    if (!s.is_ok()) {
        m_status = s;
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
    m_loc.pid = node.page.id();
    m_loc.count = node.header.cell_count;
    m_loc.index = 0;
    m_tree->release(std::move(node));
}

auto CursorImpl::previous() -> void
{
    if (!is_valid()) {
        return;
    }
    m_key_size = 0;
    m_value_size = 0;
    if (m_loc.index != 0) {
        m_loc.index--;
        return;
    }
    Node node;
    auto s = m_tree->acquire(node, m_loc.pid);
    if (!s.is_ok()) {
        m_status = s;
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
    m_loc.pid = node.page.id();
    m_loc.count = node.header.cell_count;
    m_loc.index = node.header.cell_count - 1;
    m_tree->release(std::move(node));
}

auto CursorImpl::seek_to(Node node, Size index) -> void
{
    CALICO_EXPECT_TRUE(node.header.is_external);
    const auto pid = node.page.id();
    const auto count = node.header.cell_count;
    m_tree->release(std::move(node));

    m_key_size = 0;
    m_value_size = 0;

    if (count && index < count) {
        m_loc.index = static_cast<PageSize>(index);
        m_loc.count = static_cast<PageSize>(count);
        m_loc.pid = pid;
        m_status = Status::ok();
    } else {
        m_status = default_error_status();
    }
}

auto CursorImpl::seek(const Slice &key) -> void
{
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
    auto *cursor = new(std::nothrow) CursorImpl {tree};
    if (cursor != nullptr) {
        invalidate(*cursor, default_error_status());
    }
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

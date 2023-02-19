#include "cursor_internal.h"
#include "node.h"
#include "tree.h"
#include "pager/pager.h"

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

auto CursorImpl::key() const -> Slice
{
    CALICO_EXPECT_TRUE(is_valid());
    if (!m_key.empty()) {
        return m_key;
    }
    if (auto node = CursorInternal::action_acquire(*this, m_loc.pid)) {
        auto cell = read_cell(*node, m_loc.index);
        auto r = m_tree->collect_key(cell);
        if (!r.has_value()) {
            m_status = r.error();
            return {};
        }
        m_key = *r;
        if (m_value.empty() && !cell.has_remote) {
            m_value = Slice {
                cell.key + cell.key_size,
                get_u32(cell.ptr),
            }.to_string();
        }
        CursorInternal::action_release(*this, std::move(*node));
        return m_key;
    } else {
        CursorInternal::invalidate(*this, node.error());
        return {};
    }
}

auto CursorImpl::value() const -> Slice
{
    CALICO_EXPECT_TRUE(is_valid());
    if (!m_value.empty()) {
        return m_value;
    }
    if (auto node = CursorInternal::action_acquire(*this, m_loc.pid)) {
        if (auto value = CursorInternal::action_collect(*this, std::move(*node), m_loc.index)) {
            m_value = *value;
            return m_value;
        } else {
            CursorInternal::invalidate(*this, value.error());
        }
    } else {
        CursorInternal::invalidate(*this, node.error());
    }
    return {};
}

auto CursorImpl::seek(const Slice &key) -> void
{
    return CursorInternal::seek(*this, key);
}

auto CursorImpl::seek_first() -> void
{
    return CursorInternal::seek_first(*this);
}

auto CursorImpl::seek_last() -> void
{
    return CursorInternal::seek_last(*this);
}

auto CursorImpl::next() -> void
{
    return CursorInternal::seek_right(*this);
}

auto CursorImpl::previous() -> void
{
    return CursorInternal::seek_left(*this);
}

auto CursorInternal::action_collect(const CursorImpl &cursor, Node node, Size index) -> tl::expected<std::string, Status>
{
    Calico_New_R(value, cursor.m_tree->collect_value(read_cell(node, index)));
    cursor.m_tree->m_pager->release(std::move(node.page));
    return value;
}

auto CursorInternal::action_acquire(const CursorImpl &cursor, Id pid) -> tl::expected<Node, Status>
{
    return cursor.m_tree->acquire(pid, false);
}

auto CursorInternal::action_search(const CursorImpl &cursor, const Slice &key) -> tl::expected<SearchResult, Status>
{
    return cursor.m_tree->search(key);
}

auto CursorInternal::action_lowest(const CursorImpl &cursor) -> tl::expected<Node, Status>
{
    return cursor.m_tree->lowest();
}

auto CursorInternal::action_highest(const CursorImpl &cursor) -> tl::expected<Node, Status>
{
    return cursor.m_tree->highest();
}

auto CursorInternal::action_release(const CursorImpl &cursor, Node node) -> void
{
    cursor.m_tree->release(std::move(node));
}

auto CursorInternal::seek_first(CursorImpl &cursor) -> void
{
    if (auto lowest = action_lowest(cursor)) {
        if (lowest->header.cell_count) {
            seek_to(cursor, std::move(*lowest), 0);
        } else {
            invalidate(cursor, Status::not_found("database is empty"));
            action_release(cursor, std::move(*lowest));
        }
    } else {
        invalidate(cursor, lowest.error());
    }
}

auto CursorInternal::seek_last(CursorImpl &cursor) -> void
{
    if (auto highest = action_highest(cursor)) {
        if (const auto count = highest->header.cell_count) {
            seek_to(cursor, std::move(*highest), count - 1);
        } else {
            invalidate(cursor, Status::not_found("database is empty"));
            action_release(cursor, std::move(*highest));
        }
    } else {
        invalidate(cursor, highest.error());
    }
}

auto CursorInternal::seek_left(CursorImpl &cursor) -> void
{
    CALICO_EXPECT_TRUE(cursor.is_valid());
    cursor.m_key.clear();
    cursor.m_value.clear();
    if (cursor.m_loc.index != 0) {
        cursor.m_loc.index--;
    } else if (auto node = action_acquire(cursor, Id {cursor.m_loc.pid})) {
        const auto prev_id = node->header.prev_id;
        action_release(cursor, std::move(*node));

        if (prev_id.is_null()) {
            invalidate(cursor, default_error_status());
            return;
        }
        node = action_acquire(cursor, prev_id);
        if (!node.has_value()) {
            invalidate(cursor, node.error());
            return;
        }
        cursor.m_loc.pid = node->page.id();
        cursor.m_loc.count = node->header.cell_count;
        cursor.m_loc.index = node->header.cell_count - 1;
        action_release(cursor, std::move(*node));
    } else {
        invalidate(cursor, node.error());
    }
}

auto CursorInternal::seek_right(CursorImpl &cursor) -> void
{
    CALICO_EXPECT_TRUE(cursor.is_valid());
    cursor.m_key.clear();
    cursor.m_value.clear();
    if (++cursor.m_loc.index < cursor.m_loc.count) {
        return;
    }
    if (auto node = action_acquire(cursor, Id {cursor.m_loc.pid})) {
        const auto next_id = node->header.next_id;
        action_release(cursor, std::move(*node));

        if (next_id.is_null()) {
            invalidate(cursor, default_error_status());
            return;
        }
        node = action_acquire(cursor, next_id);
        if (!node.has_value()) {
            invalidate(cursor, node.error());
            return;
        }
        cursor.m_loc.pid = node->page.id();
        cursor.m_loc.count = node->header.cell_count;
        cursor.m_loc.index = 0;
        action_release(cursor, std::move(*node));
    } else {
        invalidate(cursor, node.error());
    }
}

auto CursorInternal::seek_to(CursorImpl &cursor, Node node, Size index) -> void
{
    CALICO_EXPECT_TRUE(node.header.is_external);
    const auto pid = node.page.id();
    const auto count = node.header.cell_count;
    action_release(cursor, std::move(node));

    cursor.m_key.clear();
    cursor.m_value.clear();

    if (count && index < count) {
        cursor.m_loc.index = static_cast<PageSize>(index);
        cursor.m_loc.count = static_cast<PageSize>(count);
        cursor.m_loc.pid = pid;
        cursor.m_status = Status::ok();
    } else {
        invalidate(cursor, default_error_status());
    }
}

auto CursorInternal::seek(CursorImpl &cursor, const Slice &key) -> void
{
    if (auto slot = action_search(cursor, key)) {
        auto [node, index, _] = std::move(*slot);
        seek_to(cursor, std::move(node), index);
    } else {
        invalidate(cursor, slot.error());
    }
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
        auto node = action_acquire(impl, Id {impl.m_loc.pid}).value();
        node.TEST_validate();
        action_release(impl, std::move(node));
    }
}

} // namespace Calico

#include "cursor_internal.h"
#include "bplus_tree.h"
#include "node.h"

namespace Calico {

[[nodiscard]]
static auto default_error_status() -> Status
{
    return Status::not_found("cursor is invalid");
}

auto CursorInternal::action_collect(const Cursor &cursor, Node node, Size index) -> tl::expected<std::string, Status>
{
    return cursor.m_actions->collect(*cursor.m_actions->tree, std::move(node), index);
}

auto CursorInternal::action_acquire(const Cursor &cursor, Id pid) -> tl::expected<Node, Status>
{
    return cursor.m_actions->acquire(*cursor.m_actions->tree, pid, false);
}

auto CursorInternal::action_search(const Cursor &cursor, const Slice &key) -> tl::expected<SearchResult, Status>
{
    return cursor.m_actions->search(*cursor.m_actions->tree, key);
}

auto CursorInternal::action_lowest(const Cursor &cursor) -> tl::expected<Node, Status>
{
    return cursor.m_actions->lowest(*cursor.m_actions->tree);
}

auto CursorInternal::action_highest(const Cursor &cursor) -> tl::expected<Node, Status>
{
    return cursor.m_actions->highest(*cursor.m_actions->tree);
}

auto CursorInternal::action_release(const Cursor &cursor, Node node) -> void
{
    cursor.m_actions->release(*cursor.m_actions->tree, std::move(node));
}

auto CursorInternal::seek_first(Cursor &cursor) -> void
{
    if (auto lowest = action_lowest(cursor)) {
        if (lowest->header.cell_count) {
            seek_to(cursor, std::move(*lowest), 0);
        } else {
            invalidate(cursor, not_found("database is empty"));
            action_release(cursor, std::move(*lowest));
        }
    } else {
        invalidate(cursor, lowest.error());
    }
}

auto CursorInternal::seek_last(Cursor &cursor) -> void
{
    if (auto highest = action_highest(cursor)) {
        if (const auto count = highest->header.cell_count) {
            seek_to(cursor, std::move(*highest), count - 1);
        } else {
            invalidate(cursor, not_found("database is empty"));
            action_release(cursor, std::move(*highest));
        }
    } else {
        invalidate(cursor, highest.error());
    }
}

auto CursorInternal::seek_left(Cursor &cursor) -> void
{
    CALICO_EXPECT_TRUE(cursor.is_valid());
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
        cursor.m_loc.pid = node->page.id().value;
        cursor.m_loc.count = node->header.cell_count;
        cursor.m_loc.index = node->header.cell_count - 1;
        action_release(cursor, std::move(*node));
    } else {
        invalidate(cursor, node.error());
    }
}

auto CursorInternal::seek_right(Cursor &cursor) -> void
{
    CALICO_EXPECT_TRUE(cursor.is_valid());
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
        cursor.m_loc.pid = node->page.id().value;
        cursor.m_loc.count = node->header.cell_count;
        cursor.m_loc.index = 0;
        action_release(cursor, std::move(*node));
    } else {
        invalidate(cursor, node.error());
    }
}

auto CursorInternal::seek_to(Cursor &cursor, Node node, Size index) -> void
{
    CALICO_EXPECT_TRUE(node.header.is_external);
    const auto pid = node.page.id();
    const auto count = node.header.cell_count;
    action_release(cursor, std::move(node));

    if (count && index < count) {
        cursor.m_loc.index = static_cast<PageSize>(index);
        cursor.m_loc.count = static_cast<PageSize>(count);
        cursor.m_loc.pid = pid.value;
        cursor.m_status = ok();
    } else {
        invalidate(cursor, default_error_status());
    }
}

auto CursorInternal::seek(Cursor &cursor, const Slice &key) -> void
{
    if (auto slot = action_search(cursor, key)) {
        auto [node, index, exact] = std::move(*slot);
        const auto count = node.header.cell_count;
        const auto pid = node.page.id();
        action_release(cursor, std::move(node));

        if (count && index < count) {
            cursor.m_loc.index = static_cast<PageSize>(index);
            cursor.m_loc.count = static_cast<PageSize>(count);
            cursor.m_loc.pid = pid.value;
            cursor.m_status = ok();
        } else {
            invalidate(cursor, default_error_status());
        }
    } else {
        invalidate(cursor, slot.error());
    }
}

auto CursorInternal::make_cursor(BPlusTree &tree) -> Cursor
{
    Cursor cursor;
    cursor.m_actions = &tree.m_actions;
    invalidate(cursor, default_error_status());
    return cursor;
}

auto CursorInternal::invalidate(const Cursor &cursor, const Status &status) -> void
{
    CALICO_EXPECT_FALSE(status.is_ok());
    cursor.m_status = status;
}

auto CursorInternal::TEST_validate(const Cursor &cursor) -> void
{
    if (cursor.is_valid()) {
        auto node = action_acquire(cursor, Id {cursor.m_loc.pid}).value();
        node.TEST_validate();
        action_release(cursor, std::move(node));
    }
}

auto Cursor::operator==(const Cursor &rhs) const -> bool
{
    // These cursors should come from the same database.
    CALICO_EXPECT_EQ(m_actions, rhs.m_actions);
    const auto lhs_has_error = !m_status.is_ok() && !m_status.is_not_found();
    const auto rhs_has_error = !rhs.m_status.is_ok() && !rhs.m_status.is_not_found();

    if (m_status.is_ok() && rhs.m_status.is_ok()) {
        return m_loc == rhs.m_loc;
    } else if (lhs_has_error || rhs_has_error) {
        // A cursor in an exceptional state is never equal to another cursor.
        return false;
    } else if (m_status.is_not_found() || rhs.m_status.is_not_found()) {
        return m_status.is_not_found() == rhs.m_status.is_not_found();
    }
    return false;
}

auto Cursor::operator!=(const Cursor &rhs) const -> bool
{
    return !(*this == rhs);
}

auto Cursor::is_valid() const -> bool
{
    return m_status.is_ok();
}

auto Cursor::status() const -> Status
{
    return m_status;
}

auto Cursor::seek(const Slice &key) -> void
{
    CursorInternal::seek(*this, key);
}

auto Cursor::seek_first() -> void
{
    CursorInternal::seek_first(*this);
}

auto Cursor::seek_last() -> void
{
    CursorInternal::seek_last(*this);
}

auto Cursor::next() -> void
{
    CursorInternal::seek_right(*this);
}

auto Cursor::previous() -> void
{
    CursorInternal::seek_left(*this);
}

auto Cursor::key() const -> Slice
{
    CALICO_EXPECT_TRUE(is_valid());
    if (auto node = CursorInternal::action_acquire(*this, Id {m_loc.pid})) {
        m_buffer = read_key(*node, m_loc.index).to_string();
        CursorInternal::action_release(*this, std::move(*node));
        return m_buffer;
    } else {
        m_status = node.error();
        return {};
    }
}

auto Cursor::value() const -> Slice
{
    CALICO_EXPECT_TRUE(is_valid());
    if (auto node = CursorInternal::action_acquire(*this, Id {m_loc.pid})) {
        if (auto value = CursorInternal::action_collect(*this, std::move(*node), m_loc.index)) {
            m_buffer = *value;
            return m_buffer;
        } else {
            CursorInternal::invalidate(*this, value.error());
        }
    } else {
        CursorInternal::invalidate(*this, node.error());
    }
    return {};
}

auto Cursor::Position::operator==(const Position &rhs) const -> bool
{
    if (pid == rhs.pid) {
        CALICO_EXPECT_EQ(count, rhs.count);
        return index == rhs.index;
    }
    return false;
}

} // namespace Calico

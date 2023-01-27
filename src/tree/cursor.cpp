#include "cursor_internal.h"
#include "bplus_tree.h"
#include "node.h"

namespace Calico {

[[nodiscard]]
static auto default_error_status() -> Status
{
    return Status::not_found("cursor is invalid");
}

auto CursorActions::collect(Node node, Size index) -> tl::expected<std::string, Status>
{
    return collect_ptr(*tree_ptr, std::move(node), index);
}

auto CursorActions::acquire(Id pid, bool upgrade) -> tl::expected<Node, Status>
{
    return acquire_ptr(*tree_ptr, pid, upgrade);
}

auto CursorActions::release(Node node) -> void
{
    release_ptr(*tree_ptr, std::move(node));
}

auto CursorInternal::find_first(BPlusTree &tree) -> Cursor
{
    auto cursor = make_cursor(tree);
    if (auto lowest = tree.lowest()) {
        if (lowest->header.cell_count) {
            move_to(cursor, std::move(*lowest), 0);
        } else {
            invalidate(cursor, not_found("database is empty"));
        }
        tree.m_actions.release(std::move(*lowest));
    } else {
        invalidate(cursor, lowest.error());
    }
    return cursor;
}

auto CursorInternal::find_last(BPlusTree &tree) -> Cursor
{
    auto cursor = make_cursor(tree);
    if (auto highest = tree.highest()) {
        if (const auto count = highest->header.cell_count) {
            move_to(cursor, std::move(*highest), count - 1);
        } else {
            invalidate(cursor, not_found("database is empty"));
        }
        tree.m_actions.release(std::move(*highest));
    } else {
        invalidate(cursor, highest.error());
    }
    return cursor;
}

auto CursorInternal::make_cursor(BPlusTree &tree) -> Cursor
{
    Cursor cursor;
    cursor.m_actions = &tree.m_actions;
    invalidate(cursor, default_error_status());
    return cursor;
}

auto CursorInternal::id(const Cursor &cursor) -> Size
{
    CALICO_EXPECT_TRUE(cursor.is_valid());
    return cursor.m_position.ids[Cursor::Position::CENTER];
}

auto CursorInternal::index(const Cursor &cursor) -> Size
{
    CALICO_EXPECT_TRUE(cursor.is_valid());
    return cursor.m_position.index;
}

auto CursorInternal::invalidate(Cursor &cursor, const Status &status) -> void
{
    CALICO_EXPECT_FALSE(status.is_ok());
    cursor.m_status = status;
}

auto CursorInternal::seek_left(Cursor &cursor) -> bool
{
    CALICO_EXPECT_TRUE(cursor.is_valid());
    CALICO_EXPECT_EQ(cursor.m_position.index, 0);
    if (is_first(cursor)) {
        invalidate(cursor, default_error_status());
    } else {
        const Id left {cursor.m_position.ids[Cursor::Position::LEFT]};
        auto previous = cursor.m_actions->acquire(left, false);
        if (!previous.has_value()) {
            invalidate(cursor, previous.error());
            return false;
        }
        move_to(cursor, std::move(*previous), 0);
        CALICO_EXPECT_GT(cursor.m_position.cell_count, 0);
        cursor.m_position.index = cursor.m_position.cell_count;
        cursor.m_position.index--;
    }
    return true;
}

auto CursorInternal::seek_right(Cursor &cursor) -> bool
{
    CALICO_EXPECT_TRUE(cursor.is_valid());
    CALICO_EXPECT_EQ(cursor.m_position.index, cursor.m_position.cell_count - 1);
    if (is_last(cursor)) {
        invalidate(cursor, default_error_status());
    } else {
        const Id right {cursor.m_position.ids[Cursor::Position::RIGHT]};
        auto next = cursor.m_actions->acquire(right, false);
        if (!next.has_value()) {
            invalidate(cursor, next.error());
            return false;
        }
        move_to(cursor, std::move(*next), 0);
    }
    return true;
}

auto CursorInternal::is_last(const Cursor &cursor) -> bool
{
    return cursor.is_valid() && cursor.m_position.is_maximum();
}

auto CursorInternal::is_first(const Cursor &cursor) -> bool
{
    return cursor.is_valid() && cursor.m_position.is_minimum();
}

auto CursorInternal::TEST_validate(const Cursor &cursor) -> void
{
    if (cursor.is_valid()) {
        auto node = cursor.m_actions->acquire(Id {cursor.m_position.ids[Cursor::Position::CENTER]}, false);
        CALICO_EXPECT_TRUE(node.has_value());
        node->TEST_validate();
    }
}

auto Cursor::operator==(const Cursor &rhs) const -> bool
{
    // These cursors should come from the same database.
    CALICO_EXPECT_EQ(m_actions, rhs.m_actions);
    const auto lhs_has_error = !m_status.is_ok() && !m_status.is_not_found();
    const auto rhs_has_error = !rhs.m_status.is_ok() && !rhs.m_status.is_not_found();

    if (m_status.is_ok() && rhs.m_status.is_ok()) {
        return m_position == rhs.m_position;
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
    (void)key;
}

auto Cursor::seek_first() -> void
{

}

auto Cursor::seek_last() -> void
{

}

auto CursorInternal::move_to(Cursor &cursor, Node node, Size index) -> void
{
    CALICO_EXPECT_TRUE(node.header.is_external);
    const auto count = node.header.cell_count;
    auto is_valid = count && index < count;

    if (is_valid) {
        cursor.m_position.index = static_cast<std::uint16_t>(index);
        cursor.m_position.cell_count = static_cast<std::uint16_t>(count);
        cursor.m_position.ids[Cursor::Position::LEFT] = node.header.prev_id.value;
        cursor.m_position.ids[Cursor::Position::CENTER] = node.page.id().value;
        cursor.m_position.ids[Cursor::Position::RIGHT] = node.header.next_id.value;
        cursor.m_status = ok();
    } else {
        invalidate(cursor, default_error_status());
    }

    cursor.m_actions->release(std::move(node));
}

auto Cursor::next() -> void
{
    if (is_valid()) {
        if (m_position.index == m_position.cell_count - 1) {
            CursorInternal::seek_right(*this);
            return;
        } else {
            m_position.index++;
        }
    }
}

auto Cursor::previous() -> void
{
    if (is_valid()) {
        if (m_position.index == 0) {
            CursorInternal::seek_left(*this);
            return;
        } else {
            m_position.index--;
        }
    }
}

auto Cursor::key() const -> Slice
{
    CALICO_EXPECT_TRUE(is_valid());
    const auto node = m_actions->acquire(Id {m_position.ids[Position::CENTER]}, false);
    if (!node.has_value()) {
        m_status = node.error();
        return {};
    }
    return read_key(*node, m_position.index);
}

auto Cursor::value() const -> Slice
{
    CALICO_EXPECT_TRUE(is_valid());
    auto node = m_actions->acquire(Id {m_position.ids[Position::CENTER]}, false);
    if (!node.has_value()) {
        m_status = node.error();
        return {};
    }
    return *m_actions->collect(std::move(*node), m_position.index)
        .map_error([this](const Status &status) -> std::string {
            m_status = status;
            return {};
        });
}

auto Cursor::Position::operator==(const Position &rhs) const -> bool
{
    if (ids[CENTER] == rhs.ids[CENTER]) {
        CALICO_EXPECT_EQ(ids[LEFT], rhs.ids[LEFT]);
        CALICO_EXPECT_EQ(ids[RIGHT], rhs.ids[RIGHT]);
        CALICO_EXPECT_EQ(cell_count, rhs.cell_count);
        return index == rhs.index;
    }
    return false;
}

auto Cursor::Position::is_maximum() const -> bool
{
    CALICO_EXPECT_NE(ids[CENTER], 0);
    return Id {ids[RIGHT]}.is_null() && index + 1 == cell_count;
}

auto Cursor::Position::is_minimum() const -> bool
{
    CALICO_EXPECT_NE(ids[CENTER], 0);
    return cell_count && Id {ids[LEFT]}.is_null() && index == 0;
}

} // namespace Calico

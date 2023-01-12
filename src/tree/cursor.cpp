#include "cursor_internal.h"
#include "pager/pager.h"
#include "tree/internal.h"

namespace Calico {

auto CursorInternal::make_cursor(CursorActions *actions) -> Cursor
{
    Cursor cursor;
    cursor.m_actions = actions;
    invalidate(cursor);
    return cursor;
}

auto CursorInternal::id(const Cursor &cursor) -> Size
{
    CALICO_EXPECT_TRUE(cursor.is_valid());
    return cursor.m_position.ids[Cursor::Position::CURRENT];
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
        invalidate(cursor);
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
        invalidate(cursor);
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
    if (!cursor.is_valid())
        return;

    auto node = cursor.m_actions->acquire(Id {cursor.m_position.ids[Cursor::Position::CURRENT]}, false);
    CALICO_EXPECT_TRUE(node.has_value());
    node->TEST_validate();
}

auto Cursor::operator==(const Cursor &rhs) const -> bool
{
    // These cursors should come from the same database.
    CALICO_EXPECT_EQ(m_actions, rhs.m_actions);
    const auto lhs_has_error = !m_status.is_ok() && !m_status.is_not_found();
    const auto rhs_has_error = !rhs.m_status.is_ok() && !rhs.m_status.is_not_found();

    if (m_status.is_ok() && rhs.m_status.is_ok()) {
        return m_position == rhs.m_position;
        // A cursor in an exceptional state is never equal to another cursor.
    } else if (lhs_has_error || rhs_has_error) {
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

auto Cursor::operator++() -> Cursor &
{
    increment();
    return *this;
}

auto Cursor::operator++(int) -> Cursor
{
    const auto temp = *this;
    ++*this;
    return temp;
}

auto Cursor::operator--() -> Cursor &
{
    decrement();
    return *this;
}

auto Cursor::operator--(int) -> Cursor
{
    const auto temp = *this;
    --*this;
    return temp;
}

auto Cursor::is_valid() const -> bool
{
    return m_status.is_ok();
}

auto Cursor::status() const -> Status
{
    return m_status;
}

auto CursorInternal::move_to(Cursor &cursor, Node node, Size index) -> void
{
    CALICO_EXPECT_TRUE(node.is_external());
    const auto count = node.cell_count();
    auto is_valid = count && index < count;

    if (is_valid) {
        cursor.m_position.index = static_cast<std::uint16_t>(index);
        cursor.m_position.cell_count = static_cast<std::uint16_t>(count);
        cursor.m_position.ids[Cursor::Position::LEFT] = node.left_sibling_id().value;
        cursor.m_position.ids[Cursor::Position::CURRENT] = node.id().value;
        cursor.m_position.ids[Cursor::Position::RIGHT] = node.right_sibling_id().value;
        cursor.m_status = ok();
    } else {
        invalidate(cursor);
    }

    if (auto r = cursor.m_actions->release(std::move(node)); !r.has_value())
        invalidate(cursor, r.error());
}

auto Cursor::increment() -> bool
{
    if (is_valid()) {
        if (m_position.index == m_position.cell_count - 1) {
            return CursorInternal::seek_right(*this);
        } else {
            m_position.index++;
        }
        return true;
    }
    return false;
}

auto Cursor::decrement() -> bool
{
    if (is_valid()) {
        if (m_position.index == 0) {
            return CursorInternal::seek_left(*this);
        } else {
            m_position.index--;
        }
        return true;
    }
    return false;
}

auto Cursor::key() const -> Slice
{
    CALICO_EXPECT_TRUE(is_valid());
    const auto node = m_actions->acquire(Id {m_position.ids[Position::CURRENT]}, false);
    if (!node.has_value()) {
        m_status = node.error();
        return {};
    }
    return node->read_key(m_position.index);
}

auto Cursor::value() const -> std::string
{
    CALICO_EXPECT_TRUE(is_valid());
    const auto node = m_actions->acquire(Id {m_position.ids[Position::CURRENT]}, false);
    if (!node.has_value()) {
        m_status = node.error();
        return {};
    }
    return *m_actions->collect(*node, m_position.index)
        .map_error([this](const Status &status) -> std::string {
            m_status = status;
            return {};
        });
}

auto Cursor::Position::operator==(const Position &rhs) const -> bool
{
    if (ids[CURRENT] == rhs.ids[CURRENT]) {
        CALICO_EXPECT_EQ(ids[LEFT], rhs.ids[LEFT]);
        CALICO_EXPECT_EQ(ids[RIGHT], rhs.ids[RIGHT]);
        CALICO_EXPECT_EQ(cell_count, rhs.cell_count);
        return index == rhs.index;
    }
    return false;
}

auto Cursor::Position::is_maximum() const -> bool
{
    CALICO_EXPECT_NE(ids[CURRENT], 0);
    return Id {ids[RIGHT]}.is_null() && index + 1 == cell_count;
}

auto Cursor::Position::is_minimum() const -> bool
{
    CALICO_EXPECT_NE(ids[CURRENT], 0);
    return cell_count && Id {ids[LEFT]}.is_null() && index == 0;
}

} // namespace Calico

#include "cursor_internal.h"
#include "pager/pager.h"
#include "tree/internal.h"
#include "tree/node_pool.h"
#include <spdlog/fmt/fmt.h>

namespace cco {

auto CursorInternal::make_cursor(NodePool &pager, Internal &internal) -> Cursor
{
    Cursor cursor;
    cursor.m_pager = &pager;
    cursor.m_internal = &internal;
    invalidate(cursor);
    return cursor;
}

auto CursorInternal::id(const Cursor &cursor) -> Index
{
    CCO_EXPECT_TRUE(cursor.is_valid());
    return cursor.m_position.ids[Cursor::Position::CURRENT];
}

auto CursorInternal::index(const Cursor &cursor) -> Index
{
    CCO_EXPECT_TRUE(cursor.is_valid());
    return cursor.m_position.index;
}

auto CursorInternal::invalidate(Cursor &cursor, const Status &status) -> void
{
    CCO_EXPECT_FALSE(status.is_ok());
    cursor.m_status = status;
}

auto CursorInternal::seek_left(Cursor &cursor) -> bool
{
    CCO_EXPECT_TRUE(cursor.is_valid());
    CCO_EXPECT_EQ(cursor.m_position.index, 0);
    if (cursor.is_minimum()) {
        invalidate(cursor);
    } else {
        const PageId left {cursor.m_position.ids[Cursor::Position::LEFT]};
        auto previous = cursor.m_pager->acquire(left, false);
        if (!previous.has_value()) {
            invalidate(cursor, previous.error());
            return false;
        }
        move_to(cursor, std::move(*previous), 0);
        CCO_EXPECT_GT(cursor.m_position.cell_count, 0);
        cursor.m_position.index = cursor.m_position.cell_count;
        cursor.m_position.index--;
    }
    return true;
}

auto CursorInternal::seek_right(Cursor &cursor) -> bool
{
    CCO_EXPECT_TRUE(cursor.is_valid());
    CCO_EXPECT_EQ(cursor.m_position.index, cursor.m_position.cell_count - 1);
    if (cursor.is_maximum()) {
        invalidate(cursor);
    } else {
        const PageId right {cursor.m_position.ids[Cursor::Position::RIGHT]};
        auto next = cursor.m_pager->acquire(right, false);
        if (!next.has_value()) {
            invalidate(cursor, next.error());
            return false;
        }
        move_to(cursor, std::move(*next), 0);
    }
    return true;
}

auto CursorInternal::TEST_validate(const Cursor &cursor) -> void
{
    if (!cursor.is_valid())
        return;

    auto node = cursor.m_pager->acquire(PageId {cursor.m_position.ids[Cursor::Position::CURRENT]}, false);
    node->TEST_validate();
}

auto Cursor::operator==(const Cursor &rhs) const -> bool
{
    // These cursors should come from the same database.
    CCO_EXPECT_EQ(m_pager, rhs.m_pager);
    CCO_EXPECT_EQ(m_internal, rhs.m_internal);
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

auto Cursor::is_maximum() const -> bool
{
    return is_valid() && m_position.is_maximum();
}

auto Cursor::is_minimum() const -> bool
{
    return is_valid() && m_position.is_minimum();
}

auto CursorInternal::move_to(Cursor &cursor, Node node, Index index) -> void
{
    CCO_EXPECT_TRUE(node.is_external());
    const auto count = node.cell_count();
    auto is_valid = count && index < count;

    if (is_valid) {
        cursor.m_position.index = static_cast<std::uint16_t>(index);
        cursor.m_position.cell_count = static_cast<std::uint16_t>(count);
        cursor.m_position.ids[Cursor::Position::LEFT] = node.left_sibling_id().value;
        cursor.m_position.ids[Cursor::Position::CURRENT] = node.id().value;
        cursor.m_position.ids[Cursor::Position::RIGHT] = node.right_sibling_id().value;
        cursor.m_status = Status::ok();
    } else {
        invalidate(cursor);
    }

    if (auto result = cursor.m_pager->release(std::move(node)); !result.has_value())
        invalidate(cursor, result.error());
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

auto Cursor::key() const -> BytesView
{
    CCO_EXPECT_TRUE(is_valid());
    const auto node = m_pager->acquire(PageId {m_position.ids[Position::CURRENT]}, false);
    if (!node.has_value()) {
        m_status = node.error();
        return {};
    }
    return node->read_key(m_position.index);
}

auto Cursor::value() const -> std::string
{
    CCO_EXPECT_TRUE(is_valid());
    const auto node = m_pager->acquire(PageId {m_position.ids[Position::CURRENT]}, false);
    if (!node.has_value()) {
        m_status = node.error();
        return {};
    }
    return *m_internal->collect_value(*node, m_position.index)
                .map_error([this](const Status &status) -> std::string {
                    m_status = status;
                    return {};
                });
}

auto Cursor::record() const -> Record
{
    CCO_EXPECT_TRUE(is_valid());
    const auto node = m_pager->acquire(PageId {m_position.ids[Position::CURRENT]}, false);
    if (!node.has_value()) {
        m_status = node.error();
        return {};
    }
    auto value = m_internal->collect_value(*node, m_position.index);
    if (!value.has_value()) {
        m_status = node.error();
        return {};
    }
    return {btos(node->read_key(m_position.index)), std::move(*value)};
}

auto Cursor::Position::operator==(const Position &rhs) const -> bool
{
    if (ids[CURRENT] == rhs.ids[CURRENT]) {
        CCO_EXPECT_EQ(ids[LEFT], rhs.ids[LEFT]);
        CCO_EXPECT_EQ(ids[RIGHT], rhs.ids[RIGHT]);
        CCO_EXPECT_EQ(cell_count, rhs.cell_count);
        return index == rhs.index;
    }
    return false;
}

auto Cursor::Position::is_maximum() const -> bool
{
    CCO_EXPECT_NE(ids[CURRENT], 0);
    return PageId {ids[RIGHT]}.is_null() && index + 1 == cell_count;
}

auto Cursor::Position::is_minimum() const -> bool
{
    CCO_EXPECT_NE(ids[CURRENT], 0);
    return cell_count && PageId {ids[LEFT]}.is_null() && index == 0;
}

} // namespace cco

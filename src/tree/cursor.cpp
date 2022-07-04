#include "calico/cursor.h"
#include "pool/interface.h"
#include "tree/internal.h"
#include "tree/node_pool.h"

namespace calico {

Cursor::Cursor(NodePool *pool, Internal *internal)
    : m_pool {pool},
      m_internal {internal}
{
    invalidate();
}

auto Cursor::id() const -> Index
{
    CALICO_EXPECT_TRUE(m_is_valid);
    return m_position.ids[Position::CURRENT];
}

auto Cursor::index() const -> Index
{
    CALICO_EXPECT_TRUE(m_is_valid);
    return m_position.index;
}

auto Cursor::operator==(const Cursor &rhs) const -> bool
{
    if (m_position == rhs.m_position) {
        // These cursors should come from the same database.
        CALICO_EXPECT_EQ(m_pool, rhs.m_pool);
        CALICO_EXPECT_EQ(m_internal, rhs.m_internal);
        return m_is_valid == rhs.m_is_valid;
    }
    return !(m_is_valid && rhs.m_is_valid);
}

auto Cursor::operator!=(const Cursor &rhs) const -> bool
{
    return !(*this == rhs);
}


// Define prefix increment operator.
auto Cursor::operator++() -> Cursor&
{
    increment();
    return *this;
}

// Define postfix increment operator.
auto Cursor::operator++(int) -> Cursor
{
    auto temp = *this;
    ++*this;
    return temp;
}

// Define prefix decrement operator.
auto Cursor::operator--() -> Cursor&
{
    decrement();
    return *this;
}

// Define postfix decrement operator.
auto Cursor::operator--(int) -> Cursor
{
    auto temp = *this;
    --*this;
    return temp;
}

auto Cursor::is_valid() const -> bool
{
    return m_is_valid;
}

auto Cursor::is_maximum() const -> bool
{
    return m_is_valid && m_position.is_maximum();
}

auto Cursor::is_minimum() const -> bool
{
    return m_is_valid && m_position.is_minimum();
}

auto Cursor::move_to(Node node, Index index) -> void
{
    CALICO_EXPECT_TRUE(node.is_external());
    const auto count = node.cell_count();
    m_is_valid = count && index < count;

    if (m_is_valid) {
        m_position.index = index;
        m_position.cell_count = count;
        m_position.ids[Position::LEFT] = node.left_sibling_id().value;
        m_position.ids[Position::CURRENT] = node.id().value;
        m_position.ids[Position::RIGHT] = node.right_sibling_id().value;
    }
}

auto Cursor::invalidate() -> void
{
    m_is_valid = false;
}

auto Cursor::increment() -> bool
{
    if (m_is_valid) {
        if (m_position.index == m_position.cell_count - 1) {
            seek_right();
        } else {
            m_position.index++;
        }
        return true;
    }
    return false;
}

auto Cursor::decrement() -> bool
{
    if (m_is_valid) {
        if (m_position.index == 0) {
            seek_left();
        } else {
            m_position.index--;
        }
        return true;
    }
    return false;
}

auto Cursor::key() const -> BytesView
{
    CALICO_EXPECT_TRUE(m_is_valid);
    const auto node = m_pool->acquire(PID {m_position.ids[Position::CURRENT]}, false);
    return node.read_key(m_position.index);
}

auto Cursor::value() const -> std::string
{
    CALICO_EXPECT_TRUE(m_is_valid);
    const auto node = m_pool->acquire(PID {m_position.ids[Position::CURRENT]}, false);
    return m_internal->collect_value(node, m_position.index);
}

auto Cursor::record() const -> Record
{
    CALICO_EXPECT_TRUE(m_is_valid);
    const auto node = m_pool->acquire(PID {m_position.ids[Position::CURRENT]}, false);
    return {btos(node.read_key(m_position.index)), m_internal->collect_value(node, m_position.index)};
}

auto Cursor::seek_left() -> void
{
    CALICO_EXPECT_TRUE(m_is_valid);
    CALICO_EXPECT_EQ(m_position.index, 0);
    if (is_minimum()) {
        invalidate();
    } else {
        const PID left {m_position.ids[Position::LEFT]};
        move_to(m_pool->acquire(left, false), 0);
        m_position.index = m_position.cell_count - 1;
    }
}

auto Cursor::seek_right() -> void
{
    CALICO_EXPECT_TRUE(m_is_valid);
    CALICO_EXPECT_EQ(m_position.index, m_position.cell_count - 1);
    if (is_maximum()) {
        invalidate();
    } else {
        const PID right {m_position.ids[Position::RIGHT]};
        move_to(m_pool->acquire(right, false), 0);
    }
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
    return PID {ids[RIGHT]}.is_null() && index + 1 == cell_count;
}

auto Cursor::Position::is_minimum() const -> bool
{
    CALICO_EXPECT_NE(ids[CURRENT], 0);
    return cell_count && PID {ids[LEFT]}.is_null() && index == 0;
}

} // calico

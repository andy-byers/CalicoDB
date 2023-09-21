// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "cursor_impl.h"
#include "internal.h"
#include "pager.h"
#include "schema.h"

namespace calicodb
{

CursorImpl::CursorImpl(Tree &tree)
    : m_c(tree)
{
}

CursorImpl::~CursorImpl() = default;

auto CursorImpl::is_valid() const -> bool
{
    return m_c.is_valid();
}

auto CursorImpl::handle() -> void *
{
    return &m_c;
}

auto CursorImpl::key() const -> Slice
{
    return m_c.key();
}

auto CursorImpl::value() const -> Slice
{
    return m_c.value();
}

auto CursorImpl::status() const -> Status
{
    return m_c.status();
}

auto CursorImpl::seek_first() -> void
{
    seek("");
}

auto CursorImpl::seek_last() -> void
{
    m_c.activate(false);
    m_c.seek_to_last_leaf();
    m_c.fetch_record_if_valid();
}

auto CursorImpl::seek(const Slice &key) -> void
{
    m_c.activate(false);
    m_c.seek_to_leaf(key, true);
}

auto CursorImpl::find(const Slice &key) -> void
{
    m_c.activate(false);
    if (!m_c.seek_to_leaf(key, true)) {
        m_c.reset(m_c.status());
    }
}

auto CursorImpl::next() -> void
{
    CALICODB_EXPECT_TRUE(m_c.is_valid());
    m_c.activate(true);
    if (m_c.is_valid()) {
        m_c.move_right();
    }
    m_c.fetch_record_if_valid();
}

auto CursorImpl::previous() -> void
{
    CALICODB_EXPECT_TRUE(m_c.is_valid());
    m_c.activate(true);
    if (m_c.is_valid()) {
        m_c.move_left();
    }
    m_c.fetch_record_if_valid();
}

auto CursorImpl::TEST_tree_cursor() -> TreeCursor &
{
    return m_c;
}

auto CursorImpl::TEST_check_state() const -> void
{
    CALICODB_EXPECT_TRUE(m_c.assert_state());
}

} // namespace calicodb

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

auto CursorImpl::handle() -> void *
{
    return &m_c;
}

auto CursorImpl::is_valid() const -> bool
{
    return m_c.is_valid();
}

auto CursorImpl::is_bucket() const -> bool
{
    return m_c.is_bucket();
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

void CursorImpl::seek_first()
{
    seek("");
}

void CursorImpl::seek_last()
{
    m_c.activate(false);
    m_c.seek_to_last_leaf();
    m_c.read_record();
}

void CursorImpl::seek(const Slice &key)
{
    m_c.activate(false);
    m_c.seek_to_leaf(key);
    m_c.ensure_correct_leaf();
    m_c.read_record();
}

void CursorImpl::find(const Slice &key)
{
    m_c.activate(false);
    if (m_c.seek_to_leaf(key)) {
        m_c.read_record();
    } else {
        m_c.reset(m_c.status());
    }
}

void CursorImpl::next()
{
    CALICODB_EXPECT_TRUE(m_c.is_valid());
    // If the cursor was saved, and gets loaded back to a different position, then the
    // record it was on must have been erased. If it is still on a valid record, then
    // that record must have a key that compares greater than the key the cursor was
    // saved on.
    const auto moved = m_c.activate(true);
    if (m_c.is_valid()) {
        if (!moved) {
            m_c.move_right();
        }
        m_c.read_record();
    }
}

void CursorImpl::previous()
{
    CALICODB_EXPECT_TRUE(m_c.is_valid());
    m_c.activate(true);
    if (m_c.is_valid()) {
        m_c.move_left();
        m_c.read_record();
    }
}

void CursorImpl::TEST_check_state() const
{
    CALICODB_EXPECT_TRUE(m_c.assert_state());
}

} // namespace calicodb

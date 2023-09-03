// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/string.h"
#include "alloc.h"

namespace calicodb
{

String::String(String &&rhs) noexcept
    : m_ptr(exchange(rhs.m_ptr, nullptr)),
      m_len(exchange(rhs.m_len, 0U)),
      m_cap(exchange(rhs.m_cap, 0U))
{
}

auto String::operator=(String &&rhs) noexcept -> String &
{
    if (this != &rhs) {
        clear();
        m_ptr = exchange(rhs.m_ptr, nullptr);
        m_len = exchange(rhs.m_len, 0U);
        m_cap = exchange(rhs.m_cap, 0U);
    }
    return *this;
}

auto String::clear() -> void
{
    Alloc::deallocate(m_ptr);
    m_ptr = nullptr;
    m_len = 0;
    m_cap = 0;
}

} // namespace calicodb
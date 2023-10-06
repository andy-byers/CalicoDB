// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "internal_string.h"
#include "mem.h"
#include "utility.h"

namespace calicodb
{

String::String(String &&rhs) noexcept
    : m_data(exchange(rhs.m_data, nullptr)),
      m_size(exchange(rhs.m_size, 0U))
{
}

auto String::operator=(String &&rhs) noexcept -> String &
{
    if (this != &rhs) {
        clear();
        m_data = exchange(rhs.m_data, nullptr);
        m_size = exchange(rhs.m_size, 0U);
    }
    return *this;
}

auto String::clear() -> void
{
    Mem::deallocate(m_data);
    m_data = nullptr;
    m_size = 0;
}

auto String::resize(size_t size, char c) -> int
{
    if (auto *ptr = static_cast<char *>(Mem::reallocate(m_data, size + 1))) {
        if (size > m_size) {
            std::memset(ptr + m_size, c, size - m_size);
        }
        ptr[size] = '\0';
        m_size = size;
        m_data = ptr;
        return 0;
    }
    return -1;
}

} // namespace calicodb
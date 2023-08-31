// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_STRING_H
#define CALICODB_STRING_H

#include "slice.h"

namespace calicodb
{

// Wrapper for a heap-allocated C-style string
// Instances of this class are filled-out by certain library routines.
class String final
{
public:
    explicit String()
        : m_ptr(nullptr),
          m_len(0),
          m_cap(0)
    {
    }

    ~String()
    {
        clear();
    }

    String(const String &) = delete;
    auto operator=(const String &) -> String & = delete;

    String(String &&rhs) noexcept
        : m_ptr(rhs.m_ptr),
          m_len(rhs.m_len),
          m_cap(rhs.m_cap)
    {
        rhs.m_ptr = nullptr;
        rhs.m_len = 0;
        rhs.m_cap = 0;
    }

    auto operator=(String &&rhs) noexcept -> String &
    {
        if (this != &rhs) {
            clear();
            m_ptr = rhs.m_ptr;
            m_len = rhs.m_len;
            m_cap = rhs.m_cap;

            rhs.m_ptr = nullptr;
            rhs.m_len = 0;
            rhs.m_cap = 0;
        }
        return *this;
    }

    explicit operator Slice()
    {
        return m_ptr ? Slice(m_ptr, m_len) : "";
    }

    [[nodiscard]] auto is_empty() const -> bool
    {
        return length() == 0;
    }

    [[nodiscard]] auto length() const -> size_t
    {
        return m_len;
    }

    [[nodiscard]] auto c_str() const -> const char *
    {
        return m_len ? m_ptr : "";
    }

    [[nodiscard]] auto data() -> char *
    {
        return m_ptr;
    }

    auto clear() -> void;

private:
    friend class StringBuilder;

    explicit String(char *ptr, size_t len, size_t cap)
        : m_ptr(ptr),
          m_len(len),
          m_cap(cap)
    {
    }

    char *m_ptr;
    size_t m_len;
    size_t m_cap;
};

} // namespace calicodb

#endif // CALICODB_STRING_H

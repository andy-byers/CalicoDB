// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_INTERNAL_STRING_H
#define CALICODB_INTERNAL_STRING_H

#include "calicodb/slice.h"

namespace calicodb
{

// Wrapper for a heap-allocated C-style string
// Instances of this class are filled-out by certain library routines.
class String final
{
public:
    explicit String()
        : m_data(nullptr),
          m_size(0)
    {
    }

    ~String()
    {
        clear();
    }

    String(const String &) = delete;
    auto operator=(const String &) -> String & = delete;
    String(String &&rhs) noexcept;
    auto operator=(String &&rhs) noexcept -> String &;

    explicit operator Slice()
    {
        return m_size ? Slice(m_data, m_size) : "";
    }

    [[nodiscard]] auto is_empty() const -> bool
    {
        return size() == 0;
    }

    [[nodiscard]] auto size() const -> size_t
    {
        return m_size;
    }

    [[nodiscard]] auto c_str() const -> const char *
    {
        return m_size ? m_data : "";
    }

    [[nodiscard]] auto data() -> char *
    {
        return m_data;
    }

    auto clear() -> void;

private:
    friend class StringBuilder;

    explicit String(char *data, size_t size)
        : m_data(data),
          m_size(size)
    {
    }

    char *m_data;
    size_t m_size;
};

} // namespace calicodb

#endif // CALICODB_INTERNAL_STRING_H

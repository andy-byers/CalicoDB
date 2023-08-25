// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_STRING_H
#define CALICODB_STRING_H

#include <cassert>
#include <cstring>
#include <utility>

namespace calicodb
{

// Wrapper for a heap-allocated C-style string
class String final
{
public:
    explicit String(char *ptr = nullptr)
        : m_ptr(ptr)
    {
    }

    ~String()
    {
        reset();
    }

    String(const String &) = delete;
    auto operator=(const String &) -> String & = delete;

    String(String &&rhs) noexcept
        : m_ptr(std::exchange(rhs.m_ptr, nullptr))
    {
    }

    auto operator=(String &&rhs) noexcept -> String &
    {
        if (this != &rhs) {
            reset(std::exchange(rhs.m_ptr, nullptr));
        }
        return *this;
    }

    auto operator==(const String &rhs) const -> bool
    {
        return std::strcmp(c_str(), rhs.c_str()) == 0;
    }

    auto operator!=(const String &rhs) const -> bool
    {
        return !(*this == rhs);
    }

    [[nodiscard]] auto is_empty() const -> bool
    {
        return std::strlen(c_str()) == 0;
    }

    [[nodiscard]] auto c_str() const -> const char *
    {
        return m_ptr ? m_ptr : "";
    }

    auto data() -> char *
    {
        return m_ptr;
    }

    auto reset(char *ptr = nullptr) -> void;

private:
    friend class StringHelper;

    char *m_ptr;
};

} // namespace calicodb

#endif // CALICODB_STRING_H

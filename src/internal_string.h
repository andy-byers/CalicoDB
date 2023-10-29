// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_INTERNAL_STRING_H
#define CALICODB_INTERNAL_STRING_H

#include "internal_vector.h"

namespace calicodb
{

// Wrapper for a heap-allocated C-style string
class String final
{
public:
    using RawParts = Vector<char>::RawParts;

    static auto from_raw_parts(const RawParts &parts) -> String
    {
        String s;
        const auto [data, size_wo_null, capacity] = parts;
        s.m_vec = Vector<char>::from_raw_parts({data, size_wo_null + (data != nullptr), capacity});
        return s;
    }

    static auto into_raw_parts(String s) -> RawParts
    {
        // If the string consists of a single null byte, then just return nullptr for the
        // data part and free the allocation to avoid ambiguity.
        if (s.m_vec.size() <= 1) {
            s.m_vec.clear();
        }
        const auto [data, size_w_null, capacity] = Vector<char>::into_raw_parts(move(s.m_vec));
        return {data, size_w_null - (data != nullptr), capacity};
    }

    explicit String() = default;
    ~String() = default;

    String(String &&rhs) noexcept = default;
    auto operator=(String &&rhs) noexcept -> String & = default;

    explicit operator Slice() const
    {
        return {c_str(), size()};
    }

    [[nodiscard]] auto is_empty() const -> bool
    {
        return m_vec.size() <= 1;
    }

    [[nodiscard]] auto size() const -> size_t
    {
        return m_vec.is_empty() ? 0 : m_vec.size() - 1;
    }

    [[nodiscard]] auto data() -> char *
    {
        return m_vec.data();
    }

    [[nodiscard]] auto data() const -> const char *
    {
        return m_vec.data();
    }

    [[nodiscard]] auto c_str() const -> const char *
    {
        return m_vec.is_empty() ? "" : m_vec.data();
    }

    auto operator[](size_t idx) -> char &
    {
        CALICODB_EXPECT_FALSE(m_vec.is_empty());
        // Make sure the null byte isn't accessed.
        CALICODB_EXPECT_LT(idx, m_vec.size() - 1);
        return m_vec[idx];
    }

    auto operator[](size_t idx) const -> const char &
    {
        CALICODB_EXPECT_FALSE(m_vec.is_empty());
        CALICODB_EXPECT_LT(idx, m_vec.size() - 1);
        return m_vec[idx];
    }

    auto clear() -> void
    {
        m_vec.clear();
    }

    [[nodiscard]] auto resize(size_t target_size) -> int
    {
        return m_vec.resize(target_size + 1);
    }

    [[nodiscard]] auto resize(size_t target_size, char c) -> int
    {
        const auto occupied = size();
        if (resize(target_size)) {
            return -1;
        }
        if (target_size > occupied) {
            std::memset(m_vec.begin() + occupied, c,
                        target_size - occupied);
        }
        m_vec[target_size] = '\0';
        return 0;
    }

private:
    Vector<char> m_vec;
};

} // namespace calicodb

#endif // CALICODB_INTERNAL_STRING_H

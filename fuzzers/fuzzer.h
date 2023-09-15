// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.
//
// Some code was modified from https://github.com/CodeIntelligenceTesting/cifuzz.

#ifndef CALICODB_FUZZERS_FUZZER_H
#define CALICODB_FUZZERS_FUZZER_H

#include "common.h"
#include "internal.h"
#include <climits>
#include <iostream>
#include <limits>
#include <vector>

namespace calicodb
{

#define CHECK_TRUE(cond)                                 \
    do {                                                 \
        if (!(cond)) {                                   \
            std::cerr << "expected `" << #cond << "`\n"; \
            std::abort();                                \
        }                                                \
    } while (0)

#define CHECK_FALSE(cond) \
    CHECK_TRUE(!(cond))

#define CHECK_OK(expr)                                             \
    do {                                                           \
        if (auto assert_s = (expr); !assert_s.is_ok()) {           \
            std::fprintf(                                          \
                stderr,                                            \
                "expected `(" #expr ").is_ok()` but got \"%s\"\n", \
                assert_s.message());                               \
            std::abort();                                          \
        }                                                          \
    } while (0)

#define CHECK_EQ(lhs, rhs)                                                                             \
    do {                                                                                               \
        if ((lhs) != (rhs)) {                                                                          \
            std::cerr << "expected `" << #lhs "` (" << (lhs) << ") == `" #rhs "` (" << (rhs) << ")\n"; \
            std::abort();                                                                              \
        }                                                                                              \
    } while (0)

class FuzzedInputProvider
{
    const uint8_t *m_ptr;
    size_t m_len;

public:
    explicit FuzzedInputProvider(const uint8_t *ptr, size_t len)
        : m_ptr(ptr),
          m_len(len)
    {
    }

    [[nodiscard]] auto is_empty() const -> bool
    {
        return m_len == 0;
    }

    [[nodiscard]] auto length() const -> size_t
    {
        return m_len;
    }

    [[nodiscard]] auto extract_random(size_t max_len) -> std::string
    {
        std::string result;
        result.reserve(std::min(max_len, m_len));
        for (size_t i = 0; i < max_len && m_len != 0; ++i) {
            char next = static_cast<char>(m_ptr[0]);
            advance(1);
            if (next == '\\' && m_len != 0) {
                next = static_cast<char>(m_ptr[0]);
                advance(1);
                if (next != '\\') {
                    break;
                }
            }
            result += next;
        }
        result.shrink_to_fit();
        return result;
    }

    [[nodiscard]] auto extract_random() -> std::string
    {
        return extract_random(m_len);
    }

    auto advance(size_t len) -> void
    {
        CHECK_TRUE(len <= m_len);
        m_len -= len;
        m_ptr += len;
    }

    template <class T>
    auto extract_integral() -> T
    {
        return extract_integral_in_range(std::numeric_limits<T>::min(),
                                         std::numeric_limits<T>::max());
    }

    // Produces a value in range [min, max]
    template <class T>
    auto extract_integral_in_range(T min, T max) -> T
    {
        static_assert(std::is_integral<T>::value, "An integral type is required.");
        static_assert(sizeof(T) <= sizeof(uint64_t), "Unsupported integral type.");
        CALICODB_EXPECT_TRUE(min <= max);

        auto range = static_cast<uint64_t>(max) - min;
        uint64_t result = 0;
        size_t offset = 0;
        while (offset < sizeof(T) * CHAR_BIT && (range >> offset) > 0 && m_len != 0) {
            --m_len;
            result = (result << CHAR_BIT) | m_ptr[m_len];
            offset += CHAR_BIT;
        }
        if (range != std::numeric_limits<decltype(range)>::max()) {
            result = result % (range + 1);
        }
        return static_cast<T>(min + result);
    }

    [[nodiscard]] auto extract_fixed(size_t len) -> Slice
    {
        const auto fixed = peek(len);
        advance(fixed.size());
        return fixed;
    }

    // NOTE: T::kMaxValue must be aliased to the largest enumerator value
    template <typename T>
    auto extract_enum() -> T
    {
        static_assert(std::is_enum<T>::value, "|T| must be an enum type.");
        return static_cast<T>(
            extract_integral_in_range<uint32_t>(0, static_cast<uint32_t>(T::kMaxValue)));
    }

    [[nodiscard]] auto peek(size_t len) -> Slice
    {
        CHECK_TRUE(len <= m_len);
        return {reinterpret_cast<const char *>(m_ptr), len};
    }
};

} // namespace calicodb

#endif // CALICODB_FUZZERS_FUZZER_H

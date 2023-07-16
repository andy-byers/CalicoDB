// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_FUZZERS_FUZZER_H
#define CALICODB_FUZZERS_FUZZER_H

#include "utils.h"
#include <iostream>

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
                assert_s.to_string().c_str());                     \
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

class FuzzerStream
{
    static constexpr char kFakeData[kPageSize] = {};

    const U8 **m_ptr;
    std::size_t *m_len;

public:
    explicit FuzzerStream(const U8 *&ptr, std::size_t &len)
        : m_ptr(&ptr),
          m_len(&len)
    {
    }

    [[nodiscard]] auto is_empty() const -> bool
    {
        return *m_len == 0;
    }

    [[nodiscard]] auto length() const -> std::size_t
    {
        return *m_len;
    }

    [[nodiscard]] auto extract_random() -> Slice
    {
        std::size_t next_len = 0;
        for (U32 i = 0; i < std::min(2UL, *m_len); ++i) {
            // Determine a length for the binary string.
            next_len = ((next_len << 8) | (*m_ptr)[i]) & 0xFFFF;
        }
        return extract_fixed(std::min(*m_len, next_len));
    }

    [[nodiscard]] auto extract_fake_random() -> Slice
    {
        if (*m_len == 0) {
            return "";
        }
        const auto len = static_cast<std::size_t>(**m_ptr) * 16;
        (void)extract_fixed(1);
        return Slice(kFakeData, kPageSize).truncate(len);
    }

    [[nodiscard]] auto extract_fixed(std::size_t len) -> Slice
    {
        const auto fixed = peek(len);
        *m_ptr += fixed.size();
        *m_len -= fixed.size();
        return fixed;
    }

    [[nodiscard]] auto peek(std::size_t len) -> Slice
    {
        CALICODB_EXPECT_LE(len, *m_len);
        return {reinterpret_cast<const char *>(*m_ptr), len};
    }
};

} // namespace calicodb

#endif // CALICODB_FUZZERS_FUZZER_H

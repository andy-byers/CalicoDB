// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "common.h"
#include "test.h"

#include "encoding.h"
#include "logging.h"
#include "scope_guard.h"

namespace calicodb::test
{

TEST(Encoding, Fixed32)
{
    std::string s;
    for (U32 v = 0; v < 100000; v++) {
        s.resize(s.size() + sizeof(U32));
        put_u32(s.data() + s.size() - sizeof(U32), v);
    }

    const char *p = s.data();
    for (U32 v = 0; v < 100000; v++) {
        U32 actual = get_u32(p);
        ASSERT_EQ(v, actual);
        p += sizeof(U32);
    }
}

TEST(Coding, Fixed64)
{
    std::string s;
    for (int power = 0; power <= 63; power++) {
        U64 v = static_cast<U64>(1) << power;
        s.resize(s.size() + sizeof(U64) * 3);
        put_u64(s.data() + s.size() - sizeof(U64) * 3, v - 1);
        put_u64(s.data() + s.size() - sizeof(U64) * 2, v + 0);
        put_u64(s.data() + s.size() - sizeof(U64) * 1, v + 1);
    }

    const char *p = s.data();
    for (int power = 0; power <= 63; power++) {
        U64 v = static_cast<U64>(1) << power;
        U64 actual;
        actual = get_u64(p);
        ASSERT_EQ(v - 1, actual);
        p += sizeof(U64);

        actual = get_u64(p);
        ASSERT_EQ(v + 0, actual);
        p += sizeof(U64);

        actual = get_u64(p);
        ASSERT_EQ(v + 1, actual);
        p += sizeof(U64);
    }
}

// Test that encoding routines generate little-endian encodings
TEST(Encoding, EncodingOutput)
{
    std::string dst(4, '\0');
    put_u32(dst.data(), 0x04030201);
    ASSERT_EQ(0x01, static_cast<int>(dst[0]));
    ASSERT_EQ(0x02, static_cast<int>(dst[1]));
    ASSERT_EQ(0x03, static_cast<int>(dst[2]));
    ASSERT_EQ(0x04, static_cast<int>(dst[3]));

    dst.resize(8);
    put_u64(dst.data(), 0x0807060504030201ull);
    ASSERT_EQ(0x01, static_cast<int>(dst[0]));
    ASSERT_EQ(0x02, static_cast<int>(dst[1]));
    ASSERT_EQ(0x03, static_cast<int>(dst[2]));
    ASSERT_EQ(0x04, static_cast<int>(dst[3]));
    ASSERT_EQ(0x05, static_cast<int>(dst[4]));
    ASSERT_EQ(0x06, static_cast<int>(dst[5]));
    ASSERT_EQ(0x07, static_cast<int>(dst[6]));
    ASSERT_EQ(0x08, static_cast<int>(dst[7]));
}

TEST(Encoding, Varint64)
{
    // Construct the list of values to check
    std::vector<U64> values;
    // Some special values
    values.push_back(0);
    values.push_back(100);
    values.push_back(~static_cast<U64>(0));
    values.push_back(~static_cast<U64>(0) - 1);
    for (U32 k = 0; k < 64; k++) {
        // Test values near powers of two
        const U64 power = 1ull << k;
        values.push_back(power);
        values.push_back(power - 1);
        values.push_back(power + 1);
    }

    std::string s;
    for (auto v : values) {
        const auto v_len = varint_length(v);
        auto old_len = s.size();
        s.resize(old_len + v_len);
        encode_varint(s.data() + old_len, v);
    }

    const char *p = s.data();
    const char *limit = p + s.size();
    for (auto v : values) {
        ASSERT_TRUE(p < limit);
        U64 actual;
        const char *start = p;
        p = decode_varint(p, limit, actual);
        ASSERT_TRUE(p != nullptr);
        ASSERT_EQ(v, actual);
        ASSERT_EQ(varint_length(actual), p - start);
    }
    ASSERT_EQ(p, limit);
}

TEST(Encoding, Varint64Overflow)
{
    U64 result;
    std::string input("\x81\x82\x83\x84\x85\x81\x82\x83\x84\x85\x11");
    ASSERT_TRUE(decode_varint(input.data(), input.data() + input.size(), result) == nullptr);
}

TEST(Encoding, Varint64Truncation)
{
    U64 large_value = (1ull << 63) + 100ull;
    std::string s(varint_length(large_value), '\0');
    encode_varint(s.data(), large_value);
    U64 result;
    for (size_t len = 0; len < s.size() - 1; len++) {
        ASSERT_TRUE(decode_varint(s.data(), s.data() + len, result) == nullptr);
    }
    ASSERT_TRUE(decode_varint(s.data(), s.data() + s.size(), result) !=
                nullptr);
    ASSERT_EQ(large_value, result);
}

TEST(Status, StatusMessages)
{
    ASSERT_EQ("OK", Status::ok().to_string());
    ASSERT_EQ("I/O error", Status::io_error().to_string());
    ASSERT_EQ("I/O error: msg", Status::io_error("msg").to_string());
    ASSERT_EQ("corruption", Status::corruption().to_string());
    ASSERT_EQ("corruption: msg", Status::corruption("msg").to_string());
    ASSERT_EQ("invalid argument", Status::invalid_argument().to_string());
    ASSERT_EQ("invalid argument: msg", Status::invalid_argument("msg").to_string());
    ASSERT_EQ("not supported", Status::not_supported().to_string());
    ASSERT_EQ("not supported: msg", Status::not_supported("msg").to_string());
    ASSERT_EQ("busy", Status::busy().to_string());
    ASSERT_EQ("busy: msg", Status::busy("msg").to_string());
    ASSERT_EQ("busy: retry", Status::retry().to_string());
    // Choice of `Status::invalid_argument()` is arbitrary, any `Code-SubCode` combo
    // is technically legal, but may not be semantically valid (for example, it makes
    // no sense to retry when a read-only transaction attempts to write: repeating that
    // action will surely fail next time as well).
    ASSERT_EQ("invalid argument: retry", Status::invalid_argument(Status::kRetry).to_string());
}

TEST(Status, StatusCodes)
{
    ASSERT_TRUE(Status::invalid_argument().is_invalid_argument());
    ASSERT_EQ(Status::invalid_argument().code(), Status::kInvalidArgument);
    ASSERT_TRUE(Status::io_error().is_io_error());
    ASSERT_EQ(Status::io_error().code(), Status::kIOError);
    ASSERT_TRUE(Status::not_supported().is_not_supported());
    ASSERT_EQ(Status::not_supported().code(), Status::kNotSupported);
    ASSERT_TRUE(Status::corruption().is_corruption());
    ASSERT_EQ(Status::corruption().code(), Status::kCorruption);
    ASSERT_TRUE(Status::not_found().is_not_found());
    ASSERT_EQ(Status::not_found().code(), Status::kNotFound);
    ASSERT_TRUE(Status::busy().is_busy());
    ASSERT_EQ(Status::busy().code(), Status::kBusy);
    ASSERT_TRUE(Status::retry().is_retry());
    ASSERT_EQ(Status::retry().code(), Status::kBusy);
    ASSERT_EQ(Status::retry().subcode(), Status::kRetry);
    ASSERT_TRUE(Status::ok().is_ok());
    ASSERT_EQ(Status::ok().code(), Status::kOK);
}

TEST(Status, Copy)
{
    const auto s = Status::invalid_argument("status message");
    const auto t = s;
    ASSERT_TRUE(t.is_invalid_argument());
    ASSERT_EQ(t.to_string(), std::string("invalid argument: ") + "status message");

    ASSERT_TRUE(s.is_invalid_argument());
    ASSERT_EQ(s.to_string(), std::string("invalid argument: ") + "status message");
}

TEST(Status, Reassign)
{
    auto s = Status::ok();
    ASSERT_TRUE(s.is_ok());

    s = Status::invalid_argument("status message");
    ASSERT_TRUE(s.is_invalid_argument());
    ASSERT_EQ(s.to_string(), "invalid argument: status message");

    s = Status::not_supported("status message");
    ASSERT_TRUE(s.is_not_supported());
    ASSERT_EQ(s.to_string(), "not supported: status message");

    s = Status::ok();
    ASSERT_TRUE(s.is_ok());
}

TEST(Status, MoveConstructor)
{
    {
        Status ok = Status::ok();
        Status ok2 = std::move(ok);

        ASSERT_TRUE(ok2.is_ok());
    }

    {
        Status status = Status::not_found("custom kNotFound status message");
        Status status2 = std::move(status);

        ASSERT_TRUE(status2.is_not_found());
        ASSERT_EQ("not found: custom kNotFound status message", status2.to_string());
    }

    {
        Status self_moved = Status::io_error("custom kIOError status message");

        // Needed to bypass compiler warning about explicit move-assignment.
        Status &self_moved_reference = self_moved;
        self_moved_reference = std::move(self_moved);
    }
}

TEST(Logging, NumberToString)
{
    ASSERT_EQ("0", number_to_string(0));
    ASSERT_EQ("1", number_to_string(1));
    ASSERT_EQ("9", number_to_string(9));

    ASSERT_EQ("10", number_to_string(10));
    ASSERT_EQ("11", number_to_string(11));
    ASSERT_EQ("19", number_to_string(19));
    ASSERT_EQ("99", number_to_string(99));

    ASSERT_EQ("100", number_to_string(100));
    ASSERT_EQ("109", number_to_string(109));
    ASSERT_EQ("190", number_to_string(190));
    ASSERT_EQ("123", number_to_string(123));
    ASSERT_EQ("12345678", number_to_string(12345678));

    static_assert(std::numeric_limits<uint64_t>::max() == 18446744073709551615U,
                  "Test consistency check");
    ASSERT_EQ("18446744073709551000", number_to_string(18446744073709551000U));
    ASSERT_EQ("18446744073709551600", number_to_string(18446744073709551600U));
    ASSERT_EQ("18446744073709551610", number_to_string(18446744073709551610U));
    ASSERT_EQ("18446744073709551614", number_to_string(18446744073709551614U));
    ASSERT_EQ("18446744073709551615", number_to_string(18446744073709551615U));
}

void ConsumeDecimalNumberRoundtripTest(uint64_t number,
                                       const std::string &padding = "")
{
    std::string decimal_number = number_to_string(number);
    std::string input_string = decimal_number + padding;
    Slice input(input_string);
    Slice output = input;
    uint64_t result;
    ASSERT_TRUE(consume_decimal_number(output, &result));
    ASSERT_EQ(number, result);
    ASSERT_EQ(decimal_number.size(), output.data() - input.data());
    ASSERT_EQ(padding.size(), output.size());
}

TEST(Logging, ConsumeDecimalNumberRoundtrip)
{
    ConsumeDecimalNumberRoundtripTest(0);
    ConsumeDecimalNumberRoundtripTest(1);
    ConsumeDecimalNumberRoundtripTest(9);

    ConsumeDecimalNumberRoundtripTest(10);
    ConsumeDecimalNumberRoundtripTest(11);
    ConsumeDecimalNumberRoundtripTest(19);
    ConsumeDecimalNumberRoundtripTest(99);

    ConsumeDecimalNumberRoundtripTest(100);
    ConsumeDecimalNumberRoundtripTest(109);
    ConsumeDecimalNumberRoundtripTest(190);
    ConsumeDecimalNumberRoundtripTest(123);
    ASSERT_EQ("12345678", number_to_string(12345678));

    for (uint64_t i = 0; i < 100; ++i) {
        uint64_t large_number = std::numeric_limits<uint64_t>::max() - i;
        ConsumeDecimalNumberRoundtripTest(large_number);
    }
}

TEST(Logging, ConsumeDecimalNumberRoundtripWithPadding)
{
    ConsumeDecimalNumberRoundtripTest(0, " ");
    ConsumeDecimalNumberRoundtripTest(1, "abc");
    ConsumeDecimalNumberRoundtripTest(9, "x");

    ConsumeDecimalNumberRoundtripTest(10, "_");
    ConsumeDecimalNumberRoundtripTest(11, std::string("\0\0\0", 3));
    ConsumeDecimalNumberRoundtripTest(19, "abc");
    ConsumeDecimalNumberRoundtripTest(99, "padding");

    ConsumeDecimalNumberRoundtripTest(100, " ");

    for (uint64_t i = 0; i < 100; ++i) {
        uint64_t large_number = std::numeric_limits<uint64_t>::max() - i;
        ConsumeDecimalNumberRoundtripTest(large_number, "pad");
    }
}

void ConsumeDecimalNumberOverflowTest(const std::string &input_string)
{
    Slice input(input_string);
    Slice output = input;
    uint64_t result;
    ASSERT_EQ(false, consume_decimal_number(output, &result));
}

TEST(Logging, ConsumeDecimalNumberOverflow)
{
    static_assert(std::numeric_limits<uint64_t>::max() == 18446744073709551615U,
                  "Test consistency check");
    ConsumeDecimalNumberOverflowTest("18446744073709551616");
    ConsumeDecimalNumberOverflowTest("18446744073709551617");
    ConsumeDecimalNumberOverflowTest("18446744073709551618");
    ConsumeDecimalNumberOverflowTest("18446744073709551619");
    ConsumeDecimalNumberOverflowTest("18446744073709551620");
    ConsumeDecimalNumberOverflowTest("18446744073709551621");
    ConsumeDecimalNumberOverflowTest("18446744073709551622");
    ConsumeDecimalNumberOverflowTest("18446744073709551623");
    ConsumeDecimalNumberOverflowTest("18446744073709551624");
    ConsumeDecimalNumberOverflowTest("18446744073709551625");
    ConsumeDecimalNumberOverflowTest("18446744073709551626");

    ConsumeDecimalNumberOverflowTest("18446744073709551700");

    ConsumeDecimalNumberOverflowTest("99999999999999999999");
}

void ConsumeDecimalNumberNoDigitsTest(const std::string &input_string)
{
    Slice input(input_string);
    Slice output = input;
    uint64_t result;
    ASSERT_EQ(false, consume_decimal_number(output, &result));
    ASSERT_EQ(input.data(), output.data());
    ASSERT_EQ(input.size(), output.size());
}

TEST(Logging, ConsumeDecimalNumberNoDigits)
{
    ConsumeDecimalNumberNoDigitsTest("");
    ConsumeDecimalNumberNoDigitsTest(" ");
    ConsumeDecimalNumberNoDigitsTest("a");
    ConsumeDecimalNumberNoDigitsTest(" 123");
    ConsumeDecimalNumberNoDigitsTest("a123");
    ConsumeDecimalNumberNoDigitsTest(std::string("\000123", 4));
    ConsumeDecimalNumberNoDigitsTest(std::string("\177123", 4));
    ConsumeDecimalNumberNoDigitsTest(std::string("\377123", 4));
}

TEST(Slice, Construction)
{
    std::string s("123");
    ASSERT_EQ(s, Slice(s));

    const auto *p = "123";
    ASSERT_EQ(p, Slice(p));
    ASSERT_EQ(p, Slice(p, 3));
}

TEST(Slice, StartsWith)
{
    Slice slice("Hello, world!");
    ASSERT_TRUE(slice.starts_with(""));
    ASSERT_TRUE(slice.starts_with("Hello"));
    ASSERT_TRUE(slice.starts_with("Hello, world!"));
    ASSERT_FALSE(slice.starts_with(" Hello"));
    ASSERT_FALSE(slice.starts_with("ello"));
    ASSERT_FALSE(slice.starts_with("Hello, world! "));
}

TEST(Slice, Comparisons)
{
    Slice slice("Hello, world!");
    const auto shorter = slice.range(0, slice.size() - 1);
    ASSERT_LT(shorter, slice);

    ASSERT_TRUE(Slice("10") > Slice("01"));
    ASSERT_TRUE(Slice("01") < Slice("10"));
    ASSERT_TRUE(Slice("10") >= Slice("01"));
    ASSERT_TRUE(Slice("01") <= Slice("10"));
}

TEST(Slice, Ranges)
{
    Slice slice("Hello, world!");
    ASSERT_TRUE(slice.range(0, 0).is_empty());
    ASSERT_EQ(slice.range(7, 5), Slice("world"));
    ASSERT_EQ(slice, slice.range(0));
    ASSERT_EQ(slice, slice.range(0, slice.size()));
}

TEST(Slice, Advance)
{
    Slice slice("Hello, world!");
    auto copy = slice;
    slice.advance(0);
    ASSERT_EQ(slice, copy);

    slice.advance(5);
    ASSERT_EQ(slice, ", world!");

    slice.advance(slice.size());
    ASSERT_TRUE(slice.is_empty());
}

TEST(Slice, Truncate)
{
    Slice slice("Hello, world!");
    auto copy = slice;
    slice.truncate(slice.size());
    ASSERT_TRUE(slice == copy);

    slice.truncate(5);
    ASSERT_EQ(slice, "Hello");

    slice.truncate(0);
    ASSERT_TRUE(slice.is_empty());
}

TEST(Slice, Clear)
{
    Slice slice("42");
    slice.clear();
    ASSERT_TRUE(slice.is_empty());
    ASSERT_EQ(0, slice.size());
}

static constexpr auto constexpr_slice_test(Slice s, Slice answer) -> int
{
    if (s != answer.range(0, s.size())) {
        return -1;
    }
    for (std::size_t i = 0; i < s.size(); ++i) {
        if (s[i] != answer[i]) {
            return -1;
        }
    }

    (void)s.starts_with(answer);
    (void)s.data();
    (void)s.range(0, 0);
    (void)s.is_empty();
    s.advance(0);
    s.truncate(s.size());
    return 0;
}

TEST(Slice, ConstantExpressions)
{
    static constexpr std::string_view sv("42");
    static constexpr Slice s1("42");
    static constexpr Slice s2(sv);
    ASSERT_EQ(0, constexpr_slice_test(s1, sv));
    ASSERT_EQ(0, constexpr_slice_test(s1, s2));
}

TEST(Slice, NonPrintableSlice)
{
    {
        const std::string s("\x00\x01", 2);
        ASSERT_EQ(2, Slice(s).size());
    }
    {
        const std::string s("\x00", 1);
        ASSERT_EQ(0, Slice(s).compare(s));
    }
    {
        std::string s("\x00\x00", 2);
        std::string t("\x00\x01", 2);
        ASSERT_LT(Slice(s).compare(t), 0);
    }
    {
        std::string u("\x0F", 1);
        std::string v("\x00", 1);
        v[0] = static_cast<char>(0xF0);

        // Signed comparison. 0xF0 overflows a signed byte and becomes negative.
        ASSERT_LT(v[0], u[0]);

        // Unsigned comparison should come out the other way.
        ASSERT_LT(Slice(u).compare(v), 0);
    }
}

class ScopeGuardTests : public testing::Test
{
protected:
    ScopeGuardTests()
    {
        m_callback = [this] {
            ++m_calls;
        };
    }

    ~ScopeGuardTests() override = default;

    std::function<void()> m_callback;
    int m_calls = 0;
};

TEST_F(ScopeGuardTests, CallbackIsCalledOnceOnScopeExit)
{
    {
        ASSERT_EQ(m_calls, 0);
        ScopeGuard guard(m_callback);
    }
    ASSERT_EQ(m_calls, 1);
}

TEST_F(ScopeGuardTests, CallbackIsNotCalledIfCancelled)
{
    {
        ASSERT_EQ(m_calls, 0);
        ScopeGuard guard(m_callback);
        std::move(guard).cancel();
    }
    ASSERT_EQ(m_calls, 0);
}

TEST_F(ScopeGuardTests, CallbackIsNotCalledAgainIfInvoked)
{
    {
        ASSERT_EQ(m_calls, 0);
        ScopeGuard guard(m_callback);
        std::move(guard).invoke();
    }
    ASSERT_EQ(m_calls, 1);
}

#if not NDEBUG
TEST(Expect, DeathTest)
{
    ASSERT_DEATH(CALICODB_EXPECT_TRUE(false), "expect");
}

TEST(Slice, DeathTest)
{
    Slice slice("Hello, world!");
    const auto oob = slice.size() + 1;

    ASSERT_DEATH(slice.advance(oob), "Assertion failed");
    ASSERT_DEATH(slice.truncate(oob), "Assertion failed");
    ASSERT_DEATH(auto r = slice.range(oob, 1), "Assertion failed");
    ASSERT_DEATH(auto r = slice.range(0, oob), "Assertion failed");
    ASSERT_DEATH(auto r = slice.range(oob / 2, oob - 1), "Assertion failed");
    ASSERT_DEATH(auto r = slice.range(oob), "Assertion failed");
    ASSERT_DEATH(auto r = slice[oob], "Assertion failed");
    ASSERT_DEATH(Slice bad_ptr(nullptr), "Assertion failed");
    ASSERT_DEATH(Slice bad_ptr(nullptr, 123), "Assertion failed");
}
#endif // not NDEBUG

} // namespace calicodb::test

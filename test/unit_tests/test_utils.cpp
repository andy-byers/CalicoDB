// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include <array>
#include <fstream>
#include <gtest/gtest.h>
#include <vector>

#include "calicodb/slice.h"
#include "encoding.h"
#include "logging.h"
#include "scope_guard.h"
#include "unit_tests.h"
#include "utils.h"

namespace calicodb
{

#if not NDEBUG
TEST(TestUtils, ExpectationDeathTest)
{
    ASSERT_DEATH(CALICODB_EXPECT_TRUE(false), kExpectationMatcher);
}
#endif // not NDEBUG

TEST(TestUtils, EncodingIsConsistent)
{
    tools::RandomGenerator random;
    const auto u16 = U16(-1);
    const auto u32 = U32(-2);
    const auto u64 = U64(-3);
    std::string buffer(sizeof(u16) + sizeof(u32) + sizeof(u64) + 1, '\x00');

    auto dst = buffer.data();
    put_u16(dst, u16);
    put_u32(dst += sizeof(U16), u32);
    put_u64(dst + sizeof(U32), u64);

    auto src = buffer.data();
    ASSERT_EQ(u16, get_u16(src));
    ASSERT_EQ(u32, get_u32(src += sizeof(U16)));
    ASSERT_EQ(u64, get_u64(src += sizeof(U32)));
    ASSERT_EQ(buffer.back(), 0) << "buffer overflow";
}

class SliceTests : public testing::Test
{
protected:
    std::string test_string{"Hello, world!"};
    Slice slice{test_string};
};

TEST_F(SliceTests, EqualsSelf)
{
    ASSERT_TRUE(slice == slice);
}

TEST_F(SliceTests, StringLiteralSlice)
{
    ASSERT_TRUE(Slice(test_string) == Slice{"Hello, world!"});
}

TEST_F(SliceTests, StartsWith)
{
    ASSERT_TRUE(slice.starts_with(""));
    ASSERT_TRUE(slice.starts_with("Hello"));
    ASSERT_TRUE(slice.starts_with(test_string));
    ASSERT_FALSE(slice.starts_with(" Hello"));
    ASSERT_FALSE(slice.starts_with("hello"));
    ASSERT_FALSE(slice.starts_with(test_string + ' '));
}

TEST_F(SliceTests, ShorterSlicesAreLessThanIfOtherwiseEqual)
{
    const auto shorter = slice.range(0, slice.size() - 1);
    ASSERT_TRUE(shorter < slice);
}

TEST_F(SliceTests, FirstcharIsMostSignificant)
{
    ASSERT_TRUE(Slice("10") > Slice("01"));
    ASSERT_TRUE(Slice("01") < Slice("10"));
    ASSERT_TRUE(Slice("10") >= Slice("01"));
    ASSERT_TRUE(Slice("01") <= Slice("10"));
}

TEST_F(SliceTests, CanGetPartialRange)
{
    ASSERT_TRUE(slice.range(7, 5) == Slice("world"));
}

TEST_F(SliceTests, CanGetEntireRange)
{
    ASSERT_TRUE(slice == slice.range(0));
    ASSERT_TRUE(slice == slice.range(0, slice.size()));
}

TEST_F(SliceTests, EmptyRangesAreEmpty)
{
    ASSERT_TRUE(slice.range(0, 0).is_empty());
}

TEST_F(SliceTests, AdvanceByZeroDoesNothing)
{
    auto copy = slice;
    slice.advance(0);
    ASSERT_TRUE(slice == copy);
}

TEST_F(SliceTests, AdvancingByOwnLengthProducesEmptySlice)
{
    slice.advance(slice.size());
    ASSERT_TRUE(slice.is_empty());
}

TEST_F(SliceTests, TruncatingToOwnLengthDoesNothing)
{
    auto copy = slice;
    slice.truncate(slice.size());
    ASSERT_TRUE(slice == copy);
}

TEST_F(SliceTests, TruncatingToZeroLengthProducesEmptySlice)
{
    slice.truncate(0);
    ASSERT_TRUE(slice.is_empty());
}

TEST_F(SliceTests, TruncatingEmptySliceDoesNothing)
{
    slice.truncate(0);
    auto copy = slice;
    slice.truncate(0);
    ASSERT_TRUE(slice == copy);
}

#if not NDEBUG
TEST_F(SliceTests, AdvanceDeathTest)
{
    ASSERT_DEATH(slice.advance(slice.size() + 1), "Assert");
}

TEST_F(SliceTests, RangeDeathTest)
{
    Slice discard;
    ASSERT_DEATH(discard = slice.range(slice.size() + 1), "Assert");
    ASSERT_DEATH(discard = slice.range(slice.size(), 1), "Assert");
    ASSERT_DEATH(discard = slice.range(0, slice.size() + 1), "Assert");
    ASSERT_DEATH(discard = slice.range(5, slice.size()), "Assert");
}

TEST_F(SliceTests, TruncateDeathTest)
{
    ASSERT_DEATH(slice.truncate(slice.size() + 1), "Assert");
    slice.truncate(0);
    ASSERT_DEATH(slice.truncate(1), "Assert");
}
#endif // not NDEBUG

TEST_F(SliceTests, WithCppString)
{
    // Construct from and compare with C++ strings.
    std::string s("123");
    Slice bv1(s);
    ASSERT_TRUE(bv1 == s); // Uses an implicit conversion.
}

TEST_F(SliceTests, WithCString)
{
    // Construct from and compare with C-style strings.
    char a[4]{"123"}; // Null-terminated
    Slice bv1(a);
    ASSERT_TRUE(bv1 == a);

    const auto *s = "123";
    Slice bv2(s);
    ASSERT_TRUE(bv2 == s);
}

static constexpr auto constexpr_test_read(Slice bv, Slice answer)
{
    for (std::size_t i = 0; i < bv.size(); ++i) {
        CALICODB_EXPECT_EQ(bv[i], answer[i]);
    }

    (void)bv.starts_with(answer);
    (void)bv.data();
    (void)bv.range(0, 0);
    (void)bv.is_empty();
    bv.advance(0);
    bv.truncate(bv.size());
}

TEST_F(SliceTests, ConstantExpressions)
{
    static constexpr Slice bv("42");
    constexpr_test_read(bv, "42");
}

TEST(NonPrintableSliceTests, UsesStringSize)
{
    const std::string u{"\x00\x01", 2};
    ASSERT_EQ(Slice(u).size(), 2);
}

TEST(NonPrintableSliceTests, NullcharsAreEqual)
{
    const std::string u{"\x00", 1};
    const std::string v{"\x00", 1};
    ASSERT_EQ(Slice(u).compare(v), 0);
}

TEST(NonPrintableSliceTests, ComparisonDoesNotStopAtNullchars)
{
    std::string u("\x00\x00", 2);
    std::string v("\x00\x01", 2);
    ASSERT_LT(Slice(u).compare(v), 0);
}

TEST(NonPrintableSliceTests, BytesAreUnsignedWhenCompared)
{
    std::string u("\x0F", 1);
    std::string v("\x00", 1);
    v[0] = static_cast<char>(0xF0);

    // Signed comparison. 0xF0 overflows a signed byte and becomes negative.
    ASSERT_LT(v[0], u[0]);

    // Unsigned comparison should come out the other way.
    ASSERT_LT(Slice(u).compare(v), 0);
}

TEST(NonPrintableSliceTests, Conversions)
{
    // We need to pass in the size, since the first character is '\0'. Otherwise, the length will be 0.
    std::string u("\x00\x01", 2);
    const Slice s(u);
    ASSERT_EQ(s.size(), 2);
    ASSERT_EQ(s[0], '\x00');
    ASSERT_EQ(s[1], '\x01');
}

TEST(NonPrintableSliceTests, CStyleStringLengths)
{
    const auto a = "ab";
    const char b[]{'4', '2', '\x00'};
    ASSERT_EQ(Slice(a).size(), 2);
    ASSERT_EQ(Slice(b).size(), 2);
}

TEST(NonPrintableSliceTests, NullByteInMiddleOfLiteralGivesIncorrectLength)
{
    const auto a = "\x12\x00\x34";
    const char b[]{'4', '\x00', '2', '\x00'};

    ASSERT_EQ(std::char_traits<char>::length(a), 1);
    ASSERT_EQ(std::char_traits<char>::length(b), 1);
    ASSERT_EQ(Slice(a).size(), 1);
    ASSERT_EQ(Slice(b).size(), 1);
}

template <class T>
auto run_nullability_check()
{
    const auto x = T::null();
    const T y(x.value + 1);

    ASSERT_TRUE(x.is_null());
    ASSERT_FALSE(y.is_null());
}

template <class T>
auto run_equality_comparisons()
{
    T x(1);
    T y(2);

    CALICODB_EXPECT_TRUE(x == x);
    CALICODB_EXPECT_TRUE(x != y);
    ASSERT_EQ(x, x);
    ASSERT_NE(x, y);
}

template <class T>
auto run_ordering_comparisons()
{
    T x(1);
    T y(2);

    CALICODB_EXPECT_TRUE(x < y);
    CALICODB_EXPECT_TRUE(x <= x and x <= y);
    ASSERT_LT(x, y);
    ASSERT_LE(x, x);
    ASSERT_LE(x, y);
}

TEST(IdTests, TypesAreSizedCorrectly)
{
    Id id;
    static_assert(Id::kSize == sizeof(id) && Id::kSize == sizeof(id.value));
}

TEST(IdTests, IdentifiersAreNullable)
{
    run_nullability_check<Id>();
    ASSERT_FALSE(Id::root().is_null());
    ASSERT_TRUE(Id::root().is_root());
}

TEST(IdTests, IdentifiersAreEqualityComparable)
{
    run_equality_comparisons<Id>();
}

TEST(IdTests, IdentifiersAreOrderable)
{
    run_ordering_comparisons<Id>();
}

TEST(StatusTests, StatusMessages)
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
    ASSERT_EQ("invalid argument: readonly", Status::invalid_argument(Status::kReadonly).to_string());
}

TEST(StatusTests, NonOkStatusSavesMessage)
{
    static constexpr auto message = "status message";
    auto s = Status::invalid_argument(message);
    ASSERT_EQ(s.to_string(), std::string("invalid argument: ") + message);
    ASSERT_TRUE(s.is_invalid_argument());
}

TEST(StatusTests, StatusCanBeCopied)
{
    const auto s = Status::invalid_argument("status message");
    const auto t = s;
    ASSERT_TRUE(t.is_invalid_argument());
    ASSERT_EQ(t.to_string(), std::string("invalid argument: ") + "status message");

    ASSERT_TRUE(s.is_invalid_argument());
    ASSERT_EQ(s.to_string(), std::string("invalid argument: ") + "status message");
}

TEST(StatusTests, StatusCanBeReassigned)
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

TEST(StatusTests, StatusCodesAreCorrect)
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

TEST(StatusTests, OkStatusCanBeCopied)
{
    const auto src = Status::ok();
    const auto dst = src;
    ASSERT_TRUE(src.is_ok());
    ASSERT_TRUE(dst.is_ok());
    ASSERT_EQ(src.to_string(), "OK");
    ASSERT_EQ(dst.to_string(), "OK");
}

TEST(StatusTests, NonOkStatusCanBeCopied)
{
    const auto src1 = Status::invalid_argument("status message");
    const auto src2 = Status::invalid_argument(Status::kReadonly);
    const auto dst1 = src1;
    const auto dst2 = src2;
    ASSERT_TRUE(src1.is_invalid_argument());
    ASSERT_TRUE(src2.is_invalid_argument());
    ASSERT_TRUE(dst1.is_invalid_argument());
    ASSERT_TRUE(dst2.is_invalid_argument());
    ASSERT_EQ(src1.to_string(), "invalid argument: status message");
    ASSERT_EQ(src2.to_string(), "invalid argument: readonly");
    ASSERT_EQ(dst1.to_string(), "invalid argument: status message");
    ASSERT_EQ(dst2.to_string(), "invalid argument: readonly");
    ASSERT_EQ(dst2.subcode(), Status::kReadonly);
}

TEST(StatusTests, OkStatusCanBeMoved)
{
    auto src = Status::ok();
    const auto dst = std::move(src);
    ASSERT_TRUE(src.is_ok());
    ASSERT_TRUE(dst.is_ok());
    ASSERT_EQ(src.to_string(), "OK");
    ASSERT_EQ(dst.to_string(), "OK");
}

TEST(StatusTests, NonOkStatusCanBeMoved)
{
    auto src = Status::invalid_argument("status message");
    const auto dst = std::move(src);
    ASSERT_TRUE(src.is_ok());
    ASSERT_TRUE(dst.is_invalid_argument());
    ASSERT_EQ(src.to_string(), "OK");
    ASSERT_EQ(dst.to_string(), "invalid argument: status message");
}

TEST(MiscTests, StringsUsesSizeParameterForComparisons)
{
    std::vector<std::string> v{
        std::string{"\x11\x00\x33", 3},
        std::string{"\x11\x00\x22", 3},
        std::string{"\x11\x00\x11", 3},
    };
    std::sort(begin(v), end(v));
    ASSERT_EQ(v[0][2], '\x11');
    ASSERT_EQ(v[1][2], '\x22');
    ASSERT_EQ(v[2][2], '\x33');
}

class InterceptorTests
    : public testing::Test,
      public EnvTestHarness<tools::TestEnv>
{
};

TEST_F(InterceptorTests, RespectsPrefix)
{
    QUICK_INTERCEPTOR("./test", tools::kSyscallOpen);

    File *editor;
    assert_special_error(env().new_file("./test", Env::kCreate, editor));
    ASSERT_OK(env().new_file("./wal", Env::kCreate, editor));
    delete editor;
}

TEST_F(InterceptorTests, RespectsSyscallType)
{
    QUICK_INTERCEPTOR("./test", tools::kSyscallWrite);

    File *editor;
    ASSERT_OK(env().new_file("./test", Env::kCreate, editor));
    assert_special_error(editor->write(0, {}));
    delete editor;
}

TEST(Logging, WriteFormattedString)
{
    std::string s;
    append_fmt_string(s, "%s %d %f", "abc", 42, 1.0);
}

TEST(Logging, LogMessage)
{
    tools::TestDir testdir(".");
    std::ofstream ofs(testdir.as_child("output"));
    std::string str(1'024, '0');
    tools::StreamSink sink(ofs);
    logv(&sink, "%s", str.c_str());
}

TEST(Logging, ConsumeDecimalNumberIgnoresLeadingZeros)
{
    U64 v;
    Slice slice("0000000123");
    consume_decimal_number(slice, &v);
    ASSERT_EQ(v, 123);
}

TEST(LevelDB_Logging, NumberToString)
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

TEST(LevelDB_Logging, ConsumeDecimalNumberRoundtrip)
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

TEST(LevelDB_Logging, ConsumeDecimalNumberRoundtripWithPadding)
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

TEST(LevelDB_Logging, ConsumeDecimalNumberOverflow)
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

TEST(LevelDB_Logging, ConsumeDecimalNumberNoDigits)
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

TEST(Logging, ConvenienceFunctions)
{
    std::string buffer;

    append_number(buffer, 123);
    ASSERT_EQ(buffer, number_to_string(123));
    buffer.clear();

    append_escaped_string(buffer, "\t\n\r");
    ASSERT_EQ(buffer, escape_string("\t\n\r"));
    buffer.clear();

    append_double(buffer, 1.0);
    ASSERT_EQ(buffer, double_to_string(1.0));
}

TEST(LevelDB_Coding, Varint64)
{
    // Construct the list of values to check
    std::vector<uint64_t> values;
    // Some special values
    values.push_back(0);
    values.push_back(100);
    values.push_back(~static_cast<uint64_t>(0));
    values.push_back(~static_cast<uint64_t>(0) - 1);
    for (uint32_t k = 0; k < 64; k++) {
        // Test values near powers of two
        const uint64_t power = 1ull << k;
        values.push_back(power);
        values.push_back(power - 1);
        values.push_back(power + 1);
    }
    std::size_t total_size = 0;
    for (auto v : values) {
        total_size += varint_length(v);
    }

    std::string s(total_size, '\0');
    auto *ptr = s.data();
    for (size_t i = 0; i < values.size(); i++) {
        ptr = encode_varint(ptr, values[i]);
    }

    const char *p = s.data();
    const char *limit = p + s.size();
    for (size_t i = 0; i < values.size(); i++) {
        ASSERT_TRUE(p < limit);
        uint64_t actual;
        const char *start = p;
        p = decode_varint(p, actual);
        ASSERT_TRUE(p != nullptr);
        ASSERT_EQ(values[i], actual);
        ASSERT_EQ(varint_length(actual), p - start);
    }
    ASSERT_EQ(p, limit);
}

TEST(LevelDB_Coding, Varint64Overflow)
{
    uint64_t result;
    std::string input("\x81\x82\x83\x84\x85\x81\x82\x83\x84\x85\x11");
    ASSERT_TRUE(decode_varint(input.data(), result) == nullptr);
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

} // namespace calicodb

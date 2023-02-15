#include <array>
#include <vector>
#include <gtest/gtest.h>

#include "calico/slice.h"
#include "unit_tests.h"
#include "utils/encoding.h"
#include "utils/crc.h"
#include "utils/scratch.h"
#include "utils/types.h"
#include "utils/utils.h"

namespace Calico {

#if not NDEBUG
TEST(TestUtils, ExpectationDeathTest)
{
    ASSERT_DEATH(CALICO_EXPECT_TRUE(false), EXPECTATION_MATCHER);
}
#endif // not NDEBUG

TEST(TestUtils, EncodingIsConsistent)
{
    Tools::RandomGenerator random;
    const auto u16 = random.Next<std::uint16_t>(std::uint16_t(-1));
    const auto u32 = random.Next<std::uint32_t>(std::uint32_t(-1));
    const auto u64 = random.Next<std::uint64_t>(std::uint64_t(-1));
    std::string buffer(sizeof(u16) + sizeof(u32) + sizeof(u64) + 1, '\x00');

    auto dst = buffer.data();
    put_u16(dst, u16);
    put_u32(dst += sizeof(std::uint16_t), u32);
    put_u64(dst + sizeof(std::uint32_t), u64);

    auto src = buffer.data();
    ASSERT_EQ(u16, get_u16(src));
    ASSERT_EQ(u32, get_u32(src += sizeof(std::uint16_t)));
    ASSERT_EQ(u64, get_u64(src += sizeof(std::uint32_t)));
    ASSERT_EQ(buffer.back(), 0) << "buffer overflow";
}

class SliceTests: public testing::Test {
protected:
    std::string test_string {"Hello, world!"};
    Slice slice {test_string};
};

TEST_F(SliceTests, EqualsSelf)
{
    ASSERT_TRUE(slice == slice);
}

TEST_F(SliceTests, StringLiteralSlice)
{
    ASSERT_TRUE(Slice {test_string} == Slice {"Hello, world!"});
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

TEST_F(SliceTests, FirstByteIsMostSignificant)
{
    ASSERT_TRUE(Slice {"10"} > Slice {"01"});
    ASSERT_TRUE(Slice {"01"} < Slice {"10"});
    ASSERT_TRUE(Slice {"10"} >= Slice {"01"});
    ASSERT_TRUE(Slice {"01"} <= Slice {"10"});
}

TEST_F(SliceTests, CanGetPartialRange)
{
    ASSERT_TRUE(slice.range(7, 5) == Slice {"world"});
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
    std::string s {"123"};
    Span b1 {s};
    Slice bv1 {s};
    ASSERT_TRUE(b1 == s); // Uses an implicit conversion.
    ASSERT_TRUE(bv1 == s);
}

TEST_F(SliceTests, WithCString)
{
    // Construct from and compare with C-style strings.
    char a[4] {"123"}; // Null-terminated
    Span b1 {a};
    Slice bv1 {a};
    ASSERT_TRUE(b1 == a);
    ASSERT_TRUE(bv1 == a);

    const char *s {"123"};
    Slice bv2 {s};
    ASSERT_TRUE(bv2 == s);
}

TEST_F(SliceTests, Conversions)
{
    std::string data {"abc"};
    Span b {data};
    Slice bv {b};
    ASSERT_TRUE(b == bv);
    [](Slice) {}(b);
}

static constexpr auto constexpr_test_write(Span b, Slice answer)
{
    CALICO_EXPECT_EQ(b.size(), answer.size());
    for (Size i {}; i < b.size(); ++i)
        b[i] = answer[i];

    (void)b.starts_with(answer);
    (void)b.data();
    (void)b.range(0, 0);
    (void)b.is_empty();
    b.advance(0);
    b.truncate(b.size());
}

static constexpr auto constexpr_test_read(Slice bv, Slice answer)
{
    for (Size i {}; i < bv.size(); ++i)
        CALICO_EXPECT_EQ(bv[i], answer[i]);

    (void)bv.starts_with(answer);
    (void)bv.data();
    (void)bv.range(0, 0);
    (void)bv.is_empty();
    bv.advance(0);
    bv.truncate(bv.size());
}

TEST_F(SliceTests, ConstantExpressions)
{
    static constexpr Slice bv {"42"};
    constexpr_test_read(bv, "42");

    char a[] {"42"};
    Span b {a};
    constexpr_test_write(b, "ab");
    constexpr_test_read(b, "ab");
}

TEST_F(SliceTests, SubRangesHaveProperType)
{
    Slice bv1 {"42"};
    auto bv2 = bv1.range(0);
    // NOTE: Extra parenthesis seem to be necessary. ASSERT_*() and EXPECT_*() don't like angle brackets.
    ASSERT_TRUE((std::is_same_v<Slice, decltype(bv2)>));

    auto s = bv1.to_string();
    Span b1 {s};
    auto b2 = b1.range(0);
    ASSERT_TRUE((std::is_same_v<Span, decltype(b2)>));
}

TEST(UtilsTest, ZeroIsNotAPowerOfTwo)
{
    ASSERT_FALSE(is_power_of_two(0));
}

TEST(UtilsTest, PowerOfTwoComputationIsCorrect)
{
    ASSERT_TRUE(is_power_of_two(1 << 1));
    ASSERT_TRUE(is_power_of_two(1 << 2));
    ASSERT_TRUE(is_power_of_two(1 << 10));
    ASSERT_TRUE(is_power_of_two(1 << 20));
}

TEST(ScratchTest, CanChangeUnderlyingBytesObject)
{
    std::string backing {"abc"};
    Span bytes {backing};
    Scratch scratch {bytes};
    scratch->advance(1);
    scratch->truncate(1);
    ASSERT_TRUE(*scratch == "b");
}

TEST(MonotonicScratchTest, ScratchesAreDistinct)
{
    MonotonicScratchManager manager {1, 2};
    auto s1 = manager.get();
    auto s2 = manager.get();
    (*s1)[0] = 1;
    (*s2)[0] = 2;
    ASSERT_EQ((*s1)[0], 1);
    ASSERT_EQ((*s2)[0], 2);
}

TEST(MonotonicScratchTest, ScratchesRepeat)
{
    MonotonicScratchManager manager {1, 2};
    (*manager.get())[0] = 1;
    (*manager.get())[0] = 2;
    ASSERT_EQ((*manager.get())[0], 1);
    ASSERT_EQ((*manager.get())[0], 2);
}

TEST(ScratchTest, ConvertsToSlice)
{
    static constexpr auto MSG = "Hello, world!";
    MonotonicScratchManager manager {std::strlen(MSG), 1};
    auto scratch = manager.get();

    mem_copy(*scratch, Slice {MSG});
    ASSERT_TRUE(*scratch == Slice {MSG});
    ASSERT_TRUE(scratch->starts_with("Hello"));
    ASSERT_TRUE(scratch->range(7, 5) == Slice {"world"});
    ASSERT_TRUE(scratch->advance(7).truncate(5) == Slice {"world"});
}

TEST(NonPrintableSliceTests, UsesStringSize)
{
    const std::string u {"\x00\x01", 2};
    ASSERT_EQ(Slice {u}.size(), 2);
}

TEST(NonPrintableSliceTests, NullBytesAreEqual)
{
    const std::string u {"\x00", 1};
    const std::string v {"\x00", 1};
    ASSERT_EQ(compare_three_way(Slice {u}, Slice {v}), ThreeWayComparison::EQ);
}

TEST(NonPrintableSliceTests, ComparisonDoesNotStopAtNullBytes)
{
    std::string u {"\x00\x00", 2};
    std::string v {"\x00\x01", 2};
    ASSERT_EQ(compare_three_way(Slice {u}, v), ThreeWayComparison::LT);
}

TEST(NonPrintableSliceTests, BytesAreUnsignedWhenCompared)
{
    std::string u {"\x0F", 1};
    std::string v {"\x00", 1};
    v[0] = static_cast<char>(0xF0);

    // Signed comparison. 0xF0 overflows a signed byte and becomes negative.
    ASSERT_LT(v[0], u[0]);

    // Unsigned comparison should come out the other way.
    ASSERT_EQ(compare_three_way(Slice {u}, v), ThreeWayComparison::LT);
}

TEST(NonPrintableSliceTests, Conversions)
{
    // We need to pass in the size, since the first character is '\0'. Otherwise, the length will be 0.
    std::string u {"\x00\x01", 2};
    const Span s {u};
    ASSERT_EQ(s.size(), 2);
    ASSERT_EQ(s[0], '\x00');
    ASSERT_EQ(s[1], '\x01');
}

TEST(NonPrintableSliceTests, CStyleStringLengths)
{
    const auto a = "ab";
    const char b[] {'4', '2', '\x00'};
    ASSERT_EQ(Slice {a}.size(), 2);
    ASSERT_EQ(Slice {b}.size(), 2);
}

TEST(NonPrintableSliceTests, ModifyCharArray)
{
    char data[] {'a', 'b', '\x00'};
    Span bytes {data};
    bytes[0] = '4';
    bytes.advance();
    bytes[0] = '2';
    ASSERT_TRUE(Slice {data} == "42");
}

TEST(NonPrintableSliceTests, NullByteInMiddleOfLiteralGivesIncorrectLength)
{
    const auto a = "\x12\x00\x34";
    const char b[] {'4', '\x00', '2', '\x00'};

    ASSERT_EQ(std::char_traits<char>::length(a), 1);
    ASSERT_EQ(std::char_traits<char>::length(b), 1);
    ASSERT_EQ(Slice {a}.size(), 1);
    ASSERT_EQ(Slice {b}.size(), 1);
}

template<class T>
auto run_nullability_check()
{
    const auto x = T::null();
    const T y {x.value + 1};

    ASSERT_TRUE(x.is_null());
    ASSERT_FALSE(y.is_null());
}

template<class T>
auto run_equality_comparisons()
{
    T x {1};
    T y {2};

    CALICO_EXPECT_TRUE(x == x);
    CALICO_EXPECT_TRUE(x != y);
    ASSERT_EQ(x, x);
    ASSERT_NE(x, y);
}

template<class T>
auto run_ordering_comparisons()
{
    T x {1};
    T y {2};

    CALICO_EXPECT_TRUE(x < y);
    CALICO_EXPECT_TRUE(x <= x and x <= y);
    CALICO_EXPECT_TRUE(y > x);
    CALICO_EXPECT_TRUE(y >= y and y >= x);
    ASSERT_LT(x, y);
    ASSERT_LE(x, x);
    ASSERT_LE(x, y);
    ASSERT_GT(y, x);
    ASSERT_GE(y, y);
    ASSERT_GE(y, x);
}

TEST(IdTests, TypesAreSizedCorrectly)
{
    Id id;
    static_assert(sizeof(Id) == sizeof(id.value));
    static_assert(sizeof(Id) == sizeof(id.value));
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

TEST(TestUniqueNullable, ResourceIsMoved)
{
    UniqueNullable<int> moved_from {42};
    const auto moved_into = std::move(moved_from);
    ASSERT_EQ(*moved_from, 0);
    ASSERT_FALSE(moved_from.is_valid());
    ASSERT_EQ(*moved_into, 42);
    ASSERT_TRUE(moved_into.is_valid());
}

TEST(StatusTests, OkStatusHasNoMessage)
{
    auto s = Status::ok();
    ASSERT_TRUE(s.what().is_empty());
}

TEST(StatusTests, NonOkStatusSavesMessage)
{
    static constexpr auto message = "status message";
    auto s = Status::invalid_argument(message);
    ASSERT_EQ(s.what().to_string(), message);
    ASSERT_TRUE(s.is_invalid_argument());
}

TEST(StatusTests, StatusCanBeCopied)
{
    const auto s = Status::invalid_argument("invalid argument");
    const auto t = s;
    ASSERT_TRUE(t.is_invalid_argument());
    ASSERT_EQ(t.what().to_string(), "invalid argument");

    ASSERT_TRUE(s.is_invalid_argument());
    ASSERT_EQ(s.what().to_string(), "invalid argument");
}

TEST(StatusTests, StatusCanBeReassigned)
{
    auto s = Status::ok();
    ASSERT_TRUE(s.is_ok());

    s = Status::invalid_argument("invalid argument");
    ASSERT_TRUE(s.is_invalid_argument());
    ASSERT_EQ(s.what().to_string(), "invalid argument");

    s = Status::logic_error("logic error");
    ASSERT_TRUE(s.is_logic_error());
    ASSERT_EQ(s.what().to_string(), "logic error");

    s = Status::ok();
    ASSERT_TRUE(s.is_ok());
}

TEST(StatusTests, StatusCodesAreCorrect)
{
    ASSERT_TRUE(Status::invalid_argument("invalid argument").is_invalid_argument());
    ASSERT_TRUE(Status::system_error("system error").is_system_error());
    ASSERT_TRUE(Status::logic_error("logic error").is_logic_error());
    ASSERT_TRUE(Status::corruption("corruption").is_corruption());
    ASSERT_TRUE(Status::not_found("not found").is_not_found());
    ASSERT_TRUE(Status::ok().is_ok());
}

TEST(StatusTests, OkStatusCanBeCopied)
{
    const auto src = Status::ok();
    const auto dst = src;
    ASSERT_TRUE(src.is_ok());
    ASSERT_TRUE(dst.is_ok());
    ASSERT_TRUE(src.what().is_empty());
    ASSERT_TRUE(dst.what().is_empty());
}

TEST(StatusTests, NonOkStatusCanBeCopied)
{
    const auto src = Status::invalid_argument("status message");
    const auto dst = src;
    ASSERT_TRUE(src.is_invalid_argument());
    ASSERT_TRUE(dst.is_invalid_argument());
    ASSERT_EQ(src.what().to_string(), "status message");
    ASSERT_EQ(dst.what().to_string(), "status message");
}

TEST(StatusTests, OkStatusCanBeMoved)
{
    auto src = Status::ok();
    const auto dst = std::move(src);
    ASSERT_TRUE(src.is_ok());
    ASSERT_TRUE(dst.is_ok());
    ASSERT_TRUE(src.what().is_empty());
    ASSERT_TRUE(dst.what().is_empty());
}

TEST(StatusTests, NonOkStatusCanBeMoved)
{
    auto src = Status::invalid_argument("status message");
    const auto dst = std::move(src);
    ASSERT_TRUE(src.is_ok());
    ASSERT_TRUE(dst.is_invalid_argument());
    ASSERT_TRUE(src.what().is_empty());
    ASSERT_EQ(dst.what().to_string(), "status message");
}

TEST(StatusTests, MessageIsNullTerminated)
{
    auto s = Status::system_error("hello");
    const auto msg = s.what();
    ASSERT_EQ(msg, "hello");
    ASSERT_EQ(msg.size(), 5);

    // This byte is not technically part of the slice, but should be owned by the Status object.
    ASSERT_EQ(msg.data()[5], '\0');
}

TEST(MiscTests, StringsUseSizeParameterForComparisons)
{
    std::vector<std::string> v {
        std::string {"\x11\x00\x33", 3},
        std::string {"\x11\x00\x22", 3},
        std::string {"\x11\x00\x11", 3},
    };
    std::sort(begin(v), end(v));
    ASSERT_EQ(v[0][2], '\x11');
    ASSERT_EQ(v[1][2], '\x22');
    ASSERT_EQ(v[2][2], '\x33');
}

// CRC tests from LevelDB.
namespace crc32c {

    TEST(CRC, StandardResults) {
        // From rfc3720 section B.4.
        char buf[32];

        memset(buf, 0, sizeof(buf));
        ASSERT_EQ(0x8a9136aa, Value(buf, sizeof(buf)));

        memset(buf, 0xff, sizeof(buf));
        ASSERT_EQ(0x62a8ab43, Value(buf, sizeof(buf)));

        for (int i = 0; i < 32; i++) {
            buf[i] = i;
        }
        ASSERT_EQ(0x46dd794e, Value(buf, sizeof(buf)));

        for (int i = 0; i < 32; i++) {
            buf[i] = 31 - i;
        }
        ASSERT_EQ(0x113fdb5c, Value(buf, sizeof(buf)));

        uint8_t data[48] = {
            0x01, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00,
            0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x18, 0x28, 0x00, 0x00, 0x00,
            0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        };
        ASSERT_EQ(0xd9963a56, Value(reinterpret_cast<char*>(data), sizeof(data)));
    }

    TEST(CRC, Values) { ASSERT_NE(Value("a", 1), Value("foo", 3)); }

    TEST(CRC, Extend) {
        ASSERT_EQ(Value("hello world", 11), Extend(Value("hello ", 6), "world", 5));
    }

    TEST(CRC, Mask) {
        uint32_t crc = Value("foo", 3);
        ASSERT_NE(crc, Mask(crc));
        ASSERT_NE(crc, Mask(Mask(crc)));
        ASSERT_EQ(crc, Unmask(Mask(crc)));
        ASSERT_EQ(crc, Unmask(Unmask(Mask(Mask(crc)))));
    }

}  // namespace crc32c

[[nodiscard]]
auto describe_size(Size size, int precision = 4) -> std::string
{
    static constexpr Size KiB {1'024};
    static constexpr auto MiB = KiB * KiB;
    static constexpr auto GiB = MiB * KiB;

    std::ostringstream oss;
    oss.precision(precision);

    if (size < KiB) {
        oss << size << " B";
    } else if (size < MiB) {
        oss << static_cast<double>(size) / KiB << " KiB";
    } else if (size < GiB) {
        oss << static_cast<double>(size) / MiB << " MiB";
    } else {
        oss << static_cast<double>(size) / GiB << " GiB";
    }
    return oss.str();
}

TEST(SizeDescriptorTests, ProducesSensibleResults)
{
    const auto high_precision = 13;
    ASSERT_EQ(describe_size(1ULL, high_precision), "1 B");
    ASSERT_EQ(describe_size(1'024ULL, high_precision), "1 KiB");
    ASSERT_EQ(describe_size(1'048'576ULL, high_precision), "1 MiB");
    ASSERT_EQ(describe_size(1'073'741'824ULL, high_precision), "1 GiB");

    ASSERT_EQ(describe_size(11 * 1ULL, high_precision), "11 B");
    ASSERT_EQ(describe_size(22 * 1'024ULL, high_precision), "22 KiB");
    ASSERT_EQ(describe_size(33 * 1'048'576ULL, high_precision), "33 MiB");
    ASSERT_EQ(describe_size(44 * 1'073'741'824ULL, high_precision), "44 GiB");

    ASSERT_EQ(describe_size(1'000ULL, 1), "1000 B");
    ASSERT_EQ(describe_size(10'000ULL, 3), "9.77 KiB");
}

class InterceptorTests : public InMemoryTest {

};

TEST_F(InterceptorTests, RespectsPrefix)
{
    Quick_Interceptor("test/data", Tools::Interceptor::OPEN);

    Editor *editor;
    assert_special_error(storage_handle().new_editor("test/data", &editor));
    expect_ok(storage_handle().new_editor("test/wal", &editor));
    delete editor;
}

TEST_F(InterceptorTests, RespectsSyscallType)
{
    Quick_Interceptor("test/data", Tools::Interceptor::WRITE);

    Editor *editor;
    expect_ok(storage_handle().new_editor("test/data", &editor));
    assert_special_error(editor->write({}, 0));
    delete editor;
}

TEST(LoggingTests, StringifiesNumbers)
{
    auto message = number_to_string(123);
    append_number(message, 4);
    append_number(message, 56);
    ASSERT_EQ(message, "123456");
}

TEST(LoggingTests, StringifiesMaximumNumber)
{
    auto n = std::numeric_limits<Size>::max();
    auto s = number_to_string(n);
    while (!s.empty()) {
        ASSERT_EQ(s.back(), n%10 + '0');
        s.pop_back();
        n /= 10;
    }
}

TEST(LoggingTests, EscapesStrings)
{
    auto message = escape_string("\x01\x02\x03");
    append_escaped_string(message, "\x04");
    append_escaped_string(message, "\x05\x06");
    ASSERT_EQ(message, "\\x01\\x02\\x03\\x04\\x05\\x06");
}

TEST(LoggingTests, OnlyEscapesUnprintableCharacters)
{
    for (Size i {}; i < 256; ++i) {
        char data[] {static_cast<char>(i)};
        const auto str = escape_string({data, 1});
        if (std::isprint(*data)) {
            ASSERT_EQ(str.front(), *data);
        } else {
            ASSERT_EQ(str.front(), '\\');
        }
    }
}

} // namespace Calico
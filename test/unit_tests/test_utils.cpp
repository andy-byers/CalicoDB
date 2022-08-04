#include <gtest/gtest.h>

#include "calico/bytes.h"
#include "calico/options.h"
#include "calico/status.h"
#include "random.h"
#include "unit_tests.h"
#include "utils/encoding.h"
#include "utils/expect.h"
#include "utils/identifier.h"
#include "utils/layout.h"
#include "utils/scratch.h"
#include "utils/types.h"
#include "utils/utils.h"

namespace {

using namespace cco;

TEST(AssertionDeathTest, Assert)
{
    ASSERT_DEATH(CCO_EXPECT_TRUE(false), BOOL_EXPECTATION_MATCHER);
}

TEST(TestEncoding, ReadsAndWrites)
{
    Random random{0};
    const auto u16 = random.next_int(std::numeric_limits<uint16_t>::max());
    const auto u32 = random.next_int(std::numeric_limits<uint32_t>::max());
    const auto u64 = random.next_int(std::numeric_limits<uint64_t>::max());
    std::vector<cco::Byte> buffer(sizeof(uint16_t) + sizeof(uint32_t) + sizeof(uint64_t) + 1);

    auto dst = buffer.data();
    put_u16(dst, u16);
    put_u32(dst += sizeof(uint16_t), u32);
    put_u64(dst += sizeof(uint32_t), u64);

    auto src = buffer.data();
    ASSERT_EQ(u16, get_u16(src));
    ASSERT_EQ(u32, get_u32(src += sizeof(uint16_t)));
    ASSERT_EQ(u64, get_u64(src += sizeof(uint32_t)));
    ASSERT_EQ(buffer.back(), 0) << "Buffer overflow";
}

class SliceTests: public testing::Test {
protected:
    std::string test_string {"Hello, world!"};
    Bytes bytes {stob(test_string)};
};

TEST_F(SliceTests, EqualsSelf)
{
    ASSERT_TRUE(bytes == bytes);
}

TEST_F(SliceTests, ShorterSlicesAreLessThanIfOtherwiseEqual)
{
    const auto shorter = bytes.range(0, bytes.size() - 1);
    ASSERT_TRUE(shorter < bytes);
}

TEST_F(SliceTests, FirstByteIsMostSignificant)
{
    ASSERT_TRUE(stob("10") > stob("01"));
    ASSERT_TRUE(stob("01") < stob("10"));
    ASSERT_TRUE(stob("10") >= stob("01"));
    ASSERT_TRUE(stob("01") <= stob("10"));
}

TEST_F(SliceTests, CanGetPartialRange)
{
    ASSERT_TRUE(bytes.range(7, 5) == stob("world"));
}

TEST_F(SliceTests, CanGetEntireRange)
{
    ASSERT_TRUE(bytes == bytes.range(0));
    ASSERT_TRUE(bytes == bytes.range(0, bytes.size()));
}

TEST_F(SliceTests, EmptyRangesAreEmpty)
{
    ASSERT_TRUE(bytes.range(0, 0).is_empty());
}

TEST_F(SliceTests, RangeDeathTest)
{
    BytesView discard;
    ASSERT_DEATH(discard = bytes.range(bytes.size() + 1), "Assert");
    ASSERT_DEATH(discard = bytes.range(bytes.size(), 1), "Assert");
    ASSERT_DEATH(discard = bytes.range(0, bytes.size() + 1), "Assert");
    ASSERT_DEATH(discard = bytes.range(5, bytes.size()), "Assert");
}

TEST_F(SliceTests, AdvanceByZeroDoesNothing)
{
    auto copy = bytes;
    bytes.advance(0);
    ASSERT_TRUE(bytes == copy);
}

TEST_F(SliceTests, AdvancingByOwnLengthProducesEmptySlice)
{
    bytes.advance(bytes.size());
    ASSERT_TRUE(bytes.is_empty());
}

TEST_F(SliceTests, AdvanceDeathTest)
{
    ASSERT_DEATH(bytes.advance(bytes.size() + 1), "Assert");
}

TEST_F(SliceTests, TruncatingToOwnLengthDoesNothing)
{
    auto copy = bytes;
    bytes.truncate(bytes.size());
    ASSERT_TRUE(bytes == copy);
}

TEST_F(SliceTests, TruncatingToZeroLengthProducesEmptySlice)
{
    bytes.truncate(0);
    ASSERT_TRUE(bytes.is_empty());
}

TEST_F(SliceTests, TruncatingEmptySliceDoesNothing)
{
    bytes.truncate(0);
    auto copy = bytes;
    bytes.truncate(0);
    ASSERT_TRUE(bytes == copy);
}

TEST_F(SliceTests, TruncateDeathTest)
{
    ASSERT_DEATH(bytes.truncate(bytes.size() + 1), "Assert");
    bytes.truncate(0);
    ASSERT_DEATH(bytes.truncate(1), "Assert");
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

TEST(ScratchTest, ScratchesAreUnique)
{
    RollingScratchManager manager {1};
    auto s1 = manager.get();
    auto s2 = manager.get();
    auto s3 = manager.get();
    s1.data()[0] = 1;
    s2.data()[0] = 2;
    s3.data()[0] = 3;
    ASSERT_EQ(s1.data()[0], 1);
    ASSERT_EQ(s2.data()[0], 2);
    ASSERT_EQ(s3.data()[0], 3);
}

TEST(NonPrintableSliceTests, UsesStringSize)
{
    // We can construct a string holding non-printable bytes by specifying its size along with
    // a string literal that is not null-terminated.
    std::string u {"\x00\x01", 2};
    ASSERT_EQ(stob(u).size(), 2);
}

TEST(NonPrintableSliceTests, NullBytesAreEqual)
{
    std::string u {"\x00", 1};
    std::string v {"\x00", 1};
    ASSERT_EQ(compare_three_way(stob(u), stob(v)), ThreeWayComparison::EQ);
}

TEST(NonPrintableSliceTests, ComparisonDoesNotStopAtNullBytes)
{
    std::string u {"\x00\x00", 2};
    std::string v {"\x00\x01", 2};
    ASSERT_EQ(compare_three_way(stob(u), stob(v)), ThreeWayComparison::LT);
}

TEST(NonPrintableSliceTests, BytesAreUnsignedWhenCompared)
{
    std::string u {"\x0F", 1};
    std::string v {"\x00", 1};
    v[0] = static_cast<char>(0xF0);

    // Signed comparison. 0xF0 overflows a signed byte and becomes negative.
    ASSERT_LT(v[0], u[0]);

    // Unsigned comparison should come out the other way.
    ASSERT_EQ(compare_three_way(stob(u), stob(v)), ThreeWayComparison::LT);
}

TEST(NonPrintableSliceTests, Conversions)
{
    std::string u {"\x00\x01", 2};
    const auto s = btos(stob(u));
    ASSERT_EQ(s.size(), 2);
    ASSERT_EQ(s[0], '\x00');
    ASSERT_EQ(s[1], '\x01');
}

template<class Id> auto run_comparisons()
{
    Id a {1ULL};
    Id b {2ULL};

    ASSERT_EQ(a, a);
    CCO_EXPECT_EQ(a, a);
    CCO_EXPECT_TRUE(a == a);

    ASSERT_NE(a, b);
    CCO_EXPECT_NE(a, b);
    CCO_EXPECT_TRUE(a != b);

    ASSERT_LT(a, b);
    CCO_EXPECT_LT(a, b);
    CCO_EXPECT_TRUE(a < b);

    ASSERT_LE(a, a);
    ASSERT_LE(a, b);
    CCO_EXPECT_LE(a, a);
    CCO_EXPECT_LE(a, b);
    CCO_EXPECT_TRUE(a <= a);
    CCO_EXPECT_TRUE(a <= b);

    ASSERT_GT(b, a);
    CCO_EXPECT_GT(b, a);
    CCO_EXPECT_TRUE(b > a);

    ASSERT_GE(a, a);
    ASSERT_GE(b, a);
    CCO_EXPECT_GE(a, a);
    CCO_EXPECT_GE(b, a);
    CCO_EXPECT_TRUE(a >= a);
    CCO_EXPECT_TRUE(b >= a);
}

TEST(IdentifierTest, PIDsAreComparable)
{
    run_comparisons<PageId>();
}

TEST(IdentifierTest, LSNsAreComparable)
{
    run_comparisons<SequenceNumber>();
}

template<class Id> auto run_addition()
{
    const auto res = Id {1} + Id {2};
    ASSERT_EQ(res.value, 3);
}

TEST(IdentifierTest, PIDsCanBeAdded)
{
    run_comparisons<PageId>();
}

TEST(IdentifierTest, LSNsCanBeAdded)
{
    run_comparisons<SequenceNumber>();
}

TEST(IdentifierTest, LSNsCanBeIncremented)
{
    auto lsn = SequenceNumber::null();
    ASSERT_EQ(lsn.value, 0);
    lsn++;
    ASSERT_EQ(lsn.value, 1);
    ++lsn;
    ASSERT_EQ(lsn.value, 2);
}

TEST(TestUniqueNullable, ResourceIsMoved)
{
    UniqueNullable<int> moved_from {123};
    const auto moved_into = std::move(moved_from);
    ASSERT_EQ(*moved_from, 0);
    ASSERT_FALSE(moved_from.is_valid());
    ASSERT_EQ(*moved_into, 123);
    ASSERT_TRUE(moved_into.is_valid());
}

TEST(CellSizeTests, AtLeastFourCellsCanFitInAnInternalNonRootNode)
{
    const auto start = NodeLayout::header_offset(PageId {2ULL}) +
                       NodeLayout::HEADER_SIZE +
                       CELL_POINTER_SIZE;
    Size page_size {MINIMUM_PAGE_SIZE};
    while (page_size <= MAXIMUM_PAGE_SIZE) {
        const auto max_local = get_max_local(page_size) + MAX_CELL_HEADER_SIZE;
        ASSERT_LE(max_local * 4, page_size - start);
        page_size <<= 1;
    }
}

TEST(StatusTests, StatusCodesAreCorrect)
{
    ASSERT_TRUE(Status::invalid_argument("").is_invalid_argument());
    ASSERT_TRUE(Status::system_error("").is_system_error());
    ASSERT_TRUE(Status::logic_error("").is_logic_error());
    ASSERT_TRUE(Status::corruption("").is_corruption());
    ASSERT_TRUE(Status::not_found().is_not_found());
    ASSERT_TRUE(Status::ok().is_ok());
}

} // <anonymous>
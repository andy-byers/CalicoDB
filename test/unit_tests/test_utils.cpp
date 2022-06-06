#include <gtest/gtest.h>

#include "cub/bytes.h"
#include "cub/common.h"
#include "random.h"
#include "unit_tests.h"
#include "utils/assert.h"
#include "utils/encoding.h"
#include "utils/identifier.h"
#include "utils/scratch.h"
#include "utils/utils.h"

namespace {

using namespace cub;

TEST(AssertionDeathTest, Assert)
{
    ASSERT_DEATH(CUB_EXPECT(false), EXPECTATION_MATCHER);
}

TEST(TestEncoding, ReadsAndWrites)
{
    Random random{0};
    const auto u16 = random.next_int(std::numeric_limits<uint16_t>::max());
    const auto u32 = random.next_int(std::numeric_limits<uint32_t>::max());
    const auto u64 = random.next_int(std::numeric_limits<uint64_t>::max());
    std::vector<Byte> buffer(sizeof(uint16_t) + sizeof(uint32_t) + sizeof(uint64_t) + 1);

    auto dst = buffer.data();
    put_uint16(dst, u16);
    put_uint32(dst += sizeof(uint16_t), u32);
    put_uint64(dst += sizeof(uint32_t), u64);

    auto src = buffer.data();
    ASSERT_EQ(u16, get_uint16(src));
    ASSERT_EQ(u32, get_uint32(src += sizeof(uint16_t)));
    ASSERT_EQ(u64, get_uint64(src += sizeof(uint32_t)));
    ASSERT_EQ(buffer.back(), 0) << "Buffer overflow";
}

class SliceTests: public testing::Test {
protected:
    std::string test_string {"Hello, world!"};
    Bytes bytes {_b(test_string)};
};

TEST_F(SliceTests, EqualsSelf)
{
    ASSERT_TRUE(bytes == bytes);
}

TEST_F(SliceTests, ShorterSlicesAreLessThan)
{
    const auto shorter = bytes.range(0, bytes.size() - 1);
    ASSERT_TRUE(shorter < bytes);
}

TEST_F(SliceTests, CanGetPartialRange)
{
    ASSERT_TRUE(bytes.range(7, 5) == _b("world"));
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
    ASSERT_DEATH(discard = bytes.range(bytes.size() + 1), EXPECTATION_MATCHER);
    ASSERT_DEATH(discard = bytes.range(bytes.size(), 1), EXPECTATION_MATCHER);
    ASSERT_DEATH(discard = bytes.range(0, bytes.size() + 1), EXPECTATION_MATCHER);
    ASSERT_DEATH(discard = bytes.range(5, bytes.size()), EXPECTATION_MATCHER);
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
    ASSERT_DEATH(bytes.advance(bytes.size() + 1), EXPECTATION_MATCHER);
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
    ASSERT_DEATH(bytes.truncate(bytes.size() + 1), EXPECTATION_MATCHER);
    bytes.truncate(0);
    ASSERT_DEATH(bytes.truncate(1), EXPECTATION_MATCHER);
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
    ScratchManager manager {1};
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

template<class Id> auto run_comparisons()
{
    Id a {1};
    Id b {2};

    ASSERT_EQ(a, a);
    CUB_EXPECT_EQ(a, a);
    CUB_EXPECT_TRUE(a == a);

    ASSERT_NE(a, b);
    CUB_EXPECT_NE(a, b);
    CUB_EXPECT_TRUE(a != b);

    ASSERT_LT(a, b);
    CUB_EXPECT_LT(a, b);
    CUB_EXPECT_TRUE(a < b);

    ASSERT_LE(a, a);
    ASSERT_LE(a, b);
    CUB_EXPECT_LE(a, a);
    CUB_EXPECT_LE(a, b);
    CUB_EXPECT_TRUE(a <= a);
    CUB_EXPECT_TRUE(a <= b);

    ASSERT_GT(b, a);
    CUB_EXPECT_GT(b, a);
    CUB_EXPECT_TRUE(b > a);

    ASSERT_GE(a, a);
    ASSERT_GE(b, a);
    CUB_EXPECT_GE(a, a);
    CUB_EXPECT_GE(b, a);
    CUB_EXPECT_TRUE(a >= a);
    CUB_EXPECT_TRUE(b >= a);
}

TEST(IdentifierTest, PIDsAreComparable)
{
    run_comparisons<PID>();
}

TEST(IdentifierTest, LSNsAreComparable)
{
    run_comparisons<LSN>();
}

template<class Id> auto run_addition()
{
    const auto res = Id {1} + Id {2};
    ASSERT_EQ(res.value, 3);
}

TEST(IdentifierTest, PIDsCanBeAdded)
{
    run_comparisons<PID>();
}

TEST(IdentifierTest, LSNsCanBeAdded)
{
    run_comparisons<LSN>();
}

TEST(IdentifierTest, LSNsCanBeIncremented)
{
    LSN lsn {0};
    ASSERT_EQ(lsn.value, 0);
    lsn++;
    ASSERT_EQ(lsn.value, 1);
    ++lsn;
    ASSERT_EQ(lsn.value, 2);
}

} // <anonymous>
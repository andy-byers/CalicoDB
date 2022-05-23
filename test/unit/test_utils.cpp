#include <gtest/gtest.h>

#include "common.h"
#include "random.h"
#include "utils/assert.h"
#include "utils/encoding.h"
#include "utils/scratch.h"
#include "bytes.h"
#include "utils/utils.h"
#include "unit.h"

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

TEST_F(SliceTests, ShorterSlicesCompareAsLessThan)
{
    ASSERT_TRUE(_b(test_string.substr(0, test_string.size() - 1)) < bytes);
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

TEST_F(SliceTests, CanAdvanceToEnd)
{
    bytes.advance(bytes.size());
    ASSERT_TRUE(bytes.is_empty());
}

TEST_F(SliceTests, AdvanceDeathTest)
{
    ASSERT_DEATH(bytes.advance(bytes.size() + 1), EXPECTATION_MATCHER);
}

TEST_F(SliceTests, TruncateToSameSizeDoesNothing)
{
    auto copy = bytes;
    bytes.truncate(bytes.size());
    ASSERT_TRUE(bytes == copy);
}

TEST_F(SliceTests, CanTruncateToEmpty)
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

TEST_F(SliceTests, CanAdvanceAndTruncate)
{
    bytes.advance(3);
    bytes.truncate(bytes.size() - 2);
    bytes.advance(4);
    bytes.truncate(bytes.size() - 3);
    ASSERT_EQ(_s(bytes), "w");
}

TEST(UtilsTest, ZeroIsNotAPowerOfTwo)
{
    ASSERT_FALSE(is_power_of_two(0));
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

} // <anonymous>
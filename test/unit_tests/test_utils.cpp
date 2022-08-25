#include <gtest/gtest.h>
#include <thread>

#include "calico/bytes.h"
#include "calico/options.h"
#include "calico/status.h"
#include "random.h"
#include "unit_tests.h"
#include "utils/encoding.h"
#include "utils/expect.h"
#include "utils/layout.h"
#include "utils/queue.h"
#include "utils/scratch.h"
#include "utils/types.h"
#include "utils/utils.h"

namespace {

using namespace calico;

TEST(AssertionDeathTest, Assert)
{
    ASSERT_DEATH(CALICO_EXPECT_TRUE(false), EXPECTATION_MATCHER);
}

TEST(TestEncoding, ReadsAndWrites)
{
    Random random{0};
    const auto u16 = random.next_int(std::numeric_limits<uint16_t>::max());
    const auto u32 = random.next_int(std::numeric_limits<uint32_t>::max());
    const auto u64 = random.next_int(std::numeric_limits<uint64_t>::max());
    std::vector<calico::Byte> buffer(sizeof(uint16_t) + sizeof(uint32_t) + sizeof(uint64_t) + 1);

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

TEST_F(SliceTests, StringLiteralSlice)
{
    ASSERT_TRUE(stob(test_string) == stob("Hello, world!"));
}

TEST_F(SliceTests, StartsWith)
{
    ASSERT_TRUE(stob("Hello, world!").starts_with(stob("Hello")));
    ASSERT_FALSE(stob("Hello, world!").starts_with(stob(" Hello")));
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

TEST(MonotonicScratchTest, ScratchesAreDistinct)
{
    MonotonicScratchManager<3> manager {1};
    auto s1 = manager.get();
    auto s2 = manager.get();
    auto s3 = manager.get();
    (*s1)[0] = 1;
    (*s2)[0] = 2;
    (*s3)[0] = 3;
    ASSERT_EQ((*s1)[0], 1);
    ASSERT_EQ((*s2)[0], 2);
    ASSERT_EQ((*s3)[0], 3);
}

TEST(MonotonicScratchTest, ScratchesRepeat)
{
    MonotonicScratchManager<3> manager {1};
    (*manager.get())[0] = 1;
    (*manager.get())[0] = 2;
    (*manager.get())[0] = 3;
    ASSERT_EQ((*manager.get())[0], 1);
    ASSERT_EQ((*manager.get())[0], 2);
    ASSERT_EQ((*manager.get())[0], 3);
}

TEST(ScratchTest, BehavesLikeASlice)
{
    static constexpr auto MSG = "Hello, world!";
    MonotonicScratchManager<1> manager {strlen(MSG)};
    auto scratch = manager.get();

    mem_copy(*scratch, stob(MSG));
    ASSERT_TRUE(*scratch == stob(MSG));
    ASSERT_TRUE(scratch->starts_with(stob("Hello")));
    ASSERT_TRUE(scratch->range(7, 5) == stob("world"));
    ASSERT_TRUE(scratch->advance(7).truncate(5) == stob("world"));
}

TEST(NonPrintableSliceTests, UsesStringSize)
{
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

TEST(NonPrintableSliceTests, CStyleStringLengths)
{
    const auto a = "ab";
    const char b[] {'4', '2', '\x00'};
    ASSERT_EQ(stob(a).size(), 2);
    ASSERT_EQ(stob(b).size(), 2);
}

TEST(NonPrintableSliceTests, ModifyCharArray)
{
    char data[] {'a', 'b', '\x00'};
    auto bytes = stob(data);
    bytes[0] = '4';
    bytes.advance();
    bytes[0] = '2';
    ASSERT_TRUE(stob(data) == stob("42"));
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
    CALICO_EXPECT_TRUE(x == 1);
    CALICO_EXPECT_TRUE(x != y);
    CALICO_EXPECT_TRUE(x != 2);
    ASSERT_EQ(x, x);
    ASSERT_EQ(x, 1);
    ASSERT_NE(x, y);
    ASSERT_NE(x, 2);
}

template<class T>
auto run_ordering_comparisons()
{
    T x {1};
    T y {2};

    CALICO_EXPECT_TRUE(x < y and x < 2);
    CALICO_EXPECT_TRUE(x <= x and x <= y);
    CALICO_EXPECT_TRUE(x <= 1 and x <= 2);
    CALICO_EXPECT_TRUE(y > x and y > 1);
    CALICO_EXPECT_TRUE(y >= y and y >= x);
    CALICO_EXPECT_TRUE(y >= 2 and y >= 1);
    ASSERT_LT(x, y);
    ASSERT_LT(x, 2);
    ASSERT_LE(x, x);
    ASSERT_LE(x, y);
    ASSERT_LE(x, 1);
    ASSERT_LE(x, 2);
    ASSERT_GT(y, x);
    ASSERT_GT(y, 1);
    ASSERT_GE(y, y);
    ASSERT_GE(y, x);
    ASSERT_GE(y, 2);
    ASSERT_GE(y, 1);
}

TEST(SimpleDSLTests, TypesAreSizedCorrectly)
{
    static_assert(sizeof(PageId) == sizeof(PageId::Type));
    static_assert(sizeof(SequenceId) == sizeof(SequenceId::Type));
}

TEST(SimpleDSLTests, PageIdsAreNullable)
{
    run_nullability_check<PageId>();
    ASSERT_FALSE(PageId::root().is_null());
    ASSERT_TRUE(PageId::root().is_root());
}

TEST(SimpleDSLTests, SequenceIdsAreNullable)
{
    run_nullability_check<SequenceId>();
    ASSERT_FALSE(SequenceId::base().is_null());
    ASSERT_TRUE(SequenceId::base().is_base());
}

TEST(SimpleDSLTests, PageIdsAreEqualityComparable)
{
    run_equality_comparisons<PageId>();
}

TEST(SimpleDSLTests, SequenceIdsAreEqualityComparable)
{
    run_equality_comparisons<SequenceId>();
}

TEST(SimpleDSLTests, SequenceIdsAreOrderable)
{
    run_ordering_comparisons<SequenceId>();
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

TEST(CellSizeTests, AtLeastFourCellsCanFitInAnInternalNonRootNode)
{
    const auto start = NodeLayout::header_offset(PageId {2}) +
                       NodeLayout::HEADER_SIZE +
                       CELL_POINTER_SIZE;
    Size page_size {MINIMUM_PAGE_SIZE};
    while (page_size <= MAXIMUM_PAGE_SIZE) {
        const auto max_local = get_max_local(page_size) + MAX_CELL_HEADER_SIZE;
        ASSERT_LE(max_local * 4, page_size - start);
        page_size <<= 1;
    }
}

TEST(StatusTests, OkStatusHasNoMessage)
{
    auto s = Status::ok();
    ASSERT_TRUE(s.what().empty());
}

TEST(StatusTests, NonOkStatusSavesMessage)
{
    static constexpr auto message = "status message";
    auto s = Status::invalid_argument(message);
    ASSERT_EQ(s.what(), message);
    ASSERT_TRUE(s.is_invalid_argument());
}

TEST(StatusTests, StatusCanBeCopied)
{
    auto s = Status::invalid_argument("invalid argument");
    auto t = s;
    ASSERT_TRUE(t.is_invalid_argument());
    ASSERT_EQ(t.what(), "invalid argument");

    t = Status::ok();
    ASSERT_TRUE(s.is_invalid_argument());
    ASSERT_EQ(s.what(), "invalid argument");
}

TEST(StatusTests, StatusCanBeReassigned)
{
    auto s = Status::ok();
    ASSERT_TRUE(s.is_ok());

    s = Status::invalid_argument("invalid argument");
    ASSERT_TRUE(s.is_invalid_argument());
    ASSERT_EQ(s.what(), "invalid argument");

    s = Status::logic_error("logic error");
    ASSERT_TRUE(s.is_logic_error());
    ASSERT_EQ(s.what(), "logic error");

    s = Status::ok();
    ASSERT_TRUE(s.is_ok());
}

// Bad idea??? Uses const reference lifetime extension to name "test", which can be an lvalue or rvalue. Then we use the name in a more
// complicated expression. Seems pretty useful for creating more complicated asserts.
#define CALICO_TEST(test, expression) \
    do { \
        const auto &_test = (test);  \
        CALICO_EXPECT_TRUE(expression); \
    } while (0)

TEST(MacroTest, WeirdMacro)
{
    CALICO_TEST(75 * 2 + 10, _test >= 100 and _test <= 200);
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
    auto src = Status::ok();
    auto dst = src;
    ASSERT_TRUE(src.is_ok());
    ASSERT_TRUE(dst.is_ok());
    ASSERT_TRUE(src.what().empty());
    ASSERT_TRUE(dst.what().empty());
}

TEST(StatusTests, NonOkStatusCanBeCopied)
{
    auto src = Status::invalid_argument("status message");
    auto dst = src;
    ASSERT_TRUE(src.is_invalid_argument());
    ASSERT_TRUE(dst.is_invalid_argument());
    ASSERT_EQ(src.what(), "status message");
    ASSERT_EQ(dst.what(), "status message");
}

TEST(StatusTests, OkStatusCanBeMoved)
{
    auto src = Status::ok();
    auto dst = std::move(src);
    ASSERT_TRUE(src.is_ok());
    ASSERT_TRUE(dst.is_ok());
    ASSERT_TRUE(src.what().empty());
    ASSERT_TRUE(dst.what().empty());
}

TEST(StatusTests, NonOkStatusCanBeMoved)
{
    auto src = Status::invalid_argument("status message");
    auto dst = std::move(src);
    ASSERT_TRUE(src.is_ok());
    ASSERT_TRUE(dst.is_invalid_argument());
    ASSERT_TRUE(src.what().empty());
    ASSERT_EQ(dst.what(), "status message");
}

TEST(SimpleDSLTests, Size)
{
    PageId a {1UL};
    PageId b {2UL};
    CALICO_EXPECT_EQ(a, 1UL);
    CALICO_EXPECT_NE(b, 1UL);
}

//auto transfer_n(Queue<int> *queue, std::vector<std::optional<int>> *out, Size n) -> void*
//{
//    for (Size i {}; i < n; ++i)
//        out->emplace_back(queue->dequeue());
//    return nullptr;
//}




// Modified from RocksDB.
class QueueTests: public testing::Test {
public:
    static constexpr Size NUM_ELEMENTS {500};

    QueueTests() = default;
    ~QueueTests() override = default;

    std::array<Size, NUM_ELEMENTS> data {};
    mutable std::mutex mutex;
    Queue<Size> queue;
};

struct Consumer {
    auto operator()() -> void
    {
        std::optional<Size> next;
        while ((next = queue->dequeue())) {
            std::lock_guard lock {*mu};
            out[*next] = *next;
        }
    }

    std::mutex *mu {};
    Queue<Size> *queue {};
    Size *out {};
};

TEST_F(QueueTests, EnqueueAndDequeueST)
{
    queue.enqueue(1UL);
    queue.enqueue(2UL);
    queue.enqueue(3UL);
    ASSERT_EQ(queue.dequeue(), 1);
    ASSERT_EQ(queue.dequeue(), 2);
    ASSERT_EQ(queue.dequeue(), 3);
}

TEST_F(QueueTests, SingleProducerMultipleConsumers)
{
    static constexpr Size NUM_GROUPS {5};
    std::vector<std::thread> consumers;
    for (Size i {}; i < NUM_GROUPS; ++i)
        consumers.emplace_back(Consumer {&mutex, &queue, data.data()});

    for (Size i {}; i < NUM_ELEMENTS; ++i)
        queue.enqueue(i);

    queue.finish();

    for (auto &thread: consumers)
        thread.join();

    queue.wait_until_finish();

    Size answer {};
    ASSERT_TRUE(std::all_of(cbegin(data), cend(data), [&answer](auto result) {
        return result == answer++;
    }));
}

TEST_F(QueueTests, MultipleProducersMultipleConsumers)
{
    static constexpr Size NUM_GROUPS {5};
    static constexpr auto GROUP_SIZE = NUM_ELEMENTS / NUM_GROUPS;
    static_assert(GROUP_SIZE * NUM_GROUPS == NUM_ELEMENTS);

    std::vector<std::thread> consumers;
    for (Size i {}; i < NUM_GROUPS; ++i)
        consumers.emplace_back(Consumer {&mutex, &queue, data.data()});

    std::vector<std::thread> producers;
    for (Size i {}; i < NUM_GROUPS; ++i) {
        producers.emplace_back([i, this] {
            for (Size j {}; j < GROUP_SIZE; ++j)
                queue.enqueue(j + i*GROUP_SIZE);
        });
    }

    for (auto &thread: producers)
        thread.join();

    queue.finish();

    for (auto &thread: consumers)
        thread.join();

    Size answer {};
    ASSERT_TRUE(std::all_of(cbegin(data), cend(data), [&answer](auto result) {
        return result == answer++;
    }));
}

} // <anonymous>
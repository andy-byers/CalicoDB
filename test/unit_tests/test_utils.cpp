#include <array>
#include <thread>
#include <vector>
#include <gtest/gtest.h>

#include "calico/bytes.h"
#include "calico/options.h"
#include "calico/status.h"
#include "random.h"
#include "unit_tests.h"
#include "utils/encoding.h"
#include "utils/expect.h"
#include "utils/header.h"
#include "utils/layout.h"
#include "utils/queue.h"
#include "utils/scratch.h"
#include "utils/types.h"
#include "utils/utils.h"
#include "utils/worker.h"

namespace Calico {

TEST(AssertionDeathTest, Assert)
{
    ASSERT_DEATH(CALICO_EXPECT_TRUE(false), EXPECTATION_MATCHER);
}

TEST(TestEncoding, ReadsAndWrites)
{
    Random random{0};
    const auto u16 = random.get<std::uint16_t>();
    const auto u32 = random.get<std::uint32_t>();
    const auto u64 = random.get<std::uint64_t>();
    std::vector<Calico::Byte> buffer(sizeof(uint16_t) + sizeof(uint32_t) + sizeof(uint64_t) + 1);

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
    ASSERT_FALSE(stob("1").starts_with(stob("123")));
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

TEST_F(SliceTests, WithCppString)
{
    // Construct from and compare with C++ strings.
    std::string s {"123"};
    Bytes b1 {s};
    BytesView bv1 {s};
    ASSERT_TRUE(b1 == s); // Uses an implicit conversion.
    ASSERT_TRUE(bv1 == s);

    std::string_view sv {"123"};
    BytesView bv2 {sv};
    ASSERT_TRUE(bv2 == sv);
    ASSERT_TRUE(bv2 != std::string {"321"});
}

TEST_F(SliceTests, WithCString)
{
    // Construct from and compare with C-style strings.
    char a[4] {"123"}; // Null-terminated
    Bytes b1 {a};
    BytesView bv1 {a};
    ASSERT_TRUE(b1 == a);
    ASSERT_TRUE(bv1 == a);

    const char *s {"123"};
    BytesView bv2 {s};
    ASSERT_TRUE(bv2 == s);
}

TEST_F(SliceTests, Conversions)
{
    std::string data {"abc"};
    Bytes b {data};
    BytesView bv {b};
    ASSERT_TRUE(b == bv);
    [](BytesView) {}(b);
}

static constexpr auto constexpr_test_write(Bytes b, BytesView answer)
{
    CALICO_EXPECT_EQ(b.size(), answer.size());
    for (Size i {}; i < b.size(); ++i)
        b[i] = answer[i];

    // TODO: I have no clue why this works. std::memcmp() isn't constexpr, but starts_with(), which uses it, is...
    (void)b.starts_with(answer);
    (void)b.data();
    (void)b.range(0, 0);
    (void)b.is_empty();
    b.advance(0);
    b.truncate(b.size());
}

static constexpr auto constexpr_test_read(BytesView bv, BytesView answer)
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
    static constexpr BytesView bv {"42"};
    constexpr_test_read(bv, "42");

    char a[] {"42"};
    Bytes b {a};
    constexpr_test_write(b, "ab");
    constexpr_test_read(b, "ab");
}

TEST_F(SliceTests, SubRangesHaveProperType)
{
    BytesView bv1 {"42"};
    auto bv2 = bv1.range(0);
    // NOTE: Extra parenthesis seem to be necessary. ASSERT_*() and EXPECT_*() don't like angle brackets.
    ASSERT_TRUE((std::is_same_v<BytesView, decltype(bv2)>));

    auto s = bv1.to_string();
    Bytes b1 {s};
    auto b2 = b1.range(0);
    ASSERT_TRUE((std::is_same_v<Bytes, decltype(b2)>));
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
    Bytes bytes {backing};
    Scratch scratch {bytes};
    scratch->advance(1);
    scratch->truncate(1);
    ASSERT_TRUE(*scratch == "b");
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
    ASSERT_TRUE(scratch->starts_with("Hello"));
    ASSERT_TRUE(scratch->range(7, 5) == stob("world"));
    ASSERT_TRUE(scratch->advance(7).truncate(5) == stob("world"));
}

TEST(NonPrintableSliceTests, UsesStringSize)
{
    const std::string u {"\x00\x01", 2};
    ASSERT_EQ(BytesView {u}.size(), 2);
}

TEST(NonPrintableSliceTests, NullBytesAreEqual)
{
    const std::string u {"\x00", 1};
    const std::string v {"\x00", 1};
    ASSERT_EQ(compare_three_way(BytesView {u}, BytesView {v}), ThreeWayComparison::EQ);
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
    // We need to pass in the size, since the first character is '\0'. Otherwise, the length will be 0.
    std::string u {"\x00\x01", 2};
    const auto s = stob(u).to_string();
    ASSERT_EQ(s.size(), 2);
    ASSERT_EQ(s[0], '\x00');
    ASSERT_EQ(s[1], '\x01');
}

TEST(NonPrintableSliceTests, CStyleStringLengths)
{
    const auto a = "ab";
    const char b[] {'4', '2', '\x00'};
    ASSERT_EQ(BytesView {a}.size(), 2);
    ASSERT_EQ(BytesView {b}.size(), 2);
}

TEST(NonPrintableSliceTests, ModifyCharArray)
{
    char data[] {'a', 'b', '\x00'};
    Bytes bytes {data};
    bytes[0] = '4';
    bytes.advance();
    bytes[0] = '2';
    ASSERT_TRUE(stob(data) == stob("42"));
}

TEST(NonPrintableSliceTests, NullByteInMiddleOfLiteralGivesIncorrectLength)
{
    const auto a = "\x12\x00\x34";
    const char b[] {'4', '\x00', '2', '\x00'};

    ASSERT_EQ(std::char_traits<char>::length(a), 1);
    ASSERT_EQ(std::char_traits<char>::length(b), 1);
    ASSERT_EQ(stob(a).size(), 1);
    ASSERT_EQ(stob(b).size(), 1);
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

TEST(SimpleDSLTests, TypesAreSizedCorrectly)
{
    Id id {};
    static_assert(sizeof(Id) == sizeof(id.value));
    static_assert(sizeof(Id) == sizeof(id.value));
}

TEST(SimpleDSLTests, IdentifiersAreNullable)
{
    run_nullability_check<Id>();
    ASSERT_FALSE(Id::root().is_null());
    ASSERT_TRUE(Id::root().is_root());
}

TEST(SimpleDSLTests, IdentifiersAreEqualityComparable)
{
    run_equality_comparisons<Id>();
}

TEST(SimpleDSLTests, IdentifiersAreOrderable)
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

TEST(CellSizeTests, AtLeastFourCellsCanFitInAnInternalNonRootNode)
{
    const auto start = NodeLayout::header_offset(Id {2}) +
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
    auto s = ok();
    ASSERT_TRUE(s.what().empty());
}

TEST(StatusTests, NonOkStatusSavesMessage)
{
    static constexpr auto message = "status message";
    auto s = invalid_argument(message);
    ASSERT_EQ(s.what(), message);
    ASSERT_TRUE(s.is_invalid_argument());
}

TEST(StatusTests, StatusCanBeCopied)
{
    auto s = invalid_argument("invalid argument");
    auto t = s;
    ASSERT_TRUE(t.is_invalid_argument());
    ASSERT_EQ(t.what(), "invalid argument");

    t = ok();
    ASSERT_TRUE(s.is_invalid_argument());
    ASSERT_EQ(s.what(), "invalid argument");
}

TEST(StatusTests, StatusCanBeReassigned)
{
    auto s = ok();
    ASSERT_TRUE(s.is_ok());

    s = invalid_argument("invalid argument");
    ASSERT_TRUE(s.is_invalid_argument());
    ASSERT_EQ(s.what(), "invalid argument");

    s = logic_error("logic error");
    ASSERT_TRUE(s.is_logic_error());
    ASSERT_EQ(s.what(), "logic error");

    s = ok();
    ASSERT_TRUE(s.is_ok());
}

TEST(StatusTests, StatusCodesAreCorrect)
{
    ASSERT_TRUE(invalid_argument("invalid argument").is_invalid_argument());
    ASSERT_TRUE(system_error("system error").is_system_error());
    ASSERT_TRUE(logic_error("logic error").is_logic_error());
    ASSERT_TRUE(corruption("corruption").is_corruption());
    ASSERT_TRUE(not_found("not found").is_not_found());
    ASSERT_TRUE(ok().is_ok());
}

TEST(StatusTests, OkStatusCanBeCopied)
{
    auto src = ok();
    auto dst = src;
    ASSERT_TRUE(src.is_ok());
    ASSERT_TRUE(dst.is_ok());
    ASSERT_TRUE(src.what().empty());
    ASSERT_TRUE(dst.what().empty());
}

TEST(StatusTests, NonOkStatusCanBeCopied)
{
    auto src = invalid_argument("status message");
    auto dst = src;
    ASSERT_TRUE(src.is_invalid_argument());
    ASSERT_TRUE(dst.is_invalid_argument());
    ASSERT_EQ(src.what(), "status message");
    ASSERT_EQ(dst.what(), "status message");
}

TEST(StatusTests, OkStatusCanBeMoved)
{
    auto src = ok();
    auto dst = std::move(src);
    ASSERT_TRUE(src.is_ok());
    ASSERT_TRUE(dst.is_ok());
    ASSERT_TRUE(src.what().empty());
    ASSERT_TRUE(dst.what().empty());
}

TEST(StatusTests, NonOkStatusCanBeMoved)
{
    auto src = invalid_argument("status message");
    auto dst = std::move(src);
    ASSERT_TRUE(src.is_ok());
    ASSERT_TRUE(dst.is_invalid_argument());
    ASSERT_TRUE(src.what().empty());
    ASSERT_EQ(dst.what(), "status message");
}

TEST(StatusTests, FmtPrint)
{
    auto s = system_error("{1}::{0}", 123, 42);
    ASSERT_EQ(s.what(), "42::123");
}

// Modified from RocksDB.
class QueueTests: public testing::Test {
public:
    static constexpr Size NUM_ELEMENTS {500};
    static constexpr Size CAPACITY {16};

    QueueTests() = default;
    ~QueueTests() override = default;

    std::array<Size, NUM_ELEMENTS> data {};
    mutable std::mutex mutex;
    Queue<Size> queue {CAPACITY};
};

struct Consumer {
    auto operator()() const -> void
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

TEST(HeaderTests, EncodeAndDecodePageSize)
{
    ASSERT_EQ(decode_page_size(0), 1 << 16);
    ASSERT_EQ(encode_page_size(1 << 16), 0);

    for (Size i {1}; i < 16; ++i) {
        const auto size = 1ULL << i;
        ASSERT_EQ(decode_page_size(encode_page_size(size)), size);
    }
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

/*
 * The Worker<Event> class provides a background thread that waits on Events from a Queue<Event>. We can dispatch an event from the
 * main thread and either wait or return immediately. It also should provide fast access to its internal Status object.
 */
class BasicWorkerTests: public testing::Test {
public:
    BasicWorkerTests()
        : worker {16, [this](int event) {
            events.emplace_back(event);
            return ok();
        }}
    {}

    Worker<int> worker;
    std::vector<int> events;
};

TEST_F(BasicWorkerTests, CreateWorker)
{
    ASSERT_OK(worker.status());
    ASSERT_TRUE(events.empty());
    ASSERT_OK(std::move(worker).destroy());
}

TEST_F(BasicWorkerTests, DestroyWorker)
{
    ASSERT_OK(std::move(worker).destroy());
    ASSERT_TRUE(events.empty());
}

TEST_F(BasicWorkerTests, EventsGetAdded)
{
    worker.dispatch(1);
    worker.dispatch(2);
    worker.dispatch(3);

    // Blocks until all events are finished and the worker thread is joined.
    ASSERT_OK(std::move(worker).destroy());
    ASSERT_EQ(events.at(0), 1);
    ASSERT_EQ(events.at(1), 2);
    ASSERT_EQ(events.at(2), 3);
}

TEST_F(BasicWorkerTests, WaitOnEvent)
{
    worker.dispatch(1);
    worker.dispatch(2);
    // Let the event get processed before returning.
    worker.dispatch(3, true);

    ASSERT_EQ(events.at(0), 1);
    ASSERT_EQ(events.at(1), 2);
    ASSERT_EQ(events.at(2), 3);
    ASSERT_OK(std::move(worker).destroy());
}

TEST_F(BasicWorkerTests, SanityCheck)
{
    static constexpr int NUM_EVENTS {1'000};
    for (int i {}; i < NUM_EVENTS; ++i) {
        worker.dispatch(i, i == NUM_EVENTS - 1);
        ASSERT_OK(worker.status());
    }

    for (int i {}; i < NUM_EVENTS; ++i)
        ASSERT_EQ(events.at(static_cast<Size>(i)), i);

    ASSERT_OK(worker.status());
    ASSERT_OK(std::move(worker).destroy());
}

class WorkerFaultTests: public testing::Test {
public:
    WorkerFaultTests()
        : worker {16, [this](int event) {
            if (callback_status.is_ok())
                events.emplace_back(event);
            return callback_status;
        }}
    {}

    Status callback_status {ok()};
    Worker<int> worker;
    std::vector<int> events;
};

TEST_F(WorkerFaultTests, ErrorIsSavedAndPropagated)
{
    callback_status = system_error("42");
    worker.dispatch(1, true);
    assert_error_42(worker.status());
    assert_error_42(std::move(worker).destroy());
    ASSERT_TRUE(events.empty());
}

TEST_F(WorkerFaultTests, WorkerCannotBeRecovered)
{
    callback_status = system_error("42");
    worker.dispatch(1, true);

    // Return an OK status after failing once. Worker should remain invalidated. If we need to start the worker again,
    // we need to create a new one.
    callback_status = ok();
    worker.dispatch(2, true);
    assert_error_42(worker.status());
    assert_error_42(std::move(worker).destroy());
    ASSERT_TRUE(events.empty());
}

TEST_F(WorkerFaultTests, StopsProcessingEventsAfterError)
{
    worker.dispatch(1);
    worker.dispatch(2);
    worker.dispatch(3, true);

    callback_status = system_error("42");
    worker.dispatch(4);
    worker.dispatch(5);
    worker.dispatch(6, true);

    ASSERT_EQ(events.at(0), 1);
    ASSERT_EQ(events.at(1), 2);
    ASSERT_EQ(events.at(2), 3);
    ASSERT_EQ(events.size(), 3);

    assert_error_42(worker.status());
    assert_error_42(std::move(worker).destroy());
}

TEST_F(WorkerFaultTests, ErrorStatusContention)
{
    callback_status = system_error("42");
    worker.dispatch(1);
    worker.dispatch(2);
    worker.dispatch(3);

    // Sketchy but seems to work in practice. I'm getting a lot of these checks in before the status is
    // registered.
    Size num_hits {};
    while (worker.status().is_ok())
        num_hits++;
    ASSERT_GT(num_hits, 0);

    assert_error_42(worker.status());
    assert_error_42(std::move(worker).destroy());
    ASSERT_TRUE(events.empty());
}

} // namespace Calico
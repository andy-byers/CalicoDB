
#include "frames.h"
#include "header.h"
#include "logging.h"
#include "node.h"
#include "page.h"
#include "page_cache.h"
#include "pager.h"
#include "unit_tests.h"
#include <gtest/gtest.h>
#include <numeric>

namespace calicodb
{

class DeltaCompressionTest : public testing::Test
{
public:
    static constexpr std::size_t kPageSize {0x200};

    [[nodiscard]] auto build_deltas(const ChangeBuffer &unordered) const
    {
        ChangeBuffer deltas;
        for (const auto &delta : unordered)
            insert_delta(deltas, delta);
        compress_deltas(deltas);
        return deltas;
    }

    [[nodiscard]] auto insert_random_delta(ChangeBuffer &deltas) const
    {
        static constexpr std::size_t MIN_DELTA_SIZE {1};
        const auto offset = random.Next<std::size_t>(kPageSize - MIN_DELTA_SIZE);
        const auto size = random.Next<std::size_t>(kPageSize - offset);
        insert_delta(deltas, {offset, size});
    }

    tools::RandomGenerator random;
};

TEST_F(DeltaCompressionTest, CompressingNothingDoesNothing)
{
    const auto empty = build_deltas({});
    ASSERT_TRUE(empty.empty());
}

TEST_F(DeltaCompressionTest, InsertingEmptyDeltaDoesNothing)
{
    ChangeBuffer deltas;
    insert_delta(deltas, {123, 0});
    ASSERT_TRUE(deltas.empty());
}

TEST_F(DeltaCompressionTest, CompressingSingleDeltaDoesNothing)
{
    const auto single = build_deltas({{123, 1}});
    ASSERT_EQ(single.size(), 1);
    ASSERT_EQ(single.front().offset, 123);
    ASSERT_EQ(single.front().size, 1);
}

TEST_F(DeltaCompressionTest, DeltasAreOrdered)
{
    const auto deltas = build_deltas({
        {20, 2},
        {60, 6},
        {50, 5},
        {10, 1},
        {90, 9},
        {70, 7},
        {40, 4},
        {80, 8},
        {30, 3},
    });

    std::size_t i {1};
    ASSERT_TRUE(std::all_of(cbegin(deltas), cend(deltas), [&i](const auto &delta) {
        const auto j = std::exchange(i, i + 1);
        return delta.offset == 10 * j && delta.size == j;
    }));
    ASSERT_EQ(deltas.size(), 9);
}

TEST_F(DeltaCompressionTest, DeltasAreNotRepeated)
{
    const auto deltas = build_deltas({
        {20, 2},
        {50, 5},
        {40, 4},
        {10, 1},
        {20, 2},
        {30, 3},
        {50, 5},
        {40, 4},
        {30, 3},
        {10, 1},
    });

    std::size_t i {1};
    ASSERT_TRUE(std::all_of(cbegin(deltas), cend(deltas), [&i](const auto &delta) {
        const auto j = std::exchange(i, i + 1);
        return delta.offset == 10 * j && delta.size == j;
    }));
    ASSERT_EQ(deltas.size(), 5);
}

TEST_F(DeltaCompressionTest, OverlappingDeltasAreMerged)
{
    auto deltas = build_deltas({
        {0, 10},
        {20, 10},
        {40, 10},
    });

    insert_delta(deltas, {5, 10});
    insert_delta(deltas, {30, 10});
    compress_deltas(deltas);

    ASSERT_EQ(deltas.size(), 2);
    ASSERT_EQ(deltas[0].size, 15);
    ASSERT_EQ(deltas[0].offset, 0);
    ASSERT_EQ(deltas[1].size, 30);
    ASSERT_EQ(deltas[1].offset, 20);
}

TEST_F(DeltaCompressionTest, SanityCheck)
{
    static constexpr std::size_t NUM_INSERTS {100};
    static constexpr std::size_t MAX_DELTA_SIZE {10};
    ChangeBuffer deltas;
    for (std::size_t i {}; i < NUM_INSERTS; ++i) {
        const auto offset = random.Next<std::size_t>(kPageSize - MAX_DELTA_SIZE);
        const auto size = random.Next<std::size_t>(1, MAX_DELTA_SIZE);
        insert_delta(deltas, PageDelta {offset, size});
    }
    compress_deltas(deltas);

    std::vector<int> covering(kPageSize);
    for (const auto &[offset, size] : deltas) {
        for (std::size_t i {}; i < size; ++i) {
            ASSERT_EQ(covering.at(offset + i), 0);
            covering[offset + i]++;
        }
    }
}

class CacheTests : public testing::Test
{
public:
    Cache<int, int> target;
};

TEST_F(CacheTests, EmptyCacheBehavior)
{
    calicodb::Cache<int, int> cache;

    ASSERT_TRUE(cache.is_empty());
    ASSERT_EQ(cache.size(), 0);
    ASSERT_EQ(begin(cache), end(cache));
    ASSERT_EQ(cache.get(1), end(cache));
    ASSERT_EQ(cache.evict(), std::nullopt);
}

TEST_F(CacheTests, NonEmptyCacheBehavior)
{
    calicodb::Cache<int, int> cache;

    cache.put(1, 1);
    ASSERT_FALSE(cache.is_empty());
    ASSERT_EQ(cache.size(), 1);
    ASSERT_NE(begin(cache), end(cache));
    ASSERT_NE(cache.get(1), end(cache));
    ASSERT_NE(cache.evict(), std::nullopt);
}

TEST_F(CacheTests, ElementsArePromotedAfterUse)
{
    calicodb::Cache<int, int> cache;

    // 1*, 2, 3, 4, END
    cache.put(4, 4);
    cache.put(3, 3);
    cache.put(2, 2);
    cache.put(1, 1);

    // 3, 4, 1*, 2, END
    cache.put(4, 4);
    cache.put(4, 4);
    ASSERT_EQ(cache.get(3)->value, 3);
    ASSERT_EQ(cache.size(), 4);

    decltype(cache)::Entry entry;
    entry = cache.evict().value();
    ASSERT_FALSE(entry.hot);
    ASSERT_EQ(entry.value, 2);
    entry = cache.evict().value();
    ASSERT_FALSE(entry.hot);
    ASSERT_EQ(entry.value, 1);
    entry = cache.evict().value();
    ASSERT_TRUE(entry.hot);
    ASSERT_EQ(entry.value, 4);
    entry = cache.evict().value();
    ASSERT_TRUE(entry.hot);
    ASSERT_EQ(entry.value, 3);
}

TEST_F(CacheTests, IterationRespectsReplacementPolicy)
{
    // 1*, 2, 3, END
    target.put(3, 3);
    target.put(2, 2);
    target.put(1, 1);

    // 1, 2, 3*, END
    target.put(2, 2);
    target.put(1, 1);

    // Hottest -> coldest
    auto itr = begin(target);
    ASSERT_TRUE(itr->hot);
    ASSERT_EQ(itr++->value, 1);
    ASSERT_TRUE(itr->hot);
    ASSERT_EQ(itr++->value, 2);
    ASSERT_FALSE(itr->hot);
    ASSERT_EQ(itr++->value, 3);
    ASSERT_EQ(itr, end(target));

    // Coldest -> hottest
    auto ritr = rbegin(target);
    ASSERT_FALSE(ritr->hot);
    ASSERT_EQ(ritr++->value, 3);
    ASSERT_TRUE(ritr->hot);
    ASSERT_EQ(ritr++->value, 2);
    ASSERT_TRUE(ritr->hot);
    ASSERT_EQ(ritr++->value, 1);
    ASSERT_EQ(ritr, rend(target));
}

TEST_F(CacheTests, QueryDoesNotPromoteElements)
{
    calicodb::Cache<int, int> cache;

    // 1*, 2, 3, END
    cache.put(3, 3);
    cache.put(2, 2);
    cache.put(1, 1);

    ASSERT_EQ(cache.query(1)->value, 1);
    ASSERT_EQ(cache.query(2)->value, 2);

    // Method is const.
    const auto &ref = cache;
    ASSERT_EQ(ref.query(3)->value, 3);

    auto itr = begin(cache);
    ASSERT_EQ(itr++->value, 1);
    ASSERT_EQ(itr++->value, 2);
    ASSERT_EQ(itr++->value, 3);
    ASSERT_EQ(itr, end(cache));
}

TEST_F(CacheTests, ModifyValue)
{
    calicodb::Cache<int, int> cache;

    cache.put(1, 1);
    cache.put(1, 2);

    ASSERT_EQ(cache.size(), 1);
    ASSERT_EQ(cache.get(1)->value, 2);
}

TEST_F(CacheTests, WarmElementsAreFifoOrdered)
{
    calicodb::Cache<int, int> cache;

    // 1*, 2, 3, END
    cache.put(3, 3);
    cache.put(2, 2);
    cache.put(1, 1);

    auto itr = begin(cache);
    ASSERT_EQ(itr++->value, 1);
    ASSERT_EQ(itr++->value, 2);
    ASSERT_EQ(itr++->value, 3);
    ASSERT_EQ(itr, end(cache));

    ASSERT_EQ(cache.evict()->value, 3);
    ASSERT_EQ(cache.evict()->value, 2);
    ASSERT_EQ(cache.evict()->value, 1);
}

TEST_F(CacheTests, HotElementsAreLruOrdered)
{
    calicodb::Cache<int, int> cache;

    // 1*, 2, 3
    cache.put(3, 3);
    cache.put(2, 2);
    cache.put(1, 1);

    // 2, 3, 1*
    ASSERT_EQ(cache.get(3)->value, 3);
    ASSERT_EQ(cache.get(2)->value, 2);
    ASSERT_EQ(cache.get(1)->value, 1);

    auto itr = begin(cache);
    ASSERT_EQ(itr++->value, 1);
    ASSERT_EQ(itr++->value, 2);
    ASSERT_EQ(itr++->value, 3);
    ASSERT_EQ(itr, end(cache));

    ASSERT_EQ(cache.evict()->value, 3);
    ASSERT_EQ(cache.evict()->value, 2);
    ASSERT_EQ(cache.evict()->value, 1);
}

TEST_F(CacheTests, HotElementsAreEncounteredFirst)
{
    calicodb::Cache<int, int> cache;

    // 4*, 3, 2, 1, END
    cache.put(1, 1);
    cache.put(2, 2);
    cache.put(3, 3);
    cache.put(4, 4);

    // 3, 2, 1, 4*, END
    ASSERT_EQ(cache.get(1)->value, 1);
    ASSERT_EQ(cache.get(2)->value, 2);
    ASSERT_EQ(cache.get(3)->value, 3);

    // 3, 2, 1, 5*, 4, END
    cache.put(5, 5);

    auto itr = begin(cache);
    ASSERT_TRUE(itr->hot);
    ASSERT_EQ(itr++->value, 3);
    ASSERT_TRUE(itr->hot);
    ASSERT_EQ(itr++->value, 2);
    ASSERT_TRUE(itr->hot);
    ASSERT_EQ(itr++->value, 1);
    ASSERT_FALSE(itr->hot);
    ASSERT_EQ(itr++->value, 5);
    ASSERT_FALSE(itr->hot);
    ASSERT_EQ(itr++->value, 4);
    ASSERT_EQ(itr, end(cache));
}

TEST_F(CacheTests, SeparatorIsMovedOnInsert)
{
    calicodb::Cache<int, int> cache;

    // 4*, 3, 2, 1, END
    cache.put(1, 1);
    cache.put(2, 2);
    cache.put(3, 3);
    cache.put(4, 4);
    ASSERT_FALSE(begin(cache)->hot);
    ASSERT_EQ(begin(cache)->value, 4);

    // 4, 3*, 2, 1, END
    cache.put(4, 4);
    ASSERT_TRUE(begin(cache)->hot);
    ASSERT_EQ(begin(cache)->value, 4);

    // 3, 4, 2*, 1, END
    cache.put(3, 3);
    ASSERT_TRUE(begin(cache)->hot);
    ASSERT_EQ(begin(cache)->value, 3);

    // 2, 3, 4, 1*, END
    cache.put(2, 2);
    ASSERT_TRUE(begin(cache)->hot);
    ASSERT_EQ(begin(cache)->value, 2);

    // 1, 2, 3, 4, END*
    cache.put(1, 1);
    ASSERT_TRUE(begin(cache)->hot);
    ASSERT_EQ(begin(cache)->value, 1);
}

TEST_F(CacheTests, AddWarmElements)
{
    calicodb::Cache<int, int> cache;

    // 4*, 3, 2, 1, END
    cache.put(1, 1);
    cache.put(2, 2);
    cache.put(3, 3);
    cache.put(4, 4);
    ASSERT_FALSE(begin(cache)->hot);
    ASSERT_EQ(begin(cache)->value, 4);

    // 3, 4, 2*, 1, END
    cache.put(4, 4);
    cache.put(3, 3);

    // 3, 4, 6*, 5, 2, 1, END
    cache.put(5, 5);
    cache.put(6, 6);

    auto itr = begin(cache);
    ASSERT_TRUE(itr->hot);
    ASSERT_EQ(itr++->value, 3);
    ASSERT_TRUE(itr->hot);
    ASSERT_EQ(itr++->value, 4);
    ASSERT_FALSE(itr->hot);
    ASSERT_EQ(itr++->value, 6);
    ASSERT_FALSE(itr->hot);
    ASSERT_EQ(itr++->value, 5);
    ASSERT_FALSE(itr->hot);
    ASSERT_EQ(itr++->value, 2);
    ASSERT_FALSE(itr->hot);
    ASSERT_EQ(itr++->value, 1);
    ASSERT_EQ(itr, end(cache));
}

TEST_F(CacheTests, InsertAfterWarmElementsDepleted)
{
    calicodb::Cache<int, int> cache;

    // 4*, 3, 2, 1, END
    cache.put(1, 1);
    cache.put(2, 2);
    cache.put(3, 3);
    cache.put(4, 4);
    ASSERT_FALSE(begin(cache)->hot);
    ASSERT_EQ(begin(cache)->value, 4);

    // 3, 4, 2*, 1, END
    cache.put(4, 4);
    cache.put(3, 3);

    // 3, 4, 2*, END
    auto entry = cache.evict().value();
    ASSERT_FALSE(entry.hot);
    ASSERT_EQ(entry.value, 1);

    // 3, 4, END*
    entry = cache.evict().value();
    ASSERT_FALSE(entry.hot);
    ASSERT_EQ(entry.value, 2);

    // 4, 3, END*
    cache.put(4, 4);
    ASSERT_TRUE(prev(end(cache))->hot);
    ASSERT_EQ(prev(end(cache))->value, 3);
    ASSERT_TRUE(begin(cache)->hot);
    ASSERT_EQ(begin(cache)->value, 4);

    // 4, 3, 2*, END
    cache.put(2, 2);
    ASSERT_FALSE(prev(end(cache))->hot);
    ASSERT_EQ(prev(end(cache))->value, 2);
}

static auto check_cache_order(int hot_count, int warm_count)
{
    Cache<int, int> c;

    for (int i {1}; i <= hot_count + warm_count; ++i)
        c.put(i, i);
    for (int i {1}; i <= hot_count; ++i)
        c.put(i, i);

    // Iteration: Hot elements should be encountered first. In particular, the most-recently-
    // used hot element (if present) should be first.
    auto itr = begin(c);
    ASSERT_EQ(itr->value, hot_count ? hot_count : warm_count);
    for (int i {}; i < hot_count; ++i) {
        ASSERT_TRUE(itr++->hot);
    }
    for (int i {}; i < warm_count; ++i) {
        ASSERT_FALSE(itr++->hot);
    }

    // Eviction: Hot elements should be evicted last.
    for (int i {}; i < warm_count; ++i) {
        ASSERT_FALSE(c.evict()->hot);
    }
    for (int i {}; i < hot_count; ++i) {
        ASSERT_TRUE(c.evict()->hot);
    }
}

TEST(CacheOrderTests, CheckOrder)
{
    check_cache_order(1, 0);
    check_cache_order(0, 1);
    check_cache_order(2, 0);
    check_cache_order(0, 2);
    check_cache_order(2, 1);
    check_cache_order(1, 2);
    check_cache_order(1, 1);
    check_cache_order(2, 2);
}

TEST(MoveOnlyCacheTests, WorksWithMoveOnlyValue)
{
    calicodb::Cache<int, std::unique_ptr<int>> cache;
    cache.put(1, std::make_unique<int>(1));
    ASSERT_EQ(*cache.get(1)->value, 1);
    ASSERT_EQ(*cache.evict()->value, 1);
}

class PageRegistryTests : public testing::Test
{
public:
    ~PageRegistryTests() override = default;

    PageCache registry;
};

TEST_F(PageRegistryTests, HotEntriesAreFoundLast)
{
    registry.put(Id {11UL}, {std::size_t {11UL}});
    registry.put(Id {12UL}, {std::size_t {12UL}});
    registry.put(Id {13UL}, {std::size_t {13UL}});
    registry.put(Id {1UL}, {std::size_t {1UL}});
    registry.put(Id {2UL}, {std::size_t {2UL}});
    registry.put(Id {3UL}, {std::size_t {3UL}});
    ASSERT_EQ(registry.size(), 6);

    ASSERT_EQ(registry.get(Id {11UL})->value.index, 11UL);
    ASSERT_EQ(registry.get(Id {12UL})->value.index, 12UL);
    ASSERT_EQ(registry.get(Id {13UL})->value.index, 13UL);

    std::size_t i {}, j {};

    const auto callback = [&i, &j](auto page_id, auto entry) {
        EXPECT_EQ(page_id.value, entry.index);
        EXPECT_EQ(page_id.value, i + (j >= 3) * 10 + 1) << "The cache entries should have been visited in order {1, 2, 3, 11, 12, 13}";
        ++j;
        i = j % 3;
        return false;
    };

    ASSERT_FALSE(registry.evict(callback));
}

class FramerTests
    : public InMemoryTest,
      public testing::Test
{
public:
    explicit FramerTests()
    {
        Editor *file;
        EXPECT_OK(env->new_editor("./test", &file));

        AlignedBuffer buffer {0x200 * 8, 0x200};

        frames = std::make_unique<FrameManager>(*file, std::move(buffer), 0x200, 8);
    }

    ~FramerTests() override = default;

    std::unique_ptr<FrameManager> frames;
};

TEST_F(FramerTests, NewFramerIsSetUpCorrectly)
{
    ASSERT_EQ(frames->available(), 8);
    ASSERT_EQ(frames->page_count(), 0);
}

TEST_F(FramerTests, PinFailsWhenNoFramesAreAvailable)
{
    std::size_t fid;
    for (std::size_t i {1}; i <= 8; i++) {
        ASSERT_OK(frames->pin(Id {i}, fid));
    }
    ASSERT_TRUE(frames->pin(Id {9UL}, fid).is_not_found());

    frames->unpin(std::size_t {1UL});
    ASSERT_OK(frames->pin(Id {9UL}, fid));
}

auto write_to_page(Page &page, const std::string &message) -> void
{
    const auto offset = page_offset(page) + sizeof(Lsn);
    EXPECT_LE(offset + message.size(), page.size());
    mem_copy(page.span(offset, message.size()), message);
}

[[nodiscard]] auto read_from_page(const Page &page, std::size_t size) -> std::string
{
    const auto offset = page_offset(page) + sizeof(Lsn);
    EXPECT_LE(offset + size, page.size());
    auto message = std::string(size, '\x00');
    mem_copy(message, page.view(offset, message.size()));
    return message;
}

class PagerTests
    : public TestWithPager,
      public testing::Test
{
public:
    std::string test_message {"Hello, world!"};

    ~PagerTests() override = default;

    [[nodiscard]] auto allocate_write(const std::string &message) const
    {
        Page page;
        EXPECT_OK(pager->allocate(page));
        write_to_page(page, message);
        return page;
    }

    [[nodiscard]] auto allocate_write_release(const std::string &message) const
    {
        auto page = allocate_write(message);
        const auto id = page.id();
        pager->release(std::move(page));
        EXPECT_OK(status);
        return id;
    }

    [[nodiscard]] auto acquire_write(Id id, const std::string &message) const
    {
        Page page;
        EXPECT_OK(pager->acquire(id, page));
        pager->upgrade(page);
        write_to_page(page, message);
        return page;
    }

    auto acquire_write_release(Id id, const std::string &message) const
    {
        auto page = acquire_write(id, message);
        pager->release(std::move(page));
        EXPECT_OK(status);
    }

    [[nodiscard]] auto acquire_read_release(Id id, std::size_t size) const
    {
        Page page;
        EXPECT_OK(pager->acquire(id, page));
        auto message = read_from_page(page, size);
        pager->release(std::move(page));
        EXPECT_OK(status);
        return message;
    }
};

TEST_F(PagerTests, NewPagerIsSetUpCorrectly)
{
    ASSERT_EQ(pager->page_count(), 0);
    ASSERT_EQ(pager->bytes_written(), 0);
    ASSERT_EQ(pager->recovery_lsn(), Id::null());
    EXPECT_OK(status);
}

TEST_F(PagerTests, AllocationInceasesPageCount)
{
    [[maybe_unused]] const auto a = allocate_write_release("a");
    ASSERT_EQ(pager->page_count(), 1);
    [[maybe_unused]] const auto b = allocate_write_release("b");
    ASSERT_EQ(pager->page_count(), 2);
    [[maybe_unused]] const auto c = allocate_write_release("c");
    ASSERT_EQ(pager->page_count(), 3);
}

TEST_F(PagerTests, FirstAllocationCreatesRootPage)
{
    auto id = allocate_write_release(test_message);
    ASSERT_EQ(id, Id::root());
}

TEST_F(PagerTests, AcquireReturnsCorrectPage)
{
    const auto id = allocate_write_release(test_message);
    Page page;
    ASSERT_OK(pager->acquire(id, page));
    ASSERT_EQ(id, page.id());
    ASSERT_EQ(id, Id::root());
    pager->release(std::move(page));
}

TEST_F(PagerTests, MultipleReaders)
{
    const auto id = allocate_write_release(test_message);
    Page page_a, page_b;
    ASSERT_OK(pager->acquire(id, page_a));
    ASSERT_OK(pager->acquire(id, page_b));
    pager->release(std::move(page_a));
    pager->release(std::move(page_b));
}

template <class T>
static auto run_root_persistence_test(T &test, std::size_t n)
{
    const auto id = test.allocate_write_release(test.test_message);

    // Cause the root page to be evicted and written back, along with some other pages.
    while (test.pager->page_count() < n) {
        (void)test.allocate_write_release("...");
    }

    // Read the root page back from the file.
    ASSERT_EQ(test.acquire_read_release(id, test.test_message.size()), test.test_message);
}

TEST_F(PagerTests, RootDataPersistsInFrame)
{
    run_root_persistence_test(*this, kFrameCount);
}

TEST_F(PagerTests, RootDataPersistsInEnv)
{
    run_root_persistence_test(*this, kFrameCount * 2);
}

[[nodiscard]] auto generate_id_strings(std::size_t n)
{
    std::vector<std::size_t> id_ints(n);
    std::iota(begin(id_ints), end(id_ints), 1);

    std::vector<std::string> id_strs;
    std::transform(cbegin(id_ints), cend(id_ints), back_inserter(id_strs), [](auto id) {
        auto result = std::to_string(id);
        CDB_EXPECT_LE(result.size(), 6);
        return std::string(6 - result.size(), '0') + result;
    });
    return id_strs;
}

TEST_F(PagerTests, SanityCheck)
{
    const auto ids = generate_id_strings(500);

    for (const auto &id : ids)
        [[maybe_unused]] const auto unused = allocate_write_release(id);

    for (const auto &id : ids) { // NOTE: gtest assertion macros sometimes complain if braces are omitted.
        ASSERT_EQ(id, acquire_read_release(Id {std::stoull(id)}, id.size()));
    }
}

} // namespace calicodb
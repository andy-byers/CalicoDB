// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "frames.h"
#include "header.h"
#include "logging.h"
#include "page.h"
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

    [[nodiscard]] auto build_deltas(const std::vector<PageDelta> &unordered) const
    {
        std::vector<PageDelta> deltas;
        for (const auto &delta : unordered) {
            insert_delta(deltas, delta);
        }
        compress_deltas(deltas);
        return deltas;
    }

    [[nodiscard]] auto insert_random_delta(std::vector<PageDelta> &deltas) const
    {
        static constexpr std::size_t MIN_DELTA_SIZE {1};
        const auto offset = random.Next(kPageSize - MIN_DELTA_SIZE);
        const auto size = random.Next(kPageSize - offset);
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
    std::vector<PageDelta> deltas;
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
        {10, 1},
        {30, 3},
    });

    ASSERT_EQ(deltas.size(), 3);
    for (std::size_t i {}; i < deltas.size(); ++i) {
        ASSERT_EQ(deltas[i].offset, (i + 1) * 10);
        ASSERT_EQ(deltas[i].size, i + 1);
    }
}

TEST_F(DeltaCompressionTest, DeltasAreNotRepeated)
{
    const auto deltas = build_deltas({
        {20, 2},
        {10, 1},
        {20, 2},
        {10, 1},
    });

    ASSERT_EQ(deltas.size(), 2);
    for (std::size_t i {}; i < deltas.size(); ++i) {
        ASSERT_EQ(deltas[i].offset, (i + 1) * 10);
        ASSERT_EQ(deltas[i].size, i + 1);
    }
}

TEST_F(DeltaCompressionTest, ConnectedDeltasAreMerged)
{
    const auto deltas = build_deltas({
        {0, 1},
        {1, 2},
        {3, 1},
    });

    ASSERT_EQ(deltas.size(), 1);
    ASSERT_EQ(deltas[0].offset, 0);
    ASSERT_EQ(deltas[0].size, 4);
}

TEST_F(DeltaCompressionTest, OverlappingDeltasAreMerged)
{
    std::vector<PageDelta> deltas {
        {0, 10},
        {20, 10},
        {40, 10},
    };

    // Overlaps the first delta by 5.
    insert_delta(deltas, {5, 10});

    // Joins the second and third original deltas.
    insert_delta(deltas, {30, 10});

    // New last delta.
    insert_delta(deltas, {60, 10});

    // Overlaps the last delta by 5 and joins it to the other group.
    insert_delta(deltas, {50, 15});
    compress_deltas(deltas);

    ASSERT_EQ(deltas.size(), 2);
    ASSERT_EQ(deltas[0].size, 15);
    ASSERT_EQ(deltas[0].offset, 0);
    ASSERT_EQ(deltas[1].size, 50);
    ASSERT_EQ(deltas[1].offset, 20);
}

TEST_F(DeltaCompressionTest, SanityCheck)
{
    static constexpr std::size_t NUM_INSERTS {100};
    static constexpr std::size_t MAX_DELTA_SIZE {10};
    std::vector<PageDelta> deltas;
    for (std::size_t i {}; i < NUM_INSERTS; ++i) {
        const auto offset = random.Next(kPageSize - MAX_DELTA_SIZE);
        const auto size = random.Next(1, MAX_DELTA_SIZE);
        insert_delta(deltas, PageDelta {offset, size});
    }
    compress_deltas(deltas);

    std::vector<int> covering(kPageSize);
    for (const auto &[offset, size] : deltas) {
        for (std::size_t i {}; i < size; ++i) {
            ASSERT_EQ(covering.at(offset + i), 0);
            ++covering[offset + i];
        }
    }
}

[[nodiscard]] static auto make_cache_entry(std::uint64_t id_value) -> CacheEntry
{
    return {.page_id = Id {id_value}};
}

class PageCacheTests : public testing::Test
{
public:
    PageCache cache;
};

TEST_F(PageCacheTests, EmptyCacheBehavior)
{
    ASSERT_EQ(cache.size(), 0);
    ASSERT_EQ(cache.size(), 0);
    ASSERT_EQ(cache.get(Id::root()), nullptr);
    ASSERT_EQ(cache.evict(), std::nullopt);
}

TEST_F(PageCacheTests, OldestEntryIsEvictedFirst)
{
    cache.put(make_cache_entry(4));
    cache.put(make_cache_entry(3));
    cache.put(make_cache_entry(2));
    cache.put(make_cache_entry(1));
    ASSERT_EQ(cache.size(), 4);

    ASSERT_EQ(cache.get(Id {4})->page_id, Id {4});
    ASSERT_EQ(cache.get(Id {3})->page_id, Id {3});

    ASSERT_EQ(cache.evict()->page_id, Id {2});
    ASSERT_EQ(cache.evict()->page_id, Id {1});
    ASSERT_EQ(cache.evict()->page_id, Id {4});
    ASSERT_EQ(cache.evict()->page_id, Id {3});
    ASSERT_EQ(cache.size(), 0);
}

TEST_F(PageCacheTests, ReplacementPolicyIgnoresQuery)
{
    cache.put(make_cache_entry(2));
    cache.put(make_cache_entry(1));

    (void)cache.query(Id {2});

    ASSERT_EQ(cache.evict()->page_id, Id {2});
    ASSERT_EQ(cache.evict()->page_id, Id {1});
}

TEST_F(PageCacheTests, ReferencedEntriesAreIgnoredDuringEviction)
{
    cache.put(make_cache_entry(2));
    cache.put(make_cache_entry(1));

    cache.query(Id {2})->refcount = 1;

    ASSERT_EQ(cache.evict()->page_id, Id {1});
    ASSERT_FALSE(cache.evict().has_value());
}

class FrameManagerTests
    : public InMemoryTest,
      public testing::Test
{
public:
    static constexpr auto kPageSize = kMinPageSize;
    static constexpr auto kFrameCount = kMinFrameCount;

    explicit FrameManagerTests()
    {
        Editor *file;
        EXPECT_OK(env->new_editor("./test", file));

        AlignedBuffer buffer {kPageSize * kFrameCount, kPageSize};
        frames = std::make_unique<FrameManager>(*file, std::move(buffer), kPageSize, kFrameCount);
    }

    ~FrameManagerTests() override = default;

    std::unique_ptr<FrameManager> frames;
    PageCache cache;
};

TEST_F(FrameManagerTests, NewFrameManagerIsSetUpCorrectly)
{
    ASSERT_EQ(frames->available(), kFrameCount);
}

#ifndef NDEBUG
TEST_F(FrameManagerTests, OutOfFramesDeathTest)
{
    for (std::size_t i {}; i < kFrameCount; ++i) {
        auto *entry = cache.put(make_cache_entry(i + 1));
        ASSERT_OK(frames->pin(Id::from_index(i), *entry));
    }
    auto *entry = cache.put(make_cache_entry(kFrameCount + 1));
    ASSERT_EQ(frames->available(), 0);
    ASSERT_DEATH((void)frames->pin(Id::from_index(kFrameCount), *entry), "expect");
}
#endif // NDEBUG

auto write_to_page(Page &page, const std::string &message) -> void
{
    const auto offset = page_offset(page) + sizeof(Lsn);
    EXPECT_LE(offset + message.size(), page.size());
    std::memcpy(page.mutate(offset, message.size()), message.data(), message.size());
}

[[nodiscard]] auto read_from_page(const Page &page, std::size_t size) -> std::string
{
    const auto offset = page_offset(page) + sizeof(Lsn);
    EXPECT_LE(offset + size, page.size());
    auto message = std::string(size, '\x00');
    std::memcpy(message.data(), page.data() + offset, message.size());
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
        EXPECT_OK(state.status);
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
        EXPECT_OK(state.status);
    }

    [[nodiscard]] auto acquire_read_release(Id id, std::size_t size) const
    {
        Page page;
        EXPECT_OK(pager->acquire(id, page));
        auto message = read_from_page(page, size);
        pager->release(std::move(page));
        EXPECT_OK(state.status);
        return message;
    }
};

TEST_F(PagerTests, NewPagerIsSetUpCorrectly)
{
    ASSERT_EQ(pager->page_count(), 0);
    ASSERT_EQ(pager->bytes_written(), 0);
    ASSERT_EQ(pager->recovery_lsn(), Id::null());
    EXPECT_OK(state.status);
}

TEST_F(PagerTests, AllocatesPagesAtEOF)
{
    ASSERT_EQ(pager->page_count(), 0);
    ASSERT_EQ(allocate_write_release("a"), Id {1});
    ASSERT_EQ(pager->page_count(), 1);
    ASSERT_EQ(allocate_write_release("b"), Id {2});
    ASSERT_EQ(pager->page_count(), 2);
    ASSERT_EQ(allocate_write_release("c"), Id {3});
    ASSERT_EQ(pager->page_count(), 3);
}

TEST_F(PagerTests, AcquireReturnsCorrectPage)
{
    const auto incorrect = allocate_write_release(test_message);
    const auto correct = allocate_write_release(test_message);

    Page page;
    ASSERT_OK(pager->acquire(correct, page));
    ASSERT_EQ(correct, page.id());
    ASSERT_NE(incorrect, page.id());
    pager->release(std::move(page));
}

TEST_F(PagerTests, DataPersistsInEnv)
{
    for (std::size_t i {}; i < kFrameCount * 10; ++i) {
        (void)allocate_write_release(tools::integral_key<16>(i));
    }
    for (std::size_t i {}; i < kFrameCount * 10; ++i) {
        ASSERT_EQ(acquire_read_release(Id {i + 1}, 16), tools::integral_key<16>(i))
            << "mismatch on page " << i + 1;
    }
}

class TruncationTests : public PagerTests
{
public:
    static constexpr std::size_t kInitialPageCount {500};

    auto SetUp() -> void override
    {
        for (std::size_t i {}; i < kInitialPageCount; ++i) {
            (void)allocate_write_release(tools::integral_key(i));
        }
        ASSERT_OK(pager->flush());
    }
};

TEST_F(TruncationTests, AllocationAfterTruncation)
{
    ASSERT_OK(pager->truncate(1));

    for (std::size_t i {1}; i < kInitialPageCount; ++i) {
        (void)allocate_write_release(tools::integral_key(i));
    }

    for (std::size_t i {}; i < kInitialPageCount; ++i) {
        const auto key = tools::integral_key(i);
        ASSERT_EQ(acquire_read_release(Id::from_index(i), key.size()), key);
    }
}

TEST_F(TruncationTests, OutOfRangePagesAreDiscarded)
{
    std::size_t base_file_size, file_size;
    const auto flush_and_match_sizes = [&] {
        ASSERT_OK(env->file_size(kFilename, base_file_size));
        // If there are still cached pages past the truncation position, they will be
        // written back to disk here, causing the file size to change.
        ASSERT_OK(pager->flush());
        ASSERT_OK(env->file_size(kFilename, file_size));
        ASSERT_EQ(base_file_size, file_size);
    };

    // Make pages dirty.
    for (std::size_t i {}; i < kInitialPageCount; ++i) {
        acquire_write_release(Id {i + 1}, tools::integral_key(i));
    }
    // Should get rid of cached pages that are out-of-range.
    ASSERT_OK(pager->truncate(kInitialPageCount - kFrameCount / 2));
    flush_and_match_sizes();

    // All cached pages are out-of-range
    for (std::size_t i {}; i < kInitialPageCount - kFrameCount / 2; ++i) {
        acquire_write_release(Id {i + 1}, tools::integral_key(i));
    }
    ASSERT_OK(pager->truncate(1));
    flush_and_match_sizes();
}

} // namespace calicodb
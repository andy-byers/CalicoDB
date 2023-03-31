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

[[nodiscard]] static auto make_cache_entry(U64 id_value) -> CacheEntry
{
    return {.page_id = Id(id_value)};
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
    ASSERT_EQ(cache.next_victim(), nullptr);
}

TEST_F(PageCacheTests, OldestEntryIsEvictedFirst)
{
    cache.put(make_cache_entry(4));
    cache.put(make_cache_entry(3));
    cache.put(make_cache_entry(2));
    cache.put(make_cache_entry(1));
    ASSERT_EQ(cache.size(), 4);

    ASSERT_EQ(cache.get(Id(4))->page_id, Id(4));
    ASSERT_EQ(cache.get(Id(3))->page_id, Id(3));

    ASSERT_EQ(cache.next_victim()->page_id, Id(2));
    cache.erase(cache.next_victim()->page_id);
    ASSERT_EQ(cache.next_victim()->page_id, Id(1));
    cache.erase(cache.next_victim()->page_id);
    ASSERT_EQ(cache.next_victim()->page_id, Id(4));
    cache.erase(cache.next_victim()->page_id);
    ASSERT_EQ(cache.next_victim()->page_id, Id(3));
    cache.erase(cache.next_victim()->page_id);
    ASSERT_EQ(cache.size(), 0);
}

TEST_F(PageCacheTests, ReplacementPolicyIgnoresQuery)
{
    cache.put(make_cache_entry(2));
    cache.put(make_cache_entry(1));

    (void)cache.query(Id(2));

    ASSERT_EQ(cache.next_victim()->page_id, Id(2));
    cache.erase(cache.next_victim()->page_id);
    ASSERT_EQ(cache.next_victim()->page_id, Id(1));
    cache.erase(cache.next_victim()->page_id);
}

TEST_F(PageCacheTests, ReferencedEntriesAreIgnoredDuringEviction)
{
    cache.put(make_cache_entry(2));
    cache.put(make_cache_entry(1));

    cache.query(Id(2))->refcount = 1;

    ASSERT_EQ(cache.next_victim()->page_id, Id(1));
    cache.erase(cache.next_victim()->page_id);
    ASSERT_EQ(cache.next_victim(), nullptr);
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
        File *file;
        EXPECT_OK(env->new_file("./test", file));

        AlignedBuffer buffer {kPageSize * kFrameCount, kPageSize};
        frames = std::make_unique<FrameManager>(std::move(buffer), kPageSize, kFrameCount);
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
    for (std::size_t i = 0; i < kFrameCount; ++i) {
        auto *entry = cache.put(make_cache_entry(i + 1));
        (void)frames->pin(Id::from_index(i), *entry);
    }
    auto *entry = cache.put(make_cache_entry(kFrameCount + 1));
    ASSERT_EQ(frames->available(), 0);
    ASSERT_DEATH((void)frames->pin(Id::from_index(kFrameCount), *entry), "expect");
}
#endif // NDEBUG

auto write_to_page(Page &page, const std::string &message) -> void
{
    EXPECT_LE(page_offset(page) + message.size(), page.size());
    std::memcpy(page.data() + page.size() - message.size(), message.data(), message.size());
}

[[nodiscard]] auto read_from_page(const Page &page, std::size_t size) -> std::string
{
    EXPECT_LE(page_offset(page) + size, page.size());
    std::string message(size, '\x00');
    std::memcpy(message.data(), page.data() + page.size() - message.size(), message.size());
    return message;
}

class PagerTests
    : public TestWithPager,
      public testing::Test
{
public:
    std::string test_message = "Hello, world!";

    ~PagerTests() override = default;

    auto SetUp() -> void override
    {
        state.use_wal = true;
    }

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

    [[nodiscard]] auto read_from_file(Id id, std::size_t size) const
    {
        File *file;
        std::string message(size, '\x00');
        EXPECT_OK(env->new_file(kFilename, file));
        EXPECT_OK(file->read_exact(
            id.value * kPageSize - message.size(),
            message.size(),
            message.data()));
        delete file;
        return message;
    }
};

TEST_F(PagerTests, NewPagerIsSetUpCorrectly)
{
    ASSERT_EQ(pager->page_count(), 0);
    ASSERT_EQ(pager->bytes_written(), 0);
    EXPECT_OK(state.status);
}

TEST_F(PagerTests, AllocatesPagesAtEOF)
{
    ASSERT_EQ(pager->page_count(), 0);
    ASSERT_EQ(allocate_write_release("a"), Id(1));
    ASSERT_EQ(pager->page_count(), 1);
    ASSERT_EQ(allocate_write_release("b"), Id(2));
    ASSERT_EQ(pager->page_count(), 2);
    ASSERT_EQ(allocate_write_release("c"), Id(3));
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
    for (std::size_t i = 0; i < kFrameCount * 10; ++i) {
        (void)allocate_write_release(tools::integral_key<16>(i));
    }
    for (std::size_t i = 0; i < kFrameCount * 10; ++i) {
        ASSERT_EQ(acquire_read_release(Id(i + 1), 16), tools::integral_key<16>(i))
            << "mismatch on page " << i + 1;
    }
}

template <class Test>
static auto write_pages(Test &test, std::size_t key_offset, std::size_t num_pages)
{
    for (std::size_t i = 1; i <= num_pages; ++i) {
        const auto message = tools::integral_key<16>(i + key_offset);
        if (i > test.pager->page_count()) {
            (void)test.allocate_write_release(message);
        } else {
            test.acquire_write_release(Id(i), message);
        }
    }
}

template <class Test>
static auto read_and_check(Test &test, std::size_t key_offset, std::size_t num_pages, bool from_file = false)
{
    for (std::size_t i = 1; i <= num_pages; ++i) {
        const auto message = tools::integral_key<16>(i + key_offset);
        if (from_file) {
            ASSERT_EQ(test.read_from_file(Id(i), 16), message)
                << "mismatch on page (from file) " << i;
        } else {
            ASSERT_EQ(test.acquire_read_release(Id(i), 16), message)
                << "mismatch on page (from pager) " << i;
        }
    }
}

TEST_F(PagerTests, BasicIO)
{
    for (std::size_t i = 0; i < 10; ++i) {
        write_pages(*this, kFrameCount * i, kFrameCount * (i + 1));
        read_and_check(*this, kFrameCount * i, kFrameCount * (i + 1));
    }
}

TEST_F(PagerTests, BasicCommits)
{
    for (std::size_t i = 0; i < 10; ++i) {
        write_pages(*this, kFrameCount * i, kFrameCount * (i + 1));
        ASSERT_OK(pager->commit());
        read_and_check(*this, kFrameCount * i, kFrameCount * (i + 1));
    }
}

TEST_F(PagerTests, BasicCheckpoints)
{
    for (std::size_t i = 0; i < 10; ++i) {
        write_pages(*this, kFrameCount * i, kFrameCount * (i + 1));
        ASSERT_OK(pager->commit());
        read_and_check(*this, kFrameCount * i, kFrameCount * (i + 1));
        ASSERT_OK(pager->checkpoint());
        // Pages returned by the pager should reflect what is on disk.
        read_and_check(*this, kFrameCount * i, kFrameCount * (i + 1));
        read_and_check(*this, kFrameCount * i, kFrameCount * (i + 1), true);
    }
}

TEST_F(PagerTests, WritesBackDuringCheckpoint)
{
}

class TruncationTests : public PagerTests
{
public:
    static constexpr std::size_t kInitialPageCount = 500;

    auto SetUp() -> void override
    {
        for (std::size_t i = 0; i < kInitialPageCount; ++i) {
            (void)allocate_write_release(tools::integral_key(i));
        }
        ASSERT_OK(pager->flush_to_disk());
    }
};

TEST_F(TruncationTests, AllocationAfterTruncation)
{
    ASSERT_OK(pager->truncate(1));

    for (std::size_t i = 1; i < kInitialPageCount; ++i) {
        (void)allocate_write_release(tools::integral_key(i));
    }

    for (std::size_t i = 0; i < kInitialPageCount; ++i) {
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
        ASSERT_OK(pager->flush_to_disk());
        ASSERT_OK(env->file_size(kFilename, file_size));
        ASSERT_EQ(base_file_size, file_size);
    };

    // Make pages dirty.
    for (std::size_t i = 0; i < kInitialPageCount; ++i) {
        acquire_write_release(Id(i + 1), tools::integral_key(i));
    }
    // Should get rid of cached pages that are out-of-range.
    ASSERT_OK(pager->truncate(kInitialPageCount - kFrameCount / 2));
    flush_and_match_sizes();

    // All cached pages are out-of-range
    for (std::size_t i = 0; i < kInitialPageCount - kFrameCount / 2; ++i) {
        acquire_write_release(Id(i + 1), tools::integral_key(i));
    }
    ASSERT_OK(pager->truncate(1));
    flush_and_match_sizes();
}

} // namespace calicodb
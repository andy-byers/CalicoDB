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
    (void)cache.alloc(Id(4));
    (void)cache.alloc(Id(3));
    (void)cache.alloc(Id(2));
    (void)cache.alloc(Id(1));
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
    (void)cache.alloc(Id(2));
    (void)cache.alloc(Id(1));

    (void)cache.query(Id(2));

    ASSERT_EQ(cache.next_victim()->page_id, Id(2));
    cache.erase(cache.next_victim()->page_id);
    ASSERT_EQ(cache.next_victim()->page_id, Id(1));
    cache.erase(cache.next_victim()->page_id);
}

TEST_F(PageCacheTests, ReferencedEntriesAreIgnoredDuringEviction)
{
    (void)cache.alloc(Id(2));
    (void)cache.alloc(Id(1));

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

        AlignedBuffer buffer(kPageSize * kFrameCount, kPageSize);
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
        auto *entry = cache.alloc(Id(i + 1));
        (void)frames->pin(*entry);
    }
    auto *entry = cache.alloc(Id(kFrameCount + 1));
    ASSERT_EQ(frames->available(), 0);
    ASSERT_DEATH((void)frames->pin(*entry), "expect");
}
#endif // NDEBUG

auto write_to_page(Page &page, const std::string &message) -> void
{
    EXPECT_LE(page_offset(page.id()) + message.size(), page.size());
    std::memcpy(page.data() + page.size() - message.size(), message.data(), message.size());
}

[[nodiscard]] auto read_from_page(const Page &page, std::size_t size) -> std::string
{
    EXPECT_LE(page_offset(page.id()) + size, page.size());
    std::string message(size, '\x00');
    std::memcpy(message.data(), page.data() + page.size() - message.size(), message.size());
    return message;
}

class PagerTests
    : public TestWithPager,
      public testing::Test
{
public:
    const std::string kTestMessage = "Hello, world!";
    const std::size_t kSmallSize = kFrameCount / 2;
    const std::size_t kFullSize = kFrameCount;
    const std::size_t kLargeSize = kFrameCount * 2;

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
    }

    [[nodiscard]] auto acquire_read_release(Id id, std::size_t size) const
    {
        Page page;
        EXPECT_OK(pager->acquire(id, page));
        auto message = read_from_page(page, size);
        pager->release(std::move(page));
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
    ASSERT_EQ(pager->page_count(), 1);
    ASSERT_EQ(pager->bytes_written(), kPageSize);
}

TEST_F(PagerTests, AllocatesPagesAtEOF)
{
    ASSERT_TRUE(pager->begin_txn());
    ASSERT_EQ(pager->page_count(), 1);
    ASSERT_EQ(allocate_write_release("a"), Id(2));
    ASSERT_EQ(pager->page_count(), 2);
    ASSERT_EQ(allocate_write_release("b"), Id(3));
    ASSERT_EQ(pager->page_count(), 3);
    ASSERT_EQ(allocate_write_release("c"), Id(4));
    ASSERT_EQ(pager->page_count(), 4);
}

TEST_F(PagerTests, AcquireReturnsCorrectPage)
{
    ASSERT_TRUE(pager->begin_txn());
    const auto incorrect = allocate_write_release(kTestMessage);
    const auto correct = allocate_write_release(kTestMessage);

    Page page;
    ASSERT_OK(pager->acquire(correct, page));
    ASSERT_EQ(correct, page.id());
    ASSERT_NE(incorrect, page.id());
    pager->release(std::move(page));
}

TEST_F(PagerTests, DataPersistsInEnv)
{
    ASSERT_TRUE(pager->begin_txn());
    for (std::size_t i = 0; i < kFrameCount * 10; ++i) {
        (void)allocate_write_release(tools::integral_key<16>(i));
    }
    ASSERT_OK(pager->commit_txn());
    for (std::size_t i = 0; i < kFrameCount * 10; ++i) {
        // Skip the root page, which was already allocated and is still blank.
        ASSERT_EQ(acquire_read_release(Id(i + 2), 16), tools::integral_key<16>(i))
            << "mismatch on page " << i + 1;
    }
}

template <class Test>
static auto write_pages(Test &test, std::size_t key_offset, std::size_t num_pages, std::size_t acquire_offset = 0)
{
    for (std::size_t i = 0; i < num_pages; ++i) {
        const auto message = tools::integral_key<16>(i + key_offset);
        if (i >= test.pager->page_count()) {
            (void)test.allocate_write_release(message);
        } else {
            test.acquire_write_release(Id(acquire_offset + i + 1), message);
        }
    }
}

template <class Test>
static auto read_and_check(Test &test, std::size_t key_offset, std::size_t num_pages, bool from_file = false)
{
    for (std::size_t i = 0; i < num_pages; ++i) {
        const Id page_id(i + 1);
        const auto message = tools::integral_key<16>(i + key_offset);
        if (from_file) {
            ASSERT_EQ(test.read_from_file(page_id, 16), message)
                << "mismatch on page " << page_id.value << " read from file";
        } else {
            ASSERT_EQ(test.acquire_read_release(page_id, 16), message)
                << "mismatch on page " << page_id.value << " read from pager";
        }
    }
}

TEST_F(PagerTests, NormalReadsAndWrites)
{
    ASSERT_TRUE(pager->begin_txn());

    write_pages(*this, 123, kSmallSize);
    read_and_check(*this, 123, kSmallSize);
    write_pages(*this, 456, kFullSize);
    read_and_check(*this, 456, kFullSize);
    write_pages(*this, 789, kLargeSize);
    read_and_check(*this, 789, kLargeSize);

    ASSERT_OK(pager->commit_txn());
}

TEST_F(PagerTests, NormalCommits)
{
    ASSERT_TRUE(pager->begin_txn());
    write_pages(*this, 123, kSmallSize);
    ASSERT_OK(pager->commit_txn());
    read_and_check(*this, 123, kSmallSize);

    ASSERT_TRUE(pager->begin_txn());
    write_pages(*this, 456, kFullSize);
    ASSERT_OK(pager->commit_txn());
    read_and_check(*this, 456, kFullSize);

    ASSERT_TRUE(pager->begin_txn());
    write_pages(*this, 789, kLargeSize);
    ASSERT_OK(pager->commit_txn());
    read_and_check(*this, 789, kLargeSize);
}

TEST_F(PagerTests, BasicRollbacks)
{
    ASSERT_TRUE(pager->begin_txn());
    write_pages(*this, 123, kLargeSize);
    ASSERT_OK(pager->commit_txn());
    read_and_check(*this, 123, kLargeSize);

    ASSERT_TRUE(pager->begin_txn());
    write_pages(*this, 456, kSmallSize);
    ASSERT_OK(pager->rollback_txn());
    read_and_check(*this, 123, kLargeSize);

    ASSERT_TRUE(pager->begin_txn());
    write_pages(*this, 789, kFullSize);
    ASSERT_OK(pager->rollback_txn());
    read_and_check(*this, 123, kLargeSize);

    ASSERT_TRUE(pager->begin_txn());
    write_pages(*this, 0, kLargeSize);
    ASSERT_OK(pager->rollback_txn());
    read_and_check(*this, 123, kLargeSize);
}

TEST_F(PagerTests, BasicCheckpoints)
{
    for (std::size_t i = 0; i < 10; ++i) {
        ASSERT_TRUE(pager->begin_txn());
        write_pages(*this, kFrameCount * i, kFrameCount * (i + 1));
        ASSERT_OK(pager->commit_txn());
        read_and_check(*this, kFrameCount * i, kFrameCount * (i + 1));
        ASSERT_OK(pager->checkpoint());
        // Pages returned by the pager should reflect what is on disk.
        read_and_check(*this, kFrameCount * i, kFrameCount * (i + 1));
        read_and_check(*this, kFrameCount * i, kFrameCount * (i + 1), true);
    }
}

TEST_F(PagerTests, OnlyWritesBackCommittedWalFrames)
{
    ASSERT_TRUE(pager->begin_txn());
    write_pages(*this, 0, kLargeSize);
    ASSERT_OK(pager->commit_txn());

    ASSERT_TRUE(pager->begin_txn());
    write_pages(*this, 123, kSmallSize);
    ASSERT_OK(pager->rollback_txn());

    ASSERT_OK(pager->checkpoint());
    read_and_check(*this, 0, kLargeSize);
}

TEST_F(PagerTests, WritesBackDuringCheckpoint)
{
}
//
// class TruncationTests : public PagerTests
//{
// public:
//    static constexpr std::size_t kInitialPageCount = 500;
//
//    auto SetUp() -> void override
//    {
//        for (std::size_t i = 0; i < kInitialPageCount; ++i) {
//            (void)allocate_write_release(tools::integral_key(i));
//        }
//        ASSERT_OK(pager->flush_to_disk());
//    }
//};
//
// TEST_F(TruncationTests, AllocationAfterTruncation)
//{
//    ASSERT_OK(pager->truncate(1));
//
//    for (std::size_t i = 1; i < kInitialPageCount; ++i) {
//        (void)allocate_write_release(tools::integral_key(i));
//    }
//
//    for (std::size_t i = 0; i < kInitialPageCount; ++i) {
//        const auto key = tools::integral_key(i);
//        ASSERT_EQ(acquire_read_release(Id::from_index(i), key.size()), key);
//    }
//}
//
// TEST_F(TruncationTests, OutOfRangePagesAreDiscarded)
//{
//    std::size_t base_file_size, file_size;
//    const auto flush_and_match_sizes = [&] {
//        ASSERT_OK(env->file_size(kFilename, base_file_size));
//        // If there are still cached pages past the truncation position, they will be
//        // written back to disk here, causing the file size to change.
//        ASSERT_OK(pager->flush_to_disk());
//        ASSERT_OK(env->file_size(kFilename, file_size));
//        ASSERT_EQ(base_file_size, file_size);
//    };
//
//    // Make pages dirty.
//    for (std::size_t i = 0; i < kInitialPageCount; ++i) {
//        acquire_write_release(Id(i + 1), tools::integral_key(i));
//    }
//    // Should get rid of cached pages that are out-of-range.
//    ASSERT_OK(pager->truncate(kInitialPageCount - kFrameCount / 2));
//    flush_and_match_sizes();
//
//    // All cached pages are out-of-range
//    for (std::size_t i = 0; i < kInitialPageCount - kFrameCount / 2; ++i) {
//        acquire_write_release(Id(i + 1), tools::integral_key(i));
//    }
//    ASSERT_OK(pager->truncate(1));
//    flush_and_match_sizes();
//}

} // namespace calicodb
// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "bufmgr.h"
#include "header.h"
#include "logging.h"
#include "page.h"
#include "pager.h"
#include "unit_tests.h"
#include <gtest/gtest.h>
#include <numeric>

namespace calicodb
{

[[nodiscard]] static auto make_key(U64 k) -> std::string
{
    return tools::integral_key<16>(k);
}

[[nodiscard]] static auto make_cache_entry(U64 id_value) -> PageRef
{
    return {.page_id = Id(id_value)};
}

// class PageCacheTests : public testing::Test
//{
// public:
//     PageCache cache;
// };
//
// TEST_F(PageCacheTests, EmptyCacheBehavior)
//{
//     ASSERT_EQ(cache.size(), 0);
//     ASSERT_EQ(cache.size(), 0);
//     ASSERT_EQ(cache.get(Id::root()), nullptr);
//     ASSERT_EQ(cache.next_victim(), nullptr);
// }
//
// TEST_F(PageCacheTests, OldestEntryIsEvictedFirst)
//{
//     (void)cache.alloc(Id(4));
//     (void)cache.alloc(Id(3));
//     (void)cache.alloc(Id(2));
//     (void)cache.alloc(Id(1));
//     ASSERT_EQ(cache.size(), 4);
//
//     ASSERT_EQ(cache.get(Id(4))->page_id, Id(4));
//     ASSERT_EQ(cache.get(Id(3))->page_id, Id(3));
//
//     ASSERT_EQ(cache.next_victim()->page_id, Id(2));
//     cache.erase(cache.next_victim()->page_id);
//     ASSERT_EQ(cache.next_victim()->page_id, Id(1));
//     cache.erase(cache.next_victim()->page_id);
//     ASSERT_EQ(cache.next_victim()->page_id, Id(4));
//     cache.erase(cache.next_victim()->page_id);
//     ASSERT_EQ(cache.next_victim()->page_id, Id(3));
//     cache.erase(cache.next_victim()->page_id);
//     ASSERT_EQ(cache.size(), 0);
// }
//
// TEST_F(PageCacheTests, ReplacementPolicyIgnoresQuery)
//{
//     (void)cache.alloc(Id(2));
//     (void)cache.alloc(Id(1));
//
//     (void)cache.query(Id(2));
//
//     ASSERT_EQ(cache.next_victim()->page_id, Id(2));
//     cache.erase(cache.next_victim()->page_id);
//     ASSERT_EQ(cache.next_victim()->page_id, Id(1));
//     cache.erase(cache.next_victim()->page_id);
// }
//
// TEST_F(PageCacheTests, ReferencedEntriesAreIgnoredDuringEviction)
//{
//     (void)cache.alloc(Id(2));
//     (void)cache.alloc(Id(1));
//
//     cache.query(Id(2))->refcount = 1;
//
//     ASSERT_EQ(cache.next_victim()->page_id, Id(1));
//     cache.erase(cache.next_victim()->page_id);
//     ASSERT_EQ(cache.next_victim(), nullptr);
// }
//
// class FrameManagerTests
//     : public EnvTestHarness<tools::FakeEnv>,
//       public testing::Test
//{
// public:
//     static constexpr auto kPageSize = kMinPageSize;
//     static constexpr auto kPagerFrames = kMinFrameCount;
//
//     explicit FrameManagerTests()
//     {
//         frames = std::make_unique<BufferManager>(kPageSize, kPagerFrames);
//     }
//
//     ~FrameManagerTests() override = default;
//
//     std::unique_ptr<BufferManager> frames;
//     PageCache cache;
// };
//
// TEST_F(FrameManagerTests, NewFrameManagerIsSetUpCorrectly)
//{
//     ASSERT_EQ(frames->available(), kPagerFrames);
// }
//
//#ifndef NDEBUG
// TEST_F(FrameManagerTests, OutOfFramesDeathTest)
//{
//     for (std::size_t i = 0; i < kPagerFrames; ++i) {
//         auto *entry = cache.alloc(Id(i + 1));
//         (void)frames->pin(*entry);
//     }
//     auto *entry = cache.alloc(Id(kPagerFrames + 1));
//     ASSERT_EQ(frames->available(), 0);
//     ASSERT_DEATH((void)frames->pin(*entry), "expect");
// }
//#endif // NDEBUG

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

class PagerWalTestHarness
{
public:
    static constexpr std::size_t kPagerFrames = kMinFrameCount; // Number of frames available to the pager
    static constexpr std::size_t kSomePages = kPagerFrames / 5; // Just a few pages
    static constexpr std::size_t kFullCache = kPagerFrames;     // Enough pages to fill the page cache
    static constexpr std::size_t kManyPages = kPagerFrames * 5; // Lots of pages, enough to cause many evictions
    static constexpr U32 kPageSize = kMinPageSize;

    auto init() -> void
    {
        ASSERT_NE(env, nullptr);

        const Wal::Parameters wal_param = {
            kWalFilename,
            kPageSize,
            env};
        ASSERT_OK(Wal::open(wal_param, m_wal));

        const Pager::Parameters pager_param = {
            kDBFilename,
            env,
            m_wal,
            nullptr,
            &m_state,
            kPagerFrames,
            kPageSize,
        };
        ASSERT_OK(Pager::open(pager_param, m_pager));
        // Write the freshly-allocated root page to the DB file.
        ASSERT_EQ(m_pager->mode(), Pager::kDirty);
        ASSERT_OK(m_pager->commit_txn());
        m_state.use_wal = true;
    }

    virtual ~PagerWalTestHarness()
    {
        delete m_pager;
        delete m_wal;
        delete env;
    }

    [[nodiscard]] auto fake_allocate_with_status(Page &page) const -> Status
    {
        // Get a page from the end of the file. This will increase the page count, but
        // won't skip pointer map pages or attempt to get a page from the freelist.
        auto s = m_pager->acquire(Id(m_pager->page_count() + 1), page);
        if (s.is_ok()) {
            m_pager->upgrade(page);
        }
        return s;
    }

    [[nodiscard]] auto fake_allocate() const -> Page
    {
        Page page;
        EXPECT_OK(fake_allocate_with_status(page));
        return page;
    }

    [[nodiscard]] auto allocate_write(const std::string &message) const
    {
        auto page = fake_allocate();
        write_to_page(page, message);
        return page;
    }

    auto allocate_write_release(const std::string &message) const
    {
        auto page = allocate_write(message);
        const auto id = page.id();
        m_pager->release(std::move(page));
        return id;
    }

    [[nodiscard]] auto acquire_write(Id id, const std::string &message) const
    {
        Page page;
        EXPECT_OK(m_pager->acquire(id, page));
        m_pager->upgrade(page);
        write_to_page(page, message);
        return page;
    }

    auto acquire_write_release(Id id, const std::string &message) const
    {
        auto page = acquire_write(id, message);
        m_pager->release(std::move(page));
    }

    [[nodiscard]] auto acquire_read_release(Id id, std::size_t size) const
    {
        Page page;
        EXPECT_OK(m_pager->acquire(id, page));
        auto message = read_from_page(page, size);
        m_pager->release(std::move(page));
        return message;
    }

    [[nodiscard]] auto create_freelist_pages(std::size_t n) const -> Status
    {
        CHECK_TRUE(n < kPagerFrames);
        std::vector<Page> pages;
        for (std::size_t i = 0; i < n; ++i) {
            Page page;
            // Use the real allocate method (not fake_allocate()), which doesn't hand out pointer map pages.
            // We should not destroy pointer map pages; doing so indicates a programming error. Pointer map
            // pages are destroyed naturally when the file shrinks (and the last page is never a pointer map
            // page, unless the DB was unable to allocate the page following it: a state which requires a
            // rollback anyway).
            CALICODB_TRY(m_pager->allocate(page));
            pages.emplace_back(std::move(page));
        }
        while (!pages.empty()) {
            CALICODB_TRY(m_pager->destroy(std::move(pages.back())));
            pages.pop_back();
        }
        return Status::ok();
    }

    [[nodiscard]] auto read_from_db_file(Id id, std::size_t size) const
    {
        File *file;
        std::string message(size, '\x00');
        EXPECT_OK(env->new_file(kDBFilename, file));
        EXPECT_OK(file->read_exact(
            id.value * kPageSize - message.size(),
            message.size(),
            message.data()));
        delete file;
        return message;
    }

    [[nodiscard]] auto count_db_pages() -> std::size_t
    {
        std::size_t file_size;
        EXPECT_OK(env->file_size(kDBFilename, file_size));
        EXPECT_EQ(file_size, file_size / kPageSize * kPageSize);
        return file_size / kPageSize;
    }

    DBState m_state;
    Env *env = nullptr;
    Wal *m_wal = nullptr;
    Pager *m_pager = nullptr;
};

class PagerTests
    : public PagerWalTestHarness,
      public testing::Test
{
public:
    const std::string kTestMessage = "Hello, world!";

    ~PagerTests() override = default;

    auto SetUp() -> void override
    {
        env = new tools::FakeEnv;
        init();
    }
};

TEST_F(PagerTests, NewPagerIsSetUpCorrectly)
{
    ASSERT_EQ(m_pager->page_count(), 1);
    ASSERT_EQ(m_pager->statistics().bytes_written, 1'024)
        << "test should direct first write to the DB file";
}

TEST_F(PagerTests, AllocatesPagesAtEOF)
{
    ASSERT_TRUE(m_pager->begin_txn());
    ASSERT_EQ(m_pager->page_count(), 1);
    ASSERT_EQ(allocate_write_release("a"), Id(2));
    ASSERT_EQ(m_pager->page_count(), 2);
    ASSERT_EQ(allocate_write_release("b"), Id(3));
    ASSERT_EQ(m_pager->page_count(), 3);
    ASSERT_EQ(allocate_write_release("c"), Id(4));
    ASSERT_EQ(m_pager->page_count(), 4);
    ASSERT_OK(m_pager->commit_txn());
}

TEST_F(PagerTests, AcquireReturnsCorrectPage)
{
    ASSERT_TRUE(m_pager->begin_txn());
    (void)allocate_write_release("foo");
    const auto page_id = allocate_write_release("foo");
    ASSERT_OK(m_pager->commit_txn());

    ASSERT_EQ(acquire_read_release(page_id, 3 /* bytes */), "foo");
}

template <class Test>
static auto write_pages(Test &test, std::size_t key_offset, std::size_t num_pages, std::size_t acquire_offset = 0)
{
    for (std::size_t i = 0; i < num_pages; ++i) {
        const auto message = make_key(i + key_offset);
        test.acquire_write_release(Id(acquire_offset + i + 1), message);
    }
}

template <class Test>
static auto read_and_check(Test &test, std::size_t key_offset, std::size_t num_pages, bool from_file = false)
{
    for (std::size_t i = 0; i < num_pages; ++i) {
        const Id page_id(i + 1);
        const auto message = make_key(i + key_offset);
        if (from_file) {
            ASSERT_EQ(test.read_from_db_file(page_id, 16), message)
                << "mismatch on page " << page_id.value << " read from file";
        } else {
            ASSERT_EQ(test.acquire_read_release(page_id, 16), message)
                << "mismatch on page " << page_id.value << " read from pager";
        }
    }
}

TEST_F(PagerTests, NormalReadsAndWrites)
{
    ASSERT_TRUE(m_pager->begin_txn());

    write_pages(*this, 123, kSomePages);
    read_and_check(*this, 123, kSomePages);
    write_pages(*this, 456, kFullCache);
    read_and_check(*this, 456, kFullCache);
    write_pages(*this, 789, kManyPages);
    read_and_check(*this, 789, kManyPages);

    ASSERT_OK(m_pager->commit_txn());
}

TEST_F(PagerTests, NormalCommits)
{
    ASSERT_TRUE(m_pager->begin_txn());
    write_pages(*this, 123, kSomePages);
    ASSERT_OK(m_pager->commit_txn());
    read_and_check(*this, 123, kSomePages);

    ASSERT_TRUE(m_pager->begin_txn());
    write_pages(*this, 456, kFullCache);
    ASSERT_OK(m_pager->commit_txn());
    read_and_check(*this, 456, kFullCache);

    ASSERT_TRUE(m_pager->begin_txn());
    write_pages(*this, 789, kManyPages);
    ASSERT_OK(m_pager->commit_txn());
    read_and_check(*this, 789, kManyPages);
}

TEST_F(PagerTests, BasicRollbacks)
{
    ASSERT_TRUE(m_pager->begin_txn());
    write_pages(*this, 123, kManyPages);
    ASSERT_OK(m_pager->commit_txn());
    read_and_check(*this, 123, kManyPages);

    ASSERT_TRUE(m_pager->begin_txn());
    write_pages(*this, 456, kSomePages);
    ASSERT_OK(m_pager->rollback_txn());
    read_and_check(*this, 123, kManyPages);

    ASSERT_TRUE(m_pager->begin_txn());
    write_pages(*this, 789, kFullCache);
    ASSERT_OK(m_pager->rollback_txn());
    read_and_check(*this, 123, kManyPages);

    ASSERT_TRUE(m_pager->begin_txn());
    write_pages(*this, 0, kManyPages);
    ASSERT_OK(m_pager->rollback_txn());
    read_and_check(*this, 123, kManyPages);
}

TEST_F(PagerTests, RollbackPageCounts)
{
    ASSERT_TRUE(m_pager->begin_txn());
    write_pages(*this, 0, 10);
    ASSERT_EQ(m_pager->page_count(), 10);
    ASSERT_OK(m_pager->rollback_txn());
    ASSERT_EQ(m_pager->page_count(), 1);

    ASSERT_EQ(m_pager->page_count(), 1);
    ASSERT_TRUE(m_pager->begin_txn());
    write_pages(*this, 123, 10);
    ASSERT_EQ(m_pager->page_count(), 10);
    ASSERT_OK(m_pager->commit_txn());

    ASSERT_TRUE(m_pager->begin_txn());
    write_pages(*this, 456, 20);
    ASSERT_EQ(m_pager->page_count(), 20);
    ASSERT_OK(m_pager->rollback_txn());
    ASSERT_EQ(m_pager->page_count(), 10);
    read_and_check(*this, 123, 10);
}

TEST_F(PagerTests, BasicCheckpoints)
{
    for (std::size_t i = 0; i < 10; ++i) {
        ASSERT_TRUE(m_pager->begin_txn());
        write_pages(*this, kPagerFrames * i, kPagerFrames * (i + 1));
        ASSERT_OK(m_pager->commit_txn());
        read_and_check(*this, kPagerFrames * i, kPagerFrames * (i + 1));
        ASSERT_OK(m_pager->checkpoint());
        // Pages returned by the pager should reflect what is on disk.
        read_and_check(*this, kPagerFrames * i, kPagerFrames * (i + 1));
        read_and_check(*this, kPagerFrames * i, kPagerFrames * (i + 1), true);
    }
}

TEST_F(PagerTests, SequentialPageUsage)
{
    ASSERT_TRUE(m_pager->begin_txn());
    write_pages(*this, 0, kManyPages);
    write_pages(*this, 42, kManyPages);
    ASSERT_OK(m_pager->commit_txn());
    read_and_check(*this, 42, kManyPages);
}

TEST_F(PagerTests, ReverseSequentialPageUsage)
{
    ASSERT_TRUE(m_pager->begin_txn());
    write_pages(*this, 0, kManyPages);

    for (std::size_t i = 0; i < kManyPages; ++i) {
        const auto j = kManyPages - i - 1;
        acquire_write_release(Id(j + 1), make_key(j + 42));
    }
    ASSERT_OK(m_pager->commit_txn());
    read_and_check(*this, 42, kManyPages);
}

TEST_F(PagerTests, RandomPageUsage)
{
    std::vector<unsigned> is(kManyPages);
    std::iota(begin(is), end(is), 0);
    std::default_random_engine rng(42);
    std::shuffle(begin(is), end(is), rng);

    ASSERT_TRUE(m_pager->begin_txn());
    write_pages(*this, 0, is.size());
    for (auto i : is) {
        acquire_write_release(Id(i + 1), make_key(i + 42));
    }
    ASSERT_OK(m_pager->commit_txn());
    read_and_check(*this, 42, is.size());
}

TEST_F(PagerTests, OnlyWritesBackCommittedWalFrames)
{
    ASSERT_TRUE(m_pager->begin_txn());
    write_pages(*this, 42, kManyPages);
    ASSERT_OK(m_pager->commit_txn());

    // Modify the first kSomePages frames, then roll back the changes.
    ASSERT_TRUE(m_pager->begin_txn());
    write_pages(*this, 0, kSomePages);
    ASSERT_OK(m_pager->rollback_txn());

    ASSERT_OK(m_pager->checkpoint());
    read_and_check(*this, 42, kManyPages);
}

TEST_F(PagerTests, TransactionBehavior)
{
    // Only able to start a transaction once.
    ASSERT_TRUE(m_pager->begin_txn());
    ASSERT_FALSE(m_pager->begin_txn());

    // Empty transactions are OK.
    ASSERT_OK(m_pager->commit_txn());
    ASSERT_TRUE(m_pager->begin_txn());
    ASSERT_OK(m_pager->rollback_txn());
}

TEST_F(PagerTests, AcquirePastEOF)
{
    // Create "kManyPages" pages.
    ASSERT_TRUE(m_pager->begin_txn());
    write_pages(*this, 0, kManyPages);
    ASSERT_OK(m_pager->commit_txn());

    // ID of a page that is way past the logical end of the DB file (the physical
    // size is still 0, but conceptually, there are kManyPages pages in existence).
    const auto kOutOfBounds = kManyPages * 10;

    Page page;
    ASSERT_OK(m_pager->acquire(Id(kOutOfBounds), page));
    ASSERT_EQ(page.id(), Id(kOutOfBounds));

    // Since this is a new page, it must be upgraded. Otherwise, it won't ever be
    // written to the WAL, and there will be no indication that the DB size changed.
    // Usually, new pages are obtained by calling Pager::allocate(), but this should
    // work as well.
    ASSERT_TRUE(m_pager->begin_txn());
    m_pager->upgrade(page);
    m_pager->release(std::move(page));
    ASSERT_OK(m_pager->commit_txn());

    ASSERT_EQ(m_pager->page_count(), kOutOfBounds)
        << "DB page count was not updated";

    // Cause the out-of-bounds page to be evicted.
    ASSERT_TRUE(m_pager->begin_txn());
    write_pages(*this, 0, kManyPages);
    ASSERT_OK(m_pager->commit_txn());

    ASSERT_EQ(count_db_pages(), 1)
        << "file have 1 page: no checkpoint has occurred";

    ASSERT_OK(m_pager->checkpoint());
    ASSERT_EQ(m_pager->page_count(), kOutOfBounds);
    ASSERT_EQ(count_db_pages(), kOutOfBounds);

    // Intervening pages should be usable now. They are not in the WAL, so they must
    // be read from the DB file, modified in memory, written back to the WAL, then
    // read out of the WAL again.
    ASSERT_TRUE(m_pager->begin_txn());
    write_pages(*this, 42, kOutOfBounds);
    ASSERT_OK(m_pager->commit_txn());
    read_and_check(*this, 42, kOutOfBounds);
}

TEST_F(PagerTests, FreelistUsage)
{
    ASSERT_TRUE(m_pager->begin_txn());
    ASSERT_OK(create_freelist_pages(kSomePages * 2));
    write_pages(*this, 123, kSomePages * 2);
    ASSERT_OK(m_pager->commit_txn());
    read_and_check(*this, 123, kSomePages * 2);

    ASSERT_TRUE(m_pager->begin_txn());
    write_pages(*this, 456, kSomePages);
    ASSERT_OK(m_pager->rollback_txn());
    read_and_check(*this, 123, kSomePages * 2);

    ASSERT_OK(m_pager->checkpoint());
    read_and_check(*this, 123, kSomePages * 2);
    read_and_check(*this, 123, kSomePages * 2, true);
}

#ifndef NDEBUG
TEST_F(PagerTests, InvalidModeDeathTest)
{
    ASSERT_EQ(m_pager->mode(), Pager::kOpen);
    ASSERT_DEATH((void)m_pager->commit_txn(), "expect");
    ASSERT_DEATH((void)m_pager->rollback_txn(), "expect");

    m_pager->set_status(Status::io_error("I/O error"));
    ASSERT_EQ(m_pager->mode(), Pager::kError);
    ASSERT_DEATH((void)m_pager->begin_txn(), "expect");
    ASSERT_DEATH((void)m_pager->checkpoint(), "expect");
}

TEST_F(PagerTests, DoubleFreeDeathTest)
{
    ASSERT_TRUE(m_pager->begin_txn());
    for (std::size_t i = 0; i < 2; ++i) {
        for (std::size_t j = 0; j < 2; ++j) {
            Page page;
            ASSERT_OK(m_pager->allocate(page));

            if (i) {
                m_pager->release(std::move(page));
            } else {
                ASSERT_OK(m_pager->destroy(std::move(page)));
            }

            if (j) {
                ASSERT_DEATH(m_pager->release(std::move(page)), "expect");
            } else {
                ASSERT_DEATH((void)m_pager->destroy(std::move(page)), "expect");
            }
        }
    }
    ASSERT_OK(m_pager->commit_txn());
}

TEST_F(PagerTests, DestroyPointerMapPageDeathTest)
{
    ASSERT_TRUE(m_pager->begin_txn());
    Page page;
    ASSERT_OK(m_pager->acquire(Id(2), page));
    ASSERT_DEATH((void)m_pager->destroy(std::move(page)), "expect");
    ASSERT_OK(m_pager->commit_txn());
}
#endif // NDEBUG

class TruncationTests : public PagerTests
{
public:
    static constexpr std::size_t kInitialPageCount = 500;

    auto SetUp() -> void override
    {
        PagerTests::SetUp();
        ASSERT_TRUE(m_pager->begin_txn());
        write_pages(*this, 0, kInitialPageCount);
    }

    auto TearDown() -> void override
    {
        if (m_pager->mode() != Pager::kOpen) {
            ASSERT_OK(m_pager->commit_txn());
        }
    }
};

TEST_F(TruncationTests, AllocationAfterTruncation)
{
    m_pager->set_page_count(1);

    write_pages(*this, 0, kInitialPageCount * 2);
    read_and_check(*this, 0, kInitialPageCount * 2);
}

TEST_F(TruncationTests, OnlyValidPagesAreCheckpointed)
{
    // Should get rid of cached pages that are out-of-range.
    m_pager->set_page_count(kInitialPageCount / 2);

    std::size_t file_size;
    ASSERT_OK(env->file_size(kDBFilename, file_size));
    ASSERT_EQ(file_size, kPageSize)
        << "root page was not allocated";

    ASSERT_OK(m_pager->commit_txn());

    // When the WAL is enabled, the DB file is not written until checkpoint.
    ASSERT_OK(env->file_size(kDBFilename, file_size));
    ASSERT_EQ(file_size, kPageSize);

    // If there are still cached pages past the truncation position, they will be
    // written back to disk here, causing the file size to be incorrect.
    ASSERT_OK(m_pager->checkpoint());

    ASSERT_OK(env->file_size(kDBFilename, file_size));
    ASSERT_EQ(file_size, kInitialPageCount * kPageSize / 2);
}

#ifndef NDEBUG
TEST_F(TruncationTests, PurgeRootDeathTest)
{
    ASSERT_DEATH(m_pager->set_page_count(0), "expect");
}
#endif // NDEBUG

class RandomDirtyListBuilder
{
public:
    explicit RandomDirtyListBuilder(std::size_t page_size)
        : m_page_size(page_size),
          m_random(page_size * 256)
    {
    }

    // NOTE: Invalidates dirty lists previously obtained through this method. The pgno vector must not
    //       have any duplicate page numbers.
    auto build(const std::vector<U32> &pgno, std::vector<PageRef> &out) -> void
    {
        CALICODB_EXPECT_FALSE(pgno.empty());
        out.resize(pgno.size());

        for (std::size_t i = 0; i < out.size(); ++i) {
            while (pgno[i] * m_page_size > m_pages.size()) {
                m_pages.append(std::string(m_page_size, '\0'));
            }
            std::memcpy(
                m_pages.data() + (pgno[i] - 1) * m_page_size,
                m_random.Generate(m_page_size).data(),
                m_page_size);

            out[i].page_id = Id(pgno[i]);
            if (i != 0) {
                out[i].prev = &out[i - 1];
            }
            if (i < out.size() - 1) {
                out[i].next = &out[i + 1];
            }
        }
        for (auto &d : out) {
            d.page = m_pages.data() + d.page_id.as_index() * m_page_size;
        }
    }

    [[nodiscard]] auto data() const -> Slice
    {
        return m_pages;
    }

private:
    std::string m_pages;
    tools::RandomGenerator m_random;
    std::size_t m_page_size = 0;
};

class WalTestBase : public EnvTestHarness<tools::FakeEnv>
{
protected:
    static constexpr std::size_t kPageSize = kMinPageSize;

    WalTestBase()
    {
        m_param = {
            .filename = kWalFilename,
            .page_size = kPageSize,
            .env = &env(),
        };
        EXPECT_OK(Wal::open(m_param, m_wal));
    }

    virtual ~WalTestBase()
    {
        close();
    }

    auto close() -> void
    {
        ASSERT_OK(Wal::close(m_wal));
        ASSERT_EQ(m_wal, nullptr);
    }

    Wal *m_wal = nullptr;
    Wal::Parameters m_param;
};

class WalTests
    : public WalTestBase,
      public testing::Test
{
protected:
    ~WalTests() override = default;
};

TEST_F(WalTests, EmptyWalIsRemovedOnClose)
{
    ASSERT_TRUE(env().file_exists(kWalFilename));
    close();
    ASSERT_FALSE(env().file_exists(kWalFilename));
}

TEST_F(WalTests, WritingEmptyDirtyListIsNOOP)
{
    ASSERT_OK(m_wal->write(nullptr, 0));
    ASSERT_OK(m_wal->write(nullptr, 0));

    std::size_t file_size;
    ASSERT_OK(env().file_size(kWalFilename, file_size));
    ASSERT_LT(file_size, kPageSize);
}

class WalParamTests
    : public WalTestBase,
      public testing::TestWithParam<
          std::tuple<std::size_t, std::size_t, std::size_t>>
{
protected:
    static constexpr std::size_t kPageSize = kMinPageSize;

    explicit WalParamTests()
        : m_builder(kPageSize),
          m_saved(kPageSize),
          m_rng(42),
          m_fake_param {
              .filename = "fake",
              .page_size = kPageSize,
              .env = &env(),
          },
          m_fake(new tools::FakeWal(m_fake_param))
    {
    }

    ~WalParamTests() override
    {
        delete m_fake;
    }

    auto write_records(std::size_t num_pages, bool commit)
    {
        // The same "num_pages" is used each time, so every page in the builder's internal buffer
        // will be overwritten. We should get back the most-recent version of each page when the
        // WAL is queried.
        static constexpr std::size_t kNumDuplicates = 3;
        for (std::size_t i = 0; i < kNumDuplicates; ++i) {
            std::vector<U32> pgno(num_pages);
            std::iota(begin(pgno), end(pgno), 1);
            std::shuffle(begin(pgno), end(pgno), m_rng);

            std::vector<PageRef> dirty;
            m_builder.build(pgno, dirty);
            auto db_data = m_builder.data();
            auto db_size = db_data.size() / kPageSize * commit;
            EXPECT_OK(m_wal->write(&dirty[0], db_size));
            EXPECT_OK(m_fake->write(&dirty[0], db_size));
        }
    }

    auto read_and_check_records() -> void
    {
        const auto db_data = m_builder.data();
        char real[kPageSize], fake[kPageSize];

        for (std::size_t i = 0;; ++i) {
            if (i * kPageSize >= db_data.size()) {
                break;
            }
            auto *rp = real, *fp = fake;
            ASSERT_OK(m_wal->read(Id(i + 1), rp));
            ASSERT_OK(m_fake->read(Id(i + 1), fp));
            if (fp) {
                ASSERT_NE(rp, nullptr);
                CHECK_EQ(std::string(real, kPageSize),
                         std::string(fake, kPageSize));
            } else {
                ASSERT_EQ(rp, nullptr);
            }
        }
    }

    auto reopen_wals() -> void
    {
        ASSERT_OK(Wal::close(m_wal));
        ASSERT_OK(Wal::open(m_param, m_wal));
        ASSERT_OK(m_fake->abort());
    }

    auto run_and_validate_checkpoint(bool save_state = true) -> void
    {
        File *real, *fake;
        ASSERT_OK(env().new_file("real", real));
        ASSERT_OK(env().new_file("fake", fake));
        ASSERT_OK(m_wal->checkpoint(*real, nullptr));
        ASSERT_OK(m_fake->checkpoint(*fake, nullptr));

        std::size_t file_size;
        ASSERT_OK(env().file_size("fake", file_size));

        std::string real_buf(file_size, '\0');
        std::string fake_buf(file_size, '\0');
        ASSERT_OK(real->read_exact(0, real_buf.size(), real_buf.data()));
        ASSERT_OK(fake->read_exact(0, fake_buf.size(), fake_buf.data()));
        delete real;
        delete fake;

        if (save_state) {
            m_previous_db = m_builder.data().truncate(file_size).to_string();
        }
        ASSERT_EQ(real_buf, fake_buf);
        ASSERT_EQ(real_buf, m_previous_db);
    }

    auto test_write_and_read_back() -> void
    {
        for (std::size_t iteration = 0; iteration < m_iterations; ++iteration) {
            write_records(m_pages_per_iter, m_commit_interval != 0);
            read_and_check_records();
        }
    }

    auto test_operations(bool abort_uncommitted, bool reopen) -> void
    {
        for (std::size_t iteration = 0; iteration < m_iterations; ++iteration) {
            const auto is_commit = m_commit_interval && iteration % m_commit_interval == m_commit_interval - 1;
            write_records(m_pages_per_iter, is_commit);
            if (abort_uncommitted && !is_commit) {
                ASSERT_OK(m_wal->abort());
                ASSERT_OK(m_fake->abort());
            }
            if (reopen) {
                reopen_wals();
            }
            read_and_check_records();
            if (abort_uncommitted || is_commit) {
                run_and_validate_checkpoint(is_commit);
            }
        }
    }

    std::size_t m_commit_interval = std::get<0>(GetParam());
    std::size_t m_iterations = std::get<1>(GetParam());
    std::size_t m_pages_per_iter = std::get<2>(GetParam());
    std::string m_previous_db;
    std::default_random_engine m_rng;
    Wal::Parameters m_fake_param;
    RandomDirtyListBuilder m_builder;
    RandomDirtyListBuilder m_saved;
    tools::FakeWal *m_fake = nullptr;
};

TEST_P(WalParamTests, WriteAndReadBack)
{
    test_write_and_read_back();
}

TEST_P(WalParamTests, OperationsA)
{
    test_operations(true, false);
}

TEST_P(WalParamTests, OperationsB)
{
    test_operations(true, true);
}

TEST_P(WalParamTests, OperationsC)
{
    test_operations(false, false);
}

TEST_P(WalParamTests, OperationsD)
{
    test_operations(false, true);
}

INSTANTIATE_TEST_SUITE_P(
    WalParamTests,
    WalParamTests,
    ::testing::Values(
        std::make_tuple(0, 1, 1),
        std::make_tuple(0, 1, 2),
        std::make_tuple(0, 1, 3),
        std::make_tuple(0, 1, 10),
        std::make_tuple(0, 1, 100),
        std::make_tuple(0, 1, 1'000),

        std::make_tuple(0, 5, 1),
        std::make_tuple(0, 5, 2),
        std::make_tuple(0, 5, 3),
        std::make_tuple(0, 5, 10),
        std::make_tuple(0, 5, 100),
        std::make_tuple(0, 5, 200),

        std::make_tuple(1, 1, 1),
        std::make_tuple(1, 1, 2),
        std::make_tuple(1, 1, 3),
        std::make_tuple(1, 1, 10),
        std::make_tuple(1, 1, 100),
        std::make_tuple(1, 1, 1'000),

        std::make_tuple(1, 2, 1),
        std::make_tuple(1, 5, 2),
        std::make_tuple(1, 5, 3),
        std::make_tuple(1, 5, 10),
        std::make_tuple(1, 5, 100),
        std::make_tuple(1, 5, 200),

        std::make_tuple(5, 20, 1),
        std::make_tuple(5, 20, 2),
        std::make_tuple(5, 20, 3),
        std::make_tuple(5, 20, 10),
        std::make_tuple(5, 20, 50)));

class WalPagerFaultTests
    : public PagerWalTestHarness,
      public testing::TestWithParam<std::tuple<std::size_t, std::string, tools::Interceptor::Type>>
{
public:
    explicit WalPagerFaultTests()
        : random(1'024 * 1'024 * 8)
    {
    }

    ~WalPagerFaultTests() override
    {
        close_pager_and_wal();
    }

    auto SetUp() -> void override
    {
        env = new tools::TestEnv;
    }

    auto run_setup_and_operations()
    {
        (void)env->remove_file(kDBFilename);
        (void)env->remove_file(kWalFilename);

        const Wal::Parameters wal_param = {
            kWalFilename,
            kPageSize,
            env,
        };
        auto s = Wal::open(wal_param, m_wal);
        if (!s.is_ok()) {
            return;
        }

        const Pager::Parameters pager_param = {
            kDBFilename,
            env,
            m_wal,
            nullptr,
            &m_state,
            kPagerFrames,
            kPageSize,
        };

        s = Pager::open(pager_param, m_pager);
        m_state.use_wal = true;

        if (s.is_ok()) {
            (void)m_pager->begin_txn();

            std::vector<std::size_t> indices(std::get<0>(GetParam()));
            std::iota(begin(indices), end(indices), 0);
            std::default_random_engine rng(42);
            std::shuffle(begin(indices), end(indices), rng);

            // Transaction has already been started, since this is the first time the pager
            // has been opened.
            for (auto i : indices) {
                Page page;
                const Id page_id(i + 1);
                const auto message = make_key(i);
                s = m_pager->acquire(page_id, page);
                if (!s.is_ok()) {
                    break;
                }

                m_pager->upgrade(page);
                std::memcpy(page.data() + page.size() - message.size(), message.data(), message.size());
                m_pager->release(std::move(page));

                // Perform a commit every so often and checkpoint at a less frequent interval.
                if (i && i % 25 == 0) {
                    s = m_pager->commit_txn();
                    if (!s.is_ok()) {
                        break;
                    }
                    if (i % 5 == 0) {
                        s = m_pager->checkpoint();
                        if (!s.is_ok()) {
                            break;
                        }
                    }
                    ASSERT_TRUE(m_pager->begin_txn());
                }
            }
            if (s.is_ok()) {
                s = m_pager->commit_txn();
                if (s.is_ok()) {
                    s = m_pager->checkpoint();
                }
            }

            if (s.is_ok()) {
                m_counter = -1;
                // Should have written monotonically increasing integers back to the DB file.
                read_and_check(*this, 0, indices.size());
                read_and_check(*this, 0, indices.size(), true);
                m_completed = true;
            } else {
                // Only 1 interceptor is set, so this should succeed.
                ASSERT_OK(m_pager->rollback_txn());
            }
        }
        close_pager_and_wal();
    }

    auto close_pager_and_wal() -> void
    {
        (void)Wal::close(m_wal);
        delete m_pager;
        m_pager = nullptr;
        m_wal = nullptr;
    }

    tools::RandomGenerator random;
    int m_counter = 0;
    bool m_completed = false;
};

TEST_P(WalPagerFaultTests, SetupAndOperations)
{
    const auto [_, filename, type] = GetParam();
    reinterpret_cast<tools::TestEnv *>(env)
        ->add_interceptor(filename, tools::Interceptor(
                                        type,
                                        [this] {
                                            return m_counter-- == 0 ? special_error() : Status::ok();
                                        }));

    int count = 0;
    while (!m_completed) {
        m_counter = count++;
        run_setup_and_operations();
    }
}

INSTANTIATE_TEST_SUITE_P(
    WalPagerFaultTests,
    WalPagerFaultTests,
    ::testing::Values(
        std::make_tuple(10, kDBFilename, tools::Interceptor::kRead),
        std::make_tuple(10, kDBFilename, tools::Interceptor::kWrite),
        std::make_tuple(10, kWalFilename, tools::Interceptor::kRead),
        std::make_tuple(10, kWalFilename, tools::Interceptor::kWrite),

        std::make_tuple(100, kDBFilename, tools::Interceptor::kRead),
        std::make_tuple(100, kDBFilename, tools::Interceptor::kWrite),
        std::make_tuple(100, kWalFilename, tools::Interceptor::kRead),
        std::make_tuple(100, kWalFilename, tools::Interceptor::kWrite)));

} // namespace calicodb
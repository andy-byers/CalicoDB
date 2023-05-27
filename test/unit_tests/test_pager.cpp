// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "bufmgr.h"
#include "header.h"
#include "logging.h"
#include "page.h"
#include "pager.h"
#include "scope_guard.h"
#include "unit_tests.h"
#include <gtest/gtest.h>
#include <numeric>
#include <thread>

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

class PageCacheTests : public testing::Test
{
public:
    explicit PageCacheTests()
        : mgr(kMinFrameCount)
    {
    }

    ~PageCacheTests() override = default;

    Bufmgr mgr;
};

TEST_F(PageCacheTests, EmptyBehavior)
{
    ASSERT_EQ(mgr.occupied(), 0);
    ASSERT_EQ(mgr.occupied(), 0);
    ASSERT_EQ(mgr.get(Id(2)), nullptr);
    ASSERT_EQ(mgr.next_victim(), nullptr);
}

TEST_F(PageCacheTests, OldestReferenceIsEvictedFirst)
{
    (void)mgr.alloc(Id(5));
    (void)mgr.alloc(Id(4));
    (void)mgr.alloc(Id(3));
    (void)mgr.alloc(Id(2));
    ASSERT_EQ(mgr.occupied(), 4);

    ASSERT_EQ(mgr.get(Id(5))->page_id, Id(5));
    ASSERT_EQ(mgr.get(Id(4))->page_id, Id(4));

    ASSERT_EQ(mgr.next_victim()->page_id, Id(3));
    mgr.erase(mgr.next_victim()->page_id);
    ASSERT_EQ(mgr.next_victim()->page_id, Id(2));
    mgr.erase(mgr.next_victim()->page_id);
    ASSERT_EQ(mgr.next_victim()->page_id, Id(5));
    mgr.erase(mgr.next_victim()->page_id);
    ASSERT_EQ(mgr.next_victim()->page_id, Id(4));
    mgr.erase(mgr.next_victim()->page_id);
    ASSERT_EQ(mgr.occupied(), 0);
}

TEST_F(PageCacheTests, ReplacementPolicyIgnoresQuery)
{
    (void)mgr.alloc(Id(3));
    (void)mgr.alloc(Id(2));

    (void)mgr.query(Id(3));

    ASSERT_EQ(mgr.next_victim()->page_id, Id(3));
    mgr.erase(mgr.next_victim()->page_id);
    ASSERT_EQ(mgr.next_victim()->page_id, Id(2));
    mgr.erase(mgr.next_victim()->page_id);
}

TEST_F(PageCacheTests, RefcountsAreConsideredDuringEviction)
{
    (void)mgr.alloc(Id(3));
    (void)mgr.alloc(Id(2));

    mgr.query(Id(3))->refcount = 2;

    ASSERT_EQ(mgr.next_victim()->page_id, Id(2));
    mgr.erase(mgr.next_victim()->page_id);
    ASSERT_EQ(mgr.next_victim(), nullptr);
}

auto write_to_page(Page &page, const std::string &message) -> void
{
    EXPECT_LE(page_offset(page.id()) + message.size(), kPageSize);
    std::memcpy(page.data() + kPageSize - message.size(), message.data(), message.size());
}

[[nodiscard]] auto read_from_page(const Page &page, std::size_t size) -> std::string
{
    EXPECT_LE(page_offset(page.id()) + size, kPageSize);
    std::string message(size, '\x00');
    std::memcpy(message.data(), page.data() + kPageSize - message.size(), message.size());
    return message;
}

class PagerWalTestHarness
{
public:
    static constexpr std::size_t kPagerFrames = kMinFrameCount; // Number of frames available to the pager
    static constexpr std::size_t kSomePages = kPagerFrames / 5; // Just a few pages
    static constexpr std::size_t kFullCache = kPagerFrames;     // Enough pages to fill the page cache
    static constexpr std::size_t kManyPages = kPagerFrames * 5; // Lots of pages, enough to cause many evictions

    auto write_db_header() const -> void
    {
        std::string buffer(kPageSize, '\0');
        std::memcpy(buffer.data(), FileHeader::kFmtString, sizeof(FileHeader::kFmtString));
        buffer[FileHeader::kFmtVersionOfs] = FileHeader::kFmtVersion;
        put_u32(buffer.data() + FileHeader::kPageCountOffset, 1);
        tools::write_string_to_file(*env, kDBFilename, buffer);
    }

    [[nodiscard]] auto init_with_status() -> Status
    {
        CALICODB_EXPECT_NE(env, nullptr);
        CALICODB_EXPECT_FALSE(m_pager);

        File *file;
        CALICODB_TRY(env->new_file(kDBFilename, Env::kCreate, file));

        const Pager::Parameters pager_param = {
            kDBFilename,
            kWalFilename,
            file,
            env,
            nullptr,
            &m_state,
            nullptr,
            kPagerFrames,
        };
        CALICODB_TRY(Pager::open(pager_param, m_pager));
        m_pager->set_page_count(1);
        return Status::ok();
    }

    auto write_header_and_init() -> void
    {
        write_db_header();
        ASSERT_OK(init_with_status());
    }

    virtual ~PagerWalTestHarness()
    {
        delete m_pager;
        delete env;
    }

    [[nodiscard]] auto fake_allocate_with_status(Page &page) const -> Status
    {
        // Get a page from the end of the file. This will increase the page count, but
        // won't skip pointer map pages or attempt to get a page from the freelist.
        auto s = m_pager->acquire(Id(m_pager->page_count() + 1), page);
        if (s.is_ok()) {
            m_pager->mark_dirty(page);
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
        m_pager->mark_dirty(page);
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
            // We should not free pointer map pages; doing so indicates a programming error. Pointer map
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
        EXPECT_OK(env->new_file(kDBFilename, Env::kCreate, file));
        EXPECT_OK(file->read_exact(
            id.value * kPageSize - message.size(),
            message.size(),
            message.data()));
        delete file;
        return message;
    }

    [[nodiscard]] auto count_db_pages() const -> std::size_t
    {
        std::size_t file_size;
        EXPECT_OK(env->file_size(kDBFilename, file_size));
        EXPECT_EQ(file_size, file_size / kPageSize * kPageSize);
        return file_size / kPageSize;
    }

    DBState m_state;
    Env *env = nullptr;
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
        env = new tools::FakeEnv();
        write_header_and_init();
    }
};

TEST_F(PagerTests, NewPagerIsSetUpCorrectly)
{
    ASSERT_EQ(m_pager->page_count(), 1);
}

TEST_F(PagerTests, AllocatesPagesAtEOF)
{
    ASSERT_OK(m_pager->start_reader());
    ASSERT_OK(m_pager->start_writer());
    ASSERT_EQ(m_pager->page_count(), 1);
    ASSERT_EQ(allocate_write_release("a"), Id(2));
    ASSERT_EQ(m_pager->page_count(), 2);
    ASSERT_EQ(allocate_write_release("b"), Id(3));
    ASSERT_EQ(m_pager->page_count(), 3);
    ASSERT_EQ(allocate_write_release("c"), Id(4));
    ASSERT_EQ(m_pager->page_count(), 4);
    ASSERT_OK(m_pager->commit());
    m_pager->finish();
}

TEST_F(PagerTests, AcquireReturnsCorrectPage)
{
    ASSERT_OK(m_pager->start_reader());
    ASSERT_OK(m_pager->start_writer());
    (void)allocate_write_release("foo");
    const auto page_id = allocate_write_release("foo");
    ASSERT_OK(m_pager->commit());

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
    ASSERT_OK(m_pager->start_reader());
    ASSERT_OK(m_pager->start_writer());

    write_pages(*this, 123, kSomePages);
    read_and_check(*this, 123, kSomePages);
    write_pages(*this, 456, kFullCache);
    read_and_check(*this, 456, kFullCache);
    write_pages(*this, 789, kManyPages);
    read_and_check(*this, 789, kManyPages);

    ASSERT_OK(m_pager->commit());
}

TEST_F(PagerTests, NormalCommits)
{
    ASSERT_OK(m_pager->start_reader());
    ASSERT_OK(m_pager->start_writer());
    write_pages(*this, 123, kSomePages);
    ASSERT_OK(m_pager->commit());
    read_and_check(*this, 123, kSomePages);
    m_pager->finish();

    ASSERT_OK(m_pager->start_reader());
    ASSERT_OK(m_pager->start_writer());
    write_pages(*this, 456, kFullCache);
    ASSERT_OK(m_pager->commit());
    read_and_check(*this, 456, kFullCache);
    m_pager->finish();

    ASSERT_OK(m_pager->start_reader());
    ASSERT_OK(m_pager->start_writer());
    write_pages(*this, 789, kManyPages);
    ASSERT_OK(m_pager->commit());
    read_and_check(*this, 789, kManyPages);
    m_pager->finish();
}

TEST_F(PagerTests, NormalRollbacks)
{
    ASSERT_OK(m_pager->start_reader());
    ASSERT_OK(m_pager->start_writer());
    write_pages(*this, 123, kManyPages);
    ASSERT_OK(m_pager->commit());
    read_and_check(*this, 123, kManyPages);
    m_pager->finish();

    ASSERT_OK(m_pager->start_reader());
    ASSERT_OK(m_pager->start_writer());
    write_pages(*this, 456, kSomePages);
    m_pager->rollback();
    read_and_check(*this, 123, kManyPages);
    m_pager->finish();

    ASSERT_OK(m_pager->start_reader());
    ASSERT_OK(m_pager->start_writer());
    write_pages(*this, 789, kFullCache);
    m_pager->rollback();
    read_and_check(*this, 123, kManyPages);
    m_pager->finish();

    ASSERT_OK(m_pager->start_reader());
    ASSERT_OK(m_pager->start_writer());
    write_pages(*this, 0, kManyPages);
    m_pager->rollback();
    read_and_check(*this, 123, kManyPages);
    m_pager->finish();
}

TEST_F(PagerTests, RollbackPageCounts)
{
    ASSERT_OK(m_pager->start_reader());
    ASSERT_OK(m_pager->start_writer());
    write_pages(*this, 0, 10);
    ASSERT_EQ(m_pager->page_count(), 10);
    m_pager->rollback();
    ASSERT_EQ(m_pager->page_count(), 1);
    m_pager->finish();

    ASSERT_EQ(m_pager->page_count(), 1);
    ASSERT_OK(m_pager->start_reader());
    ASSERT_OK(m_pager->start_writer());
    write_pages(*this, 123, 10);
    ASSERT_EQ(m_pager->page_count(), 10);
    ASSERT_OK(m_pager->commit());
    m_pager->finish();

    ASSERT_OK(m_pager->start_reader());
    ASSERT_OK(m_pager->start_writer());
    write_pages(*this, 456, 20);
    ASSERT_EQ(m_pager->page_count(), 20);
    m_pager->rollback();
    ASSERT_EQ(m_pager->page_count(), 10);
    read_and_check(*this, 123, 10);
    m_pager->finish();
}

TEST_F(PagerTests, BasicCheckpoints)
{
    for (std::size_t i = 0; i < 10; ++i) {
        ASSERT_OK(m_pager->start_reader());
        ASSERT_OK(m_pager->start_writer());
        write_pages(*this, kPagerFrames * i, kPagerFrames * (i + 1));
        ASSERT_OK(m_pager->commit());
        read_and_check(*this, kPagerFrames * i, kPagerFrames * (i + 1));
        m_pager->finish();

        ASSERT_OK(m_pager->checkpoint(true));

        // Pages returned by the pager should reflect what is on disk.
        ASSERT_OK(m_pager->start_reader());
        read_and_check(*this, kPagerFrames * i, kPagerFrames * (i + 1));
        read_and_check(*this, kPagerFrames * i, kPagerFrames * (i + 1), true);
        m_pager->finish();
    }
}

TEST_F(PagerTests, SequentialPageUsage)
{
    ASSERT_OK(m_pager->start_reader());
    ASSERT_OK(m_pager->start_writer());
    write_pages(*this, 0, kManyPages);
    write_pages(*this, 42, kManyPages);
    ASSERT_OK(m_pager->commit());
    read_and_check(*this, 42, kManyPages);
    m_pager->finish();
}

TEST_F(PagerTests, ReverseSequentialPageUsage)
{
    ASSERT_OK(m_pager->start_reader());
    ASSERT_OK(m_pager->start_writer());
    write_pages(*this, 0, kManyPages);

    for (std::size_t i = 0; i < kManyPages; ++i) {
        const auto j = kManyPages - i - 1;
        acquire_write_release(Id(j + 1), make_key(j + 42));
    }
    ASSERT_OK(m_pager->commit());
    read_and_check(*this, 42, kManyPages);
    m_pager->finish();
}

TEST_F(PagerTests, RandomPageUsage)
{
    std::vector<unsigned> is(kManyPages);
    std::iota(begin(is), end(is), 0);
    std::default_random_engine rng(42);
    std::shuffle(begin(is), end(is), rng);

    ASSERT_OK(m_pager->start_reader());
    ASSERT_OK(m_pager->start_writer());
    write_pages(*this, 0, is.size());
    for (auto i : is) {
        acquire_write_release(Id(i + 1), make_key(i + 42));
    }
    ASSERT_OK(m_pager->commit());
    read_and_check(*this, 42, is.size());
    m_pager->finish();
}

TEST_F(PagerTests, OnlyWritesBackCommittedWalFrames)
{
    ASSERT_OK(m_pager->start_reader());
    ASSERT_OK(m_pager->start_writer());
    write_pages(*this, 42, kManyPages);
    ASSERT_OK(m_pager->commit());
    m_pager->finish();

    // Modify the first kSomePages frames, then roll back the changes.
    ASSERT_OK(m_pager->start_reader());
    ASSERT_OK(m_pager->start_writer());
    write_pages(*this, 0, kSomePages);
    m_pager->rollback();
    m_pager->finish();

    ASSERT_OK(m_pager->checkpoint(true));

    ASSERT_OK(m_pager->start_reader());
    read_and_check(*this, 42, kManyPages);
    m_pager->finish();
}

TEST_F(PagerTests, TransactionBehavior)
{
    // Only able to start a write transaction once. The second call is a NOOP.
    ASSERT_OK(m_pager->start_reader());
    ASSERT_OK(m_pager->start_writer());
    ASSERT_EQ(m_pager->mode(), Pager::kWrite);
    ASSERT_OK(m_pager->start_writer());
    ASSERT_EQ(m_pager->mode(), Pager::kWrite);

    // Empty transactions are OK.
    ASSERT_OK(m_pager->commit());

    // commit() doesn't end the transaction. finish() must be called.
    ASSERT_EQ(m_pager->mode(), Pager::kWrite);
    m_pager->finish();
    ASSERT_EQ(m_pager->mode(), Pager::kOpen);

    ASSERT_OK(m_pager->start_reader());
    ASSERT_OK(m_pager->start_writer());
    m_pager->rollback();
    m_pager->finish();

    // Only able to start a read transaction once. Second call is a NOOP.
    ASSERT_OK(m_pager->start_reader());
    ASSERT_EQ(m_pager->mode(), Pager::kRead);
    ASSERT_OK(m_pager->start_reader());
    ASSERT_EQ(m_pager->mode(), Pager::kRead);
}

TEST_F(PagerTests, AcquirePastEOF)
{
    // Create "kManyPages" pages.
    ASSERT_OK(m_pager->start_reader());
    ASSERT_OK(m_pager->start_writer());
    write_pages(*this, 0, kManyPages);
    ASSERT_OK(m_pager->commit());
    m_pager->finish();

    // ID of a page that is way past the logical end of the DB file (the physical
    // size is still 0, but conceptually, there are kManyPages pages in existence).
    const auto kOutOfBounds = kManyPages * 10;

    Page page;
    ASSERT_OK(m_pager->start_reader());
    ASSERT_OK(m_pager->start_writer());
    ASSERT_OK(m_pager->acquire(Id(kOutOfBounds), page));
    ASSERT_EQ(page.id(), Id(kOutOfBounds));

    // Since this is a new page, it must be upgraded. Otherwise, it won't ever be
    // written to the WAL, and there will be no indication that the DB size changed.
    // Usually, new pages are obtained by calling Pager::allocate(), but this should
    // work as well.
    m_pager->mark_dirty(page);
    m_pager->release(std::move(page));

    ASSERT_EQ(m_pager->page_count(), kOutOfBounds)
        << "DB page count was not updated";

    // Cause the out-of-bounds page to be evicted.
    write_pages(*this, 0, kManyPages);

    ASSERT_EQ(count_db_pages(), 1)
        << "file have 1 page: no checkpoint has occurred";

    ASSERT_OK(m_pager->commit());
    m_pager->finish();
    ASSERT_OK(m_pager->checkpoint(true));
    ASSERT_EQ(m_pager->page_count(), kOutOfBounds);
    ASSERT_EQ(count_db_pages(), kOutOfBounds);

    // Intervening pages should be usable now. They are not in the WAL, so they must
    // be read from the DB file, modified in memory, written back to the WAL, then
    // read out of the WAL again.
    ASSERT_OK(m_pager->start_reader());
    ASSERT_OK(m_pager->start_writer());
    write_pages(*this, 42, kOutOfBounds);
    ASSERT_OK(m_pager->commit());
    read_and_check(*this, 42, kOutOfBounds);
    m_pager->finish();
}

TEST_F(PagerTests, FreelistUsage)
{
    ASSERT_OK(m_pager->start_reader());
    ASSERT_OK(m_pager->start_writer());
    ASSERT_OK(create_freelist_pages(kSomePages * 2));
    write_pages(*this, 123, kSomePages * 2);
    ASSERT_OK(m_pager->commit());
    read_and_check(*this, 123, kSomePages * 2);
    m_pager->finish();

    ASSERT_OK(m_pager->start_reader());
    ASSERT_OK(m_pager->start_writer());
    write_pages(*this, 456, kSomePages);
    m_pager->rollback();
    read_and_check(*this, 123, kSomePages * 2);
    m_pager->finish();

    ASSERT_OK(m_pager->checkpoint(true));
    ASSERT_OK(m_pager->start_reader());
    read_and_check(*this, 123, kSomePages * 2);
    read_and_check(*this, 123, kSomePages * 2, true);
    m_pager->finish();
}

#ifndef NDEBUG
TEST_F(PagerTests, InvalidModeDeathTest)
{
    ASSERT_EQ(m_pager->mode(), Pager::kOpen);
    ASSERT_DEATH((void)m_pager->commit(), kExpectationMatcher);
    ASSERT_DEATH((void)m_pager->rollback(), kExpectationMatcher);

    m_pager->set_status(Status::io_error("I/O error"));
    ASSERT_EQ(m_pager->mode(), Pager::kError);
    ASSERT_DEATH((void)m_pager->start_writer(), kExpectationMatcher);
    ASSERT_DEATH((void)m_pager->checkpoint(true), kExpectationMatcher);
}

TEST_F(PagerTests, DoubleFreeDeathTest)
{
    ASSERT_OK(m_pager->start_reader());
    ASSERT_OK(m_pager->start_writer());
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
                ASSERT_DEATH(m_pager->release(std::move(page)), kExpectationMatcher);
            } else {
                ASSERT_DEATH((void)m_pager->destroy(std::move(page)), kExpectationMatcher);
            }
        }
    }
    ASSERT_OK(m_pager->commit());
}

TEST_F(PagerTests, DestroyPointerMapPageDeathTest)
{
    ASSERT_OK(m_pager->start_reader());
    ASSERT_OK(m_pager->start_writer());
    Page page;
    ASSERT_OK(m_pager->acquire(Id(2), page));
    ASSERT_DEATH((void)m_pager->destroy(std::move(page)), kExpectationMatcher);
    ASSERT_OK(m_pager->commit());
}
#endif // NDEBUG

class TruncationTests : public PagerTests
{
public:
    static constexpr std::size_t kInitialPageCount = 500;

    ~TruncationTests() override = default;

    auto SetUp() -> void override
    {
        PagerTests::SetUp();
        ASSERT_OK(m_pager->start_reader());
        ASSERT_OK(m_pager->start_writer());
        write_pages(*this, 0, kInitialPageCount);
    }

    auto TearDown() -> void override
    {
        m_pager->finish();
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

    ASSERT_OK(m_pager->commit());
    m_pager->finish();

    // When the WAL is enabled, the DB file is not written until checkpoint.
    ASSERT_OK(env->file_size(kDBFilename, file_size));
    ASSERT_EQ(file_size, kPageSize);

    // If there are still cached pages past the truncation position, they will be
    // written back to disk here, causing the file size to be incorrect.
    ASSERT_OK(m_pager->checkpoint(true));

    ASSERT_OK(env->file_size(kDBFilename, file_size));
    ASSERT_EQ(file_size, kInitialPageCount * kPageSize / 2);
}

#ifndef NDEBUG
TEST_F(TruncationTests, PurgeRootDeathTest)
{
    ASSERT_DEATH(m_pager->set_page_count(0), kExpectationMatcher);
}
#endif // NDEBUG

class RandomDirtyListBuilder
{
public:
    explicit RandomDirtyListBuilder()
        : m_random(kPageSize * 256),
          m_rng(42)
    {
    }

    // NOTE: Invalidates dirty lists previously obtained through this method. The pgno vector must not
    //       have any duplicate page numbers.
    auto build(const std::vector<U32> &pgno, std::vector<PageRef> &out) -> void
    {
        CALICODB_EXPECT_FALSE(pgno.empty());
        out.resize(pgno.size());

        for (std::size_t i = 0; i < out.size(); ++i) {
            while (pgno[i] * kPageSize > m_pages.size()) {
                m_pages.append(std::string(kPageSize, '\0'));
            }
            std::memcpy(
                m_pages.data() + (pgno[i] - 1) * kPageSize,
                m_random.Generate(kPageSize).data(),
                kPageSize);

            out[i].page_id = Id(pgno[i]);
            if (i != 0) {
                out[i].prev = &out[i - 1];
            }
            if (i < out.size() - 1) {
                out[i].next = &out[i + 1];
            }
        }
        for (auto &d : out) {
            d.page = m_pages.data() + d.page_id.as_index() * kPageSize;
        }
    }
    auto build(std::size_t num_pages, std::vector<PageRef> &out) -> void
    {
        std::vector<U32> pgno(num_pages);
        std::iota(begin(pgno), end(pgno), 1);
        std::shuffle(begin(pgno), end(pgno), m_rng);
        build(pgno, out);
    }

    [[nodiscard]] auto data() const -> Slice
    {
        return m_pages;
    }

private:
    std::string m_pages;
    tools::RandomGenerator m_random;
    std::default_random_engine m_rng;
};

class WalTestBase
{
public:
    explicit WalTestBase(Env &env)
    {
        EXPECT_OK(env.new_file(kDBFilename, Env::kCreate, m_db));

        m_param = {
            kWalFilename,
            kDBFilename,
            &env,
            m_db,
            nullptr,
            nullptr,
            false,
        };
        EXPECT_OK(Wal::open(m_param, m_wal));
    }

    virtual ~WalTestBase()
    {
        close();
    }

    auto close() -> void
    {
        ASSERT_OK(m_db->file_lock(kLockShared));
        std::size_t db_size;
        ASSERT_OK(m_wal->close(db_size));
        delete m_wal;
        delete m_db;
        m_wal = nullptr;
        m_db = nullptr;
    }

    auto wal() -> Wal &
    {
        return *m_wal;
    }
    auto db() -> File &
    {
        return *m_db;
    }

protected:
    Wal *m_wal = nullptr;
    File *m_db = nullptr;
    Wal::Parameters m_param;
};

class WalTests
    : public EnvTestHarness<PosixEnv>,
      public WalTestBase,
      public testing::Test
{
protected:
    explicit WalTests()
        : WalTestBase(env())
    {
    }

    ~WalTests() override = default;

    auto write_pages(std::size_t n) -> void
    {
        std::vector<PageRef> dirty;
        m_builder.build(n, dirty);
        ASSERT_OK(m_wal->write(&dirty.front(), n));
    }

    auto start_reader_routine(std::size_t r, std::size_t n, Status::Code outcome) -> void
    {
        bool unused;
        ASSERT_OK(m_db->file_lock(kLockShared));

        std::atomic<bool> flag(false);
        std::thread thread([this, &flag, r, n] {
            File *db;
            ASSERT_OK(m_env->new_file(kDBFilename, Env::kReadWrite, db));

            volatile void *ptr;
            ASSERT_OK(db->shm_map(0, true, ptr));

            ASSERT_OK(db->shm_lock(r, n, kShmLock | kShmWriter));
            flag.store(true, std::memory_order_release);
            while (flag.load(std::memory_order_acquire)) {
            }
            ASSERT_OK(db->shm_lock(r, n, kShmUnlock | kShmWriter));
            db->shm_unmap(true);
            delete db;
        });

        // Wait on the background thread to finish setting up.
        while (!flag.load(std::memory_order_acquire)) {
        }

        // There is nothing in the WAL, so this connection must take readmark 0 and get
        // pages from the database file. This is not possible, because the background
        // thread has the
        Status s;
        ASSERT_EQ((s = m_wal->start_reader(unused)).code(), outcome)
            << "expected " << int(outcome) << " but got " << int(s.code())
            << ": " << s.to_string();
        m_wal->finish_reader();

        flag.store(false, std::memory_order_release);
        thread.join();

        ASSERT_OK(m_wal->start_reader(unused));
        m_wal->finish_reader();
        m_db->file_unlock();
    }

    RandomDirtyListBuilder m_builder;
};

TEST_F(WalTests, EmptyWal)
{
    ASSERT_OK(m_db->file_lock(kLockShared));

    bool changed;
    ASSERT_OK(m_wal->start_reader(changed));
    ASSERT_TRUE(changed);

    ASSERT_OK(m_wal->checkpoint(true));

    char page[kPageSize];
    auto *ptr = page;
    ASSERT_OK(m_wal->read(Id(1), ptr));
    ASSERT_FALSE(ptr);
    ASSERT_OK(m_wal->read(Id(2), ptr));
    ASSERT_FALSE(ptr);
    ASSERT_OK(m_wal->read(Id(3), ptr));
    ASSERT_FALSE(ptr);

    ASSERT_OK(m_wal->checkpoint(true));

    std::size_t db_size;
    ASSERT_OK(m_env->file_size(kDBFilename, db_size));
    ASSERT_EQ(0, db_size);
}

TEST_F(WalTests, RecoversIndex)
{
    ASSERT_OK(m_db->file_lock(kLockShared));

    bool changed;
    ASSERT_OK(m_wal->start_reader(changed));
    ASSERT_TRUE(changed);

    ASSERT_OK(m_wal->start_writer());
    write_pages(100);
    m_wal->finish_writer();
    m_wal->finish_reader();

    // Writing frames from this connection won't cause a change to be reported.
    ASSERT_OK(m_wal->start_reader(changed));
    ASSERT_FALSE(changed);
    m_wal->finish_reader();

    // Make the 2 WAL index headers not equal.
    volatile void *void_ptr;
    ASSERT_OK(m_db->shm_map(0, false, void_ptr));
    auto *ptr = reinterpret_cast<volatile char *>(void_ptr);
    ++ptr[0];

    // The index header was corrupted, so it had to be recovered. This is considered
    // a change.
    ASSERT_OK(m_wal->start_reader(changed));
    ASSERT_TRUE(changed);
    m_wal->finish_reader();

    // Invalidate the checksums.
    ++ptr[0];
    ++ptr[48];

    ASSERT_OK(m_wal->start_reader(changed));
    ASSERT_TRUE(changed);

    char pages[kPageSize * 100];
    for (std::size_t i = 0; i < 100; ++i) {
        auto *p = pages + kPageSize * i;
        ASSERT_OK(m_wal->read(Id(i + 1), p));
        ASSERT_TRUE(p);
    }
    ASSERT_EQ(Slice(pages, m_builder.data().size()), m_builder.data());
    m_wal->finish_reader();
}

TEST_F(WalTests, FindsNonzeroReadmark)
{
    // No frames in the WAL, so connection seeks readmark 0. Readmark 0 is already
    // locked with a writer lock by the background thread, so the connection must
    // use readmark 1.
    start_reader_routine(3, 1, Status::kOK);
}

TEST_F(WalTests, ReportsProtocolError)
{
    // Here, readmarks 0 and 1 already have writer locks. The connection won't keep
    // looking for another readmark, it just returns busy, as it appears that index
    // recovery is running.
    start_reader_routine(3, 2, Status::kIOError);
}

TEST_F(WalTests, StartWriter)
{
    ASSERT_OK(m_db->file_lock(kLockShared));

    bool changed;
    ASSERT_OK(m_wal->start_reader(changed));
    ASSERT_TRUE(changed);

    // Change the header while this connection has a read transaction, before it
    // starts the write transaction.
    volatile void *ptr;
    ASSERT_OK(m_db->shm_map(0, false, ptr));
    ++reinterpret_cast<volatile char *>(ptr)[0];
    // Writer should report a busy status, since it looks like another writer is
    // active right now, and it may block for a long time.
    ASSERT_TRUE(m_wal->start_writer().is_busy());
    m_wal->finish_reader();

    ASSERT_OK(m_wal->start_reader(changed));
    ASSERT_TRUE(changed);
    ASSERT_OK(m_wal->start_writer());
    // Write transaction already started, additional calls are NOOPs.
    ASSERT_OK(m_wal->start_writer());

    m_wal->finish_writer();
    m_wal->finish_reader();
}

class WalParamTests
    : public EnvTestHarness<PosixEnv>,
      public WalTestBase,
      public testing::TestWithParam<
          std::tuple<std::size_t, std::size_t, std::size_t>>
{
protected:
    explicit WalParamTests()
        : WalTestBase(env()),
          m_rng(42)
    {
        EXPECT_OK(env().new_file(kDBFilename, Env::kCreate, m_fake_file));
        m_fake_param = Wal::Parameters{
            "testdb-wal",
            "testdb-db",
            &env(),
            m_fake_file,
        };
        m_fake = new tools::FakeWal(m_fake_param);
    }

    ~WalParamTests() override
    {
        delete m_fake;
        delete m_fake_file;
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
            EXPECT_OK(m_wal->write(&dirty.front(), db_size));
            EXPECT_OK(m_fake->write(&dirty.front(), db_size));
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
        ASSERT_OK(m_db->file_lock(kLockShared));
        std::size_t db_size;
        ASSERT_OK(m_wal->close(db_size));
        delete m_wal;
        m_wal = nullptr;

        // These tests use a single connection. This means that, since Wal::close()
        // returned OK, the whole WAL was written back to the database and the WAL
        // unlinked.
        m_db->file_unlock();
        ASSERT_OK(Wal::open(m_param, m_wal));

        delete m_fake;
        m_fake = new tools::FakeWal(m_fake_param);
    }

    auto run_and_validate_checkpoint(bool save_state = true) -> void
    {
        File *real, *fake;
        ASSERT_OK(env().new_file("realdb", Env::kCreate, real));
        ASSERT_OK(env().new_file("fakedb", Env::kCreate, fake));
        ASSERT_OK(m_wal->checkpoint(true));
        ASSERT_OK(m_fake->checkpoint(true));

        std::size_t file_size;
        ASSERT_OK(env().file_size("fakedb", file_size));

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
            bool changed;
            ASSERT_OK(m_wal->start_reader(changed));
            ASSERT_OK(m_wal->start_writer());
            write_records(m_pages_per_iter, m_commit_interval != 0);
            read_and_check_records();
            m_wal->finish_writer();
            m_wal->finish_reader();
        }
    }

    auto test_operations(bool reopen) -> void
    {
        for (std::size_t iteration = 0; iteration < m_iterations; ++iteration) {
            bool changed;
            ASSERT_OK(m_wal->start_reader(changed));
            ASSERT_OK(m_wal->start_writer());

            const auto is_commit = m_commit_interval && iteration % m_commit_interval == m_commit_interval - 1;
            write_records(m_pages_per_iter, is_commit);
            if (!is_commit) {
                m_wal->rollback();
                m_fake->rollback();
            }
            m_wal->finish_writer();
            m_wal->finish_reader();

            if (reopen) {
                reopen_wals();
            }
            ASSERT_OK(m_wal->start_reader(changed));
            read_and_check_records();
            m_wal->finish_reader();

            run_and_validate_checkpoint(is_commit);
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
    File *m_fake_file = nullptr;
};

TEST_P(WalParamTests, WriteAndReadBack)
{
    test_write_and_read_back();
}

TEST_P(WalParamTests, Operations1)
{
    test_operations(false);
}

TEST_P(WalParamTests, Operations2)
{
    test_operations(true);
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

} // namespace calicodb
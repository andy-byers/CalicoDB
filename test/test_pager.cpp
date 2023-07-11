// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "common.h"
#include "encoding.h"
#include "fake_env.h"
#include "freelist.h"
#include "pager.h"
#include "temp.h"
#include "test.h"
#include "wal.h"

namespace calicodb::test
{

class BufmgrTests : public testing::Test
{
public:
    explicit BufmgrTests()
        : mgr(kMinFrameCount, m_stat)
    {
    }

    ~BufmgrTests() override = default;

    Stat m_stat;
    Bufmgr mgr;
};

TEST_F(BufmgrTests, EmptyBehavior)
{
    ASSERT_EQ(mgr.occupied(), 0);
    ASSERT_EQ(mgr.occupied(), 0);
    ASSERT_EQ(mgr.get(Id(2)), nullptr);
    ASSERT_EQ(mgr.next_victim(), nullptr);
}

TEST_F(BufmgrTests, OldestReferenceIsEvictedFirst)
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

TEST_F(BufmgrTests, ReplacementPolicyIgnoresQuery)
{
    (void)mgr.alloc(Id(3));
    (void)mgr.alloc(Id(2));

    (void)mgr.query(Id(3));

    ASSERT_EQ(mgr.next_victim()->page_id, Id(3));
    mgr.erase(mgr.next_victim()->page_id);
    ASSERT_EQ(mgr.next_victim()->page_id, Id(2));
    mgr.erase(mgr.next_victim()->page_id);
}

TEST_F(BufmgrTests, RefcountsAreConsideredDuringEviction)
{
    (void)mgr.alloc(Id(3));
    (void)mgr.alloc(Id(2));

    mgr.query(Id(3))->refcount = 2;

    ASSERT_EQ(mgr.next_victim()->page_id, Id(2));
    mgr.erase(mgr.next_victim()->page_id);
    ASSERT_EQ(mgr.next_victim(), nullptr);
}

class PagerTests : public testing::Test
{
protected:
    static constexpr std::size_t kManyPages = kMinFrameCount * 100;

    Env *m_env;
    Pager *m_pager = nullptr;
    File *m_file = nullptr;
    Status m_status;
    Stat m_stat;

    explicit PagerTests()
        : m_env(new FakeEnv())
    {
    }

    ~PagerTests() override
    {
        close();
        delete m_file;
        delete m_env;
    }

    auto SetUp() -> void override
    {
        reopen();
    }

    auto reopen(Options::LockMode lock_mode = Options::kLockNormal) -> bool
    {
        close();
        (void)m_env->remove_file("db");
        (void)m_env->remove_file("wal");
        delete m_file;
        m_file = nullptr;

        auto s = m_env->new_file(
            "db",
            Env::kCreate | Env::kReadWrite,
            m_file);
        if (s.is_ok()) {
            const Pager::Parameters param = {
                "db",
                "wal",
                m_file,
                m_env,
                nullptr,
                &m_status,
                &m_stat,
                nullptr,
                kMinFrameCount,
                Options::kSyncNormal,
                lock_mode,
                true,
            };
            s = Pager::open(param, m_pager);
        }
        if (!s.is_ok()) {
            ADD_FAILURE() << s.to_string();
            delete m_file;
            return false;
        }
        return true;
    }
    auto close() -> void
    {
        delete m_pager;
        m_pager = nullptr;
    }

    std::vector<Id> m_page_ids;
    auto allocate_page(PageRef *&page_out) -> Id
    {
        if (m_pager->page_count() == 0) {
            m_pager->initialize_root();
        }

        EXPECT_OK(m_pager->allocate(page_out));
        if (m_page_ids.empty() || m_page_ids.back() < page_out->page_id) {
            m_page_ids.emplace_back(page_out->page_id);
        }
        return page_out->page_id;
    }
    auto allocate_page() -> Id
    {
        PageRef *page;
        const auto id = allocate_page(page);
        m_pager->release(page);
        return id;
    }
    auto alter_page(PageRef &page) -> void
    {
        m_pager->mark_dirty(page);
        const auto value = get_u32(page.page + kPageSize - 4);
        put_u32(page.page + kPageSize - 4, value + 1);
    }
    auto alter_page(std::size_t index) -> void
    {
        PageRef *page;
        EXPECT_OK(m_pager->acquire(m_page_ids.at(index), page));
        alter_page(*page);
        m_pager->release(page);
    }
    auto read_page(const PageRef &page) -> U32
    {
        return get_u32(page.page + kPageSize - 4);
    }
    auto read_page(std::size_t index) -> U32
    {
        if (m_page_ids.at(index).value > m_pager->page_count()) {
            return 0;
        }
        PageRef *page;
        EXPECT_OK(m_pager->acquire(m_page_ids.at(index), page));
        const auto value = read_page(*page);
        m_pager->release(page);
        return value;
    }

    template <class Fn>
    auto pager_view(const Fn &fn) -> void
    {
        ASSERT_OK(m_pager->start_reader());
        fn();
        m_pager->finish();
    }
    template <class Fn>
    auto pager_update(const Fn &fn) -> void
    {
        ASSERT_OK(m_pager->start_reader());
        ASSERT_OK(m_pager->start_writer());
        fn();
        m_pager->finish();
    }
};

TEST_F(PagerTests, AllocatePage)
{
    pager_update([this] {
        ASSERT_EQ(0, m_pager->page_count());
        // Pager layer skips pointer map pages, and the root already exists.
        ASSERT_EQ(Id(3), allocate_page());
        ASSERT_EQ(Id(4), allocate_page());
        ASSERT_EQ(Id(5), allocate_page());
        ASSERT_EQ(5, m_pager->page_count());
    });
}

TEST_F(PagerTests, AcquirePage)
{
    pager_update([this] {
        allocate_page();
        ASSERT_EQ(3, m_pager->page_count());

        PageRef *page;
        for (U32 n = 1; n < 5; ++n) {
            ASSERT_OK(m_pager->acquire(Id(n), page));
            m_pager->release(page);
            // Pager::acquire() can increase the database size by 1.
            ASSERT_EQ(n <= 3 ? 3 : n, m_pager->page_count());
        }
        // Attempt to skip page 5.
        ASSERT_TRUE(m_pager->acquire(Id(6), page).is_corruption());
    });
}

TEST_F(PagerTests, NOOP)
{
    pager_update([] {});
    pager_view([] {});
    ASSERT_OK(m_pager->checkpoint(true));
    ASSERT_OK(m_pager->checkpoint(false));
    m_pager->set_page_count(0);
    m_pager->set_status(Status::ok());

    std::size_t file_size;
    // Database size is 0 before the first checkpoint.
    ASSERT_OK(m_env->file_size("db", file_size));
    ASSERT_EQ(file_size, 0);
}

TEST_F(PagerTests, Commit)
{
    for (int iteration = 0; iteration < 6; ++iteration) {
        reopen(iteration < 3 ? Options::kLockNormal : Options::kLockExclusive);
        pager_update([this] {
            for (std::size_t i = 0; i < kManyPages; ++i) {
                PageRef *page;
                allocate_page(page);
                alter_page(*page);
                m_pager->release(
                    page,
                    // kNoCache should be ignored, since the page is dirty.
                    Pager::kNoCache);
            }
            for (std::size_t i = 0; i < kManyPages; ++i) {
                PageRef *page;
                ASSERT_OK(m_pager->acquire(m_page_ids[i], page));
                alter_page(*page);
                m_pager->release(
                    page,
                    // Drop every other update.
                    i & 1 ? Pager::kNoCache : Pager::kDiscard);
            }
            ASSERT_OK(m_pager->commit());
        });
        if (iteration % 3 > 0) {
            // Make sure we actually have all the data we need in the WAL. The root page is
            // not in the WAL, but it is blank anyway.
            ASSERT_OK(m_env->resize_file("db", 0));
            // Transfer the lost pages back.
            ASSERT_OK(m_pager->checkpoint(iteration % 3 == 1));
            // Everything should be back in the database file. The next reader shouldn't read
            // any pages from the WAL.
            ASSERT_OK(m_env->resize_file("wal", 0));
        }
        pager_view([this] {
            for (std::size_t i = 0; i < kManyPages; ++i) {
                const auto value = read_page(i);
                ASSERT_EQ(1 + (i & 1), value);
            }
        });
    }
}

TEST_F(PagerTests, Rollback)
{
    for (int iteration = 0; iteration < 6; ++iteration) {
        reopen(iteration < 3 ? Options::kLockNormal : Options::kLockExclusive);
        std::size_t page_count = 0;
        pager_update([this, &page_count] {
            for (std::size_t i = 0; i < kManyPages; ++i) {
                PageRef *page;
                allocate_page(page);
                alter_page(*page);
                m_pager->release(page);

                if (i == kManyPages / 2) {
                    ASSERT_OK(m_pager->commit());
                    page_count = m_pager->page_count();
                }
            }
        });
        if (iteration % 3 > 0) {
            ASSERT_OK(m_env->resize_file("db", 0));
            ASSERT_OK(m_pager->checkpoint(iteration % 3 == 1));
            ASSERT_OK(m_env->resize_file("wal", 0));
        }
        pager_view([this, page_count] {
            ASSERT_EQ(m_pager->page_count(), page_count);
            for (std::size_t i = 0; i < kManyPages; ++i) {
                ASSERT_EQ(i <= kManyPages / 2, read_page(i));
            }
        });
    }
}

TEST_F(PagerTests, Truncation)
{
    pager_update([this] {
        for (std::size_t i = 0; i < kManyPages; ++i) {
            allocate_page();
        }
        for (std::size_t i = 0; i < kManyPages; ++i) {
            alter_page(i);
        }
        m_pager->set_page_count(m_page_ids.at(kManyPages / 2).value);
        ASSERT_OK(m_pager->commit());
    });

    ASSERT_OK(m_pager->checkpoint(true));

    std::size_t file_size;
    ASSERT_OK(m_env->file_size("db", file_size));
    ASSERT_EQ(file_size, kPageSize * m_page_ids.at(kManyPages / 2).value);

    pager_view([this] {
        for (std::size_t i = 0; i < kManyPages; ++i) {
            EXPECT_EQ(i <= kManyPages / 2, read_page(i)) << i;
        }
    });
}

TEST_F(PagerTests, Freelist)
{
    pager_update([this] {
        PageRef *page;
        // Fill up several trunk pages.
        for (std::size_t i = 0; i < kPageSize; ++i) {
            allocate_page(page);
            m_pager->release(page);
        }
        for (std::size_t i = 0; i < kPageSize; ++i) {
            ASSERT_OK(m_pager->acquire(m_page_ids.at(i), page));
            ASSERT_OK(m_pager->destroy(page));
        }
        ASSERT_OK(m_pager->commit());
    });
    pager_update([this] {
        ASSERT_EQ(m_page_ids.back().value, m_pager->page_count());
        PageRef *page;
        for (std::size_t i = 0; i < kPageSize; ++i) {
            allocate_page(page);
            m_pager->release(page);
        }
        ASSERT_OK(m_pager->commit());
    });
    pager_view([this] {
        ASSERT_EQ(m_page_ids.back().value, m_pager->page_count());
    });
}

TEST_F(PagerTests, FreelistCorruption)
{
    pager_update([this] {
        PageRef *page;
        allocate_page(page);
        page->page_id.value = m_pager->page_count() + 1;
        ASSERT_NOK(m_pager->destroy(page));
        auto *root = &m_pager->get_root();
        ASSERT_NOK(m_pager->destroy(root));
    });
}

TEST_F(PagerTests, ReportsOutOfRangePages)
{
    pager_update([this] {
        PageRef *page;
        ASSERT_NOK(m_pager->acquire(Id(100), page));
    });
}

#ifndef NDEBUG
TEST_F(PagerTests, DeathTest)
{
    ASSERT_EQ(m_pager->mode(), Pager::kOpen);
    ASSERT_DEATH((void)m_pager->commit(), "expect");

    ASSERT_DEATH((void)m_pager->start_writer(), "expect");
    ASSERT_OK(m_pager->start_reader());
    ASSERT_DEATH((void)m_pager->checkpoint(true), "expect");
}
#endif // NDEBUG

class HashIndexTestBase
{
protected:
    explicit HashIndexTestBase()
    {
        EXPECT_OK(m_env.new_file("shm", Env::kCreate, m_shm));
        m_index = new HashIndex(m_header, m_shm);
    }

    ~HashIndexTestBase()
    {
        delete m_shm;
        delete m_index;
    }

    auto append(U32 key)
    {
        ASSERT_OK(m_index->assign(key, ++m_header.max_frame));
    }

    FakeEnv m_env;
    File *m_shm = nullptr;
    HashIndexHdr m_header = {};
    HashIndex *m_index = nullptr;
};

class HashIndexTests
    : public HashIndexTestBase,
      public testing::Test
{
protected:
    ~HashIndexTests() override = default;
};

TEST_F(HashIndexTests, FirstSegmentFrameBounds)
{
    append(1);
    append(2);
    append(3);
    append(4);

    const U32 min_frame(2);
    m_header.max_frame = 3;

    U32 value;
    ASSERT_OK(m_index->lookup(1, min_frame, value));
    ASSERT_FALSE(value);
    ASSERT_OK(m_index->lookup(2, min_frame, value));
    ASSERT_EQ(value, 2);
    ASSERT_OK(m_index->lookup(3, min_frame, value));
    ASSERT_EQ(value, 3);
    ASSERT_OK(m_index->lookup(4, min_frame, value));
    ASSERT_FALSE(value);
}

TEST_F(HashIndexTests, SecondSegmentFrameBounds)
{
    for (U32 i = 1; i <= 6'000; ++i) {
        append(i);
    }

    const U32 min_frame = 5'000;
    m_header.max_frame = 5'500;

    U32 value;
    ASSERT_OK(m_index->lookup(1, min_frame, value));
    ASSERT_FALSE(value);
    ASSERT_OK(m_index->lookup(4'999, min_frame, value));
    EXPECT_FALSE(value);
    ASSERT_OK(m_index->lookup(5'000, min_frame, value));
    ASSERT_EQ(value, 5'000);
    ASSERT_OK(m_index->lookup(5'500, min_frame, value));
    ASSERT_EQ(value, 5'500);
    ASSERT_OK(m_index->lookup(5'501, min_frame, value));
    ASSERT_FALSE(value);
    ASSERT_OK(m_index->lookup(10'000, min_frame, value));
    ASSERT_FALSE(value);
}

TEST_F(HashIndexTests, Cleanup)
{
    U32 value;
    append(1);
    append(2);
    append(3);
    append(4);

    // Performing cleanup when there are no valid frames is a NOOP. The next person to write the
    // WAL index will do so at frame 1, which automatically causes the WAL index to clear itself.
    m_header.max_frame = 0;
    m_index->cleanup();
    m_header.max_frame = 4;

    ASSERT_OK(m_index->lookup(1, 1, value));
    ASSERT_EQ(value, 1);
    ASSERT_OK(m_index->lookup(2, 1, value));
    ASSERT_EQ(value, 2);
    ASSERT_OK(m_index->lookup(3, 1, value));
    ASSERT_EQ(value, 3);
    ASSERT_OK(m_index->lookup(4, 1, value));
    ASSERT_EQ(value, 4);

    m_header.max_frame = 2;
    m_index->cleanup();
    m_header.max_frame = 4;

    ASSERT_OK(m_index->lookup(1, 1, value));
    ASSERT_EQ(value, 1);
    ASSERT_OK(m_index->lookup(2, 1, value));
    ASSERT_EQ(value, 2);
    ASSERT_OK(m_index->lookup(3, 1, value));
    ASSERT_FALSE(value);
    ASSERT_OK(m_index->lookup(4, 1, value));
    ASSERT_FALSE(value);
}

TEST_F(HashIndexTests, ReadsAndWrites)
{
    std::vector<U32> keys;
    // Write 2 full index buckets + a few extra entries.
    for (U32 i = 0; i < 4'096 * 2; ++i) {
        keys.emplace_back(i);
    }
    std::default_random_engine rng(42);
    std::shuffle(begin(keys), end(keys), rng);

    for (const auto &id : keys) {
        append(id);
    }

    const U32 lower = 1'234;
    m_header.max_frame = 5'000;

    U32 value = 1;
    for (const auto &key : keys) {
        ASSERT_EQ(m_index->fetch(value), key);
        U32 current;
        ASSERT_OK(m_index->lookup(key, lower, current));
        if (m_header.max_frame < value || value < lower) {
            ASSERT_FALSE(current);
        } else {
            ASSERT_EQ(current, value);
        }
        ++value;
    }
}

TEST_F(HashIndexTests, SimulateUsage)
{
    static constexpr std::size_t kNumTestFrames = 10'000;

    RandomGenerator random;
    std::map<U32, U32> simulated;

    for (std::size_t iteration = 0; iteration < 2; ++iteration) {
        U32 lower = 1;
        for (std::size_t frame = 1; frame <= kNumTestFrames; ++frame) {
            if (const auto r = random.Next(10); r == 0) {
                // Run a commit. The calls that validate the page-frame mapping below
                // will ignore frames below "lower". This is not exactly how the WAL works,
                // we actually use 3 index headers, 2 in the index, and 1 in memory. The
                // in-index header's max_frame is used as the position of the last commit.
                lower = m_header.max_frame + 1;
                simulated.clear();
            } else {
                // Perform a write, but only if the page does not already exist in a frame
                // in the range "lower" to "m_header.max_frame", inclusive.
                U32 value;
                const U32 key = static_cast<U32>(random.Next(1, kNumTestFrames));
                ASSERT_OK(m_index->lookup(key, lower, value));
                if (value < lower) {
                    append(key);
                    simulated.insert_or_assign(key, m_header.max_frame);
                }
            }
        }
        U32 result;
        for (const auto &[key, value] : simulated) {
            ASSERT_OK(m_index->lookup(key, lower, result));
            ASSERT_EQ(result, value);
        }
        // Reset the WAL index.
        m_header.max_frame = 0;
        simulated.clear();
    }
}

class HashIteratorTests
    : public HashIndexTestBase,
      public testing::Test
{
protected:
    ~HashIteratorTests() override = default;
};

#ifndef NDEBUG
TEST_F(HashIteratorTests, EmptyIndexDeathTest)
{
    HashIterator itr(*m_index);
    ASSERT_DEATH((void)itr.init(), "expect");
}
#endif // NDEBUG

class HashIteratorParamTests
    : public HashIndexTestBase,
      public testing::TestWithParam<std::tuple<std::size_t, std::size_t>>
{
protected:
    HashIteratorParamTests()
        : m_num_pages(std::get<1>(GetParam())),
          m_num_copies(std::get<0>(GetParam()))
    {
    }

    ~HashIteratorParamTests() override = default;

    auto test_reordering_and_deduplication()
    {
        m_header.max_frame = 0;
        m_index->cleanup();

        for (std::size_t d = 0; d < m_num_copies; ++d) {
            for (std::size_t i = 0; i < m_num_pages; ++i) {
                append(static_cast<U32>(m_num_pages - i));
            }
        }
        HashIterator itr(*m_index);
        ASSERT_OK(itr.init());
        HashIterator::Entry entry;

        for (std::size_t i = 0;; ++i) {
            if (itr.read(entry)) {
                // Keys (page IDs) are always read in order. Values (frame IDs) should be
                // the most-recent values set for the associated key.
                ASSERT_EQ(entry.key, i + 1);
                ASSERT_EQ(entry.value, m_num_pages * m_num_copies - i);
            } else {
                ASSERT_EQ(i, m_num_pages);
                break;
            }
        }
    }

    std::size_t m_num_pages = 0;
    std::size_t m_num_copies = 0;
};

TEST_P(HashIteratorParamTests, ReorderingAndDeduplication)
{
    test_reordering_and_deduplication();
}

INSTANTIATE_TEST_SUITE_P(
    HashIteratorParamTests,
    HashIteratorParamTests,
    ::testing::Values(
        std::make_tuple(1, 1),
        std::make_tuple(1, 2),
        std::make_tuple(1, 3),
        std::make_tuple(1, 10),
        std::make_tuple(1, 100),
        std::make_tuple(1, 10'000),
        std::make_tuple(1, 100'000),
        std::make_tuple(5, 1),
        std::make_tuple(5, 2),
        std::make_tuple(5, 3),
        std::make_tuple(5, 10),
        std::make_tuple(5, 100),
        std::make_tuple(5, 10'000),
        std::make_tuple(5, 100'000)));

} // namespace calicodb::test
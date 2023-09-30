// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "common.h"
#include "encoding.h"
#include "fake_env.h"
#include "pager.h"
#include "temp.h"
#include "test.h"
#include "unique_ptr.h"
#include "wal_internal.h"

namespace calicodb::test
{

// Buffer manager tests were adapted from LevelDB.
class BufmgrTests : public testing::Test
{
public:
    static constexpr uint32_t kCacheSize = 1'000;
    Dirtylist m_dirtylist;
    Stats m_stat;
    Bufmgr mgr;

    explicit BufmgrTests()
        : mgr(32, m_stat)
    {
    }

    ~BufmgrTests() override = default;

    auto SetUp() -> void override
    {
        ASSERT_EQ(mgr.reallocate(TEST_PAGE_SIZE), 0);
    }

    auto insert_and_reference(uint32_t key, uint32_t value) -> PageRef *
    {
        auto *ref = mgr.next_victim();
        if (ref == nullptr) {
            ref = mgr.allocate(TEST_PAGE_SIZE);
            EXPECT_NE(ref, nullptr);
        } else {
            if (ref->get_flag(PageRef::kDirty)) {
                m_dirtylist.remove(*ref);
            }
            mgr.erase(*ref);
        }
        if (ref) {
            ref->page_id.value = key;
            put_u32(ref->data, value);
            mgr.register_page(*ref);
            mgr.ref(*ref);
        } else {
            ADD_FAILURE() << "OOM when allocating a page reference";
        }
        return ref;
    }

    auto insert(uint32_t key, uint32_t value) -> void
    {
        if (auto *ref = insert_and_reference(key, value)) {
            mgr.unref(*ref);
        }
    }

    auto erase(uint32_t key) -> bool
    {
        if (auto *ref = mgr.query(Id(key))) {
            mgr.erase(*ref);
            return true;
        }
        return false;
    }

    auto lookup(uint32_t key) -> int
    {
        if (auto *ref = mgr.lookup(Id(key))) {
            return static_cast<int>(get_u32(ref->data));
        }
        return -1;
    }
};

TEST_F(BufmgrTests, HitAndMiss)
{
    ASSERT_EQ(-1, lookup(100));

    insert(100, 101);
    ASSERT_EQ(101, lookup(100));
    ASSERT_EQ(-1, lookup(200));
    ASSERT_EQ(-1, lookup(300));

    insert(200, 201);
    ASSERT_EQ(101, lookup(100));
    ASSERT_EQ(201, lookup(200));
    ASSERT_EQ(-1, lookup(300));
}

TEST_F(BufmgrTests, Erase)
{
    erase(200);

    insert(100, 101);
    insert(200, 201);
    erase(100);
    ASSERT_EQ(-1, lookup(100));
    ASSERT_EQ(201, lookup(200));

    erase(100);
    ASSERT_EQ(-1, lookup(100));
    ASSERT_EQ(201, lookup(200));
}

TEST_F(BufmgrTests, EvictionPolicy)
{
    insert(100, 101);
    insert(200, 201);
    insert(300, 301);
    auto *h = mgr.lookup(Id(300));
    ASSERT_NE(h, nullptr);
    mgr.ref(*h);

    // Frequently used entry must be kept around,
    // as must things that are still in use.
    for (uint32_t i = 0; i < kCacheSize + 100; i++) {
        insert(1000 + i, 2000 + i);
        ASSERT_EQ(2000 + i, lookup(1000 + i));
        ASSERT_EQ(101, lookup(100));
    }
    ASSERT_EQ(101, lookup(100));
    ASSERT_EQ(-1, lookup(200));
    ASSERT_EQ(301, lookup(300));
    mgr.unref(*h);
}

TEST_F(BufmgrTests, UseExceedsCacheSize)
{
    // Overfill the cache, keeping handles on all inserted entries.
    std::vector<PageRef *> h;
    for (uint32_t i = 0; i < kCacheSize + 100; i++) {
        h.push_back(insert_and_reference(1000 + i, 2000 + i));
    }

    // Check that all the entries can be found in the cache.
    for (uint32_t i = 0; i < h.size(); i++) {
        ASSERT_EQ(2000 + i, lookup(1000 + i));
    }

    for (auto *ref : h) {
        mgr.unref(*ref);
    }
}

#ifndef NDEBUG
TEST_F(BufmgrTests, DeathTests)
{
    auto *ref1 = insert_and_reference(2, 2);
    auto *ref2 = insert_and_reference(3, 2);
    ASSERT_DEATH(insert(2, 2), "");
    ASSERT_DEATH(insert(3, 3), "");
    mgr.unref(*ref1);
    mgr.unref(*ref2);
}
#endif // NDEBUG

class DirtylistTests : public BufmgrTests
{
public:
    auto add(uint32_t key) -> void
    {
        auto *ref = insert_and_reference(key, key);
        ASSERT_NE(ref, nullptr);
        m_dirtylist.add(*ref);
        mgr.unref(*ref);
    }

    auto remove(uint32_t key) -> void
    {
        auto *ref = mgr.lookup(Id(key));
        ASSERT_NE(ref, nullptr);
        remove(*ref);
    }
    auto remove(PageRef &ref) -> void
    {
        m_dirtylist.remove(ref);
    }

    // NOTE: This is destructive.
    auto sort_and_check() -> void
    {
        std::vector<uint32_t> pgno;
        auto *list = m_dirtylist.sort();
        for (auto *p = list; p; p = p->dirty) {
            pgno.emplace_back(p->get_page_ref()->page_id.value);
            p->get_page_ref()->clear_flag(PageRef::kDirty);
        }
        ASSERT_TRUE(std::is_sorted(begin(pgno), end(pgno)));
    }
};

TEST_F(DirtylistTests, AddAndRemove)
{
    add(2);
    add(3);
    add(4);

    remove(2);
    remove(3);
    remove(4);
}

TEST_F(DirtylistTests, SortSortedPages)
{
    for (uint32_t i = 0; i < 1'000; ++i) {
        add(i + 2);
        if (i % kMinFrameCount + 1 == kMinFrameCount) {
            sort_and_check();
        }
    }
}

TEST_F(DirtylistTests, SortUnsortedPages)
{
    std::default_random_engine rng(42);
    std::vector<uint32_t> pgno(1'000);
    std::iota(begin(pgno), end(pgno), 2);
    std::shuffle(begin(pgno), end(pgno), rng);
    for (uint32_t i = 0; i < pgno.size(); ++i) {
        add(pgno[i]);
        if (i % kMinFrameCount + 1 == kMinFrameCount) {
            sort_and_check();
        }
    }
}

#ifndef NDEBUG
TEST_F(DirtylistTests, DeathTest)
{
    // An empty dirtylist must not be sorted.
    ASSERT_DEATH(sort_and_check(), "");

    auto *ref = insert_and_reference(1, 1);
    ASSERT_DEATH(remove(*ref), "");
    mgr.unref(*ref);
}
#endif // NDEBUG

struct PagerContext {
    static const std::string db_name;
    static const std::string wal_name;
    UserPtr<File> file;
    ObjectPtr<Pager> pager;
    Status status;
    Stats stats;

    auto open(Env &env, bool exclusive = false) -> void
    {
        pager.reset();
        file.reset();

        ASSERT_OK(env.new_file(
            db_name.c_str(),
            Env::kCreate | Env::kReadWrite,
            file.ref()));
        const Pager::Parameters param = {
            db_name.c_str(),
            wal_name.c_str(),
            file.get(),
            &env,
            nullptr,
            nullptr,
            &status,
            &stats,
            nullptr,
            TEST_PAGE_SIZE,
            kMinFrameCount,
            Options::kSyncNormal,
            exclusive
                ? Options::kLockExclusive
                : Options::kLockNormal,
            true,
        };
        ASSERT_OK(Pager::open(param, pager.ref()));
        ASSERT_OK(file->file_lock(kFileShared));
    }

    template <class Fn>
    auto reader(const Fn &fn) -> void
    {
        bool changed;
        ASSERT_OK(pager->lock_reader(&changed));
        fn(*pager, *file, changed);
        pager->finish();
    }

    template <class Fn>
    auto writer(const Fn &fn) -> void
    {
        reader([&fn](auto &p, auto &f, auto) {
            ASSERT_OK(p.begin_writer());
            fn(p, f);
        });
    }
};

const std::string PagerContext::db_name = testing::TempDir() + "calicodb_pager_tests";
const std::string PagerContext::wal_name = PagerContext::db_name + kDefaultWalSuffix.to_string();

class PagerTests : public testing::Test
{
protected:
    static constexpr size_t kManyPages = kMinFrameCount * 100;

    const std::string m_db_name;
    const std::string m_wal_name;
    Env *m_env;
    UserPtr<File> m_wal_file;
    PagerContext m_ctx;

    explicit PagerTests()
        : m_db_name(testing::TempDir() + "calicodb_pager_tests"),
          m_wal_name(testing::TempDir() + "calicodb_pager_tests" + kDefaultWalSuffix.to_string()),
          m_env(new FakeEnv())
    {
    }

    ~PagerTests() override
    {
        close();
        delete m_env;
    }

    auto SetUp() -> void override
    {
        reopen();
    }

    auto open_wal_if_present() -> void
    {
        m_wal_file.reset();
        auto s = m_env->new_file(m_wal_name.c_str(), Env::kReadWrite, m_wal_file.ref());
        ASSERT_TRUE(s.is_ok() || s.is_not_found());
    }

    auto open_pager(Options::LockMode lock_mode) -> void
    {
        m_ctx.open(*m_env, lock_mode);
    }

    auto reopen(Options::LockMode lock_mode = Options::kLockNormal) -> void
    {
        close();
        (void)m_env->remove_file(m_db_name.c_str());
        (void)m_env->remove_file(m_wal_name.c_str());
        m_wal_file.reset();

        open_pager(lock_mode);
    }

    auto close() -> void
    {
        m_ctx.pager.reset();
    }

    std::vector<Id> m_page_ids;
    auto allocate_page(PageRef *&page_out) -> Id
    {
        EXPECT_OK(m_ctx.pager->allocate(page_out));
        if (m_page_ids.empty() || m_page_ids.back() < page_out->page_id) {
            m_page_ids.emplace_back(page_out->page_id);
        }
        std::memset(page_out->data, 0, TEST_PAGE_SIZE);
        return page_out->page_id;
    }
    auto allocate_page() -> Id
    {
        PageRef *page;
        const auto id = allocate_page(page);
        m_ctx.pager->release(page);
        return id;
    }
    auto alter_page(PageRef &page) -> void
    {
        m_ctx.pager->mark_dirty(page);
        const auto value = get_u32(page.data + TEST_PAGE_SIZE - 4);
        put_u32(page.data + TEST_PAGE_SIZE - 4, value + 1);
    }
    auto alter_page(size_t index) -> void
    {
        PageRef *page;
        EXPECT_OK(m_ctx.pager->acquire(m_page_ids.at(index), page));
        alter_page(*page);
        m_ctx.pager->release(page);
    }
    auto read_page(const PageRef &page) -> uint32_t
    {
        return get_u32(page.data + TEST_PAGE_SIZE - 4);
    }
    auto read_page(size_t index) -> uint32_t
    {
        EXPECT_LT(index, m_page_ids.size());
        if (m_page_ids.at(index).value > m_ctx.pager->page_count()) {
            return 0;
        }
        PageRef *page;
        EXPECT_OK(m_ctx.pager->acquire(m_page_ids.at(index), page));
        const auto value = read_page(*page);
        m_ctx.pager->release(page);
        return value;
    }
};

TEST_F(PagerTests, LockReader)
{
    bool changed;
    ASSERT_OK(m_ctx.pager->lock_reader(&changed));
    ASSERT_FALSE(changed);
    ASSERT_EQ(m_ctx.pager->page_count(), 0);
    m_ctx.pager->finish();

    open_wal_if_present();
    ASSERT_FALSE(m_wal_file);
}

TEST_F(PagerTests, BeginWriter)
{
    ASSERT_OK(m_ctx.pager->lock_reader(nullptr));
    ASSERT_OK(m_ctx.pager->begin_writer());
    ASSERT_EQ(m_ctx.pager->page_count(), 1);
    m_ctx.pager->finish();

    open_wal_if_present();
    ASSERT_TRUE(m_wal_file);

    bool changed;
    ASSERT_OK(m_ctx.pager->lock_reader(&changed));
    ASSERT_FALSE(changed);
    m_ctx.pager->finish();
}

TEST_F(PagerTests, AllocatePage)
{
    m_ctx.writer([this](auto &pager, auto &) {
        // Root already exists.
        ASSERT_EQ(Id(2), allocate_page());
        ASSERT_EQ(Id(3), allocate_page());
        ASSERT_EQ(Id(4), allocate_page());
        ASSERT_EQ(4, pager.page_count());
        ASSERT_TRUE(pager.assert_state());
    });
}

TEST_F(PagerTests, AcquirePage)
{
    m_ctx.writer([this](auto &pager, auto &) {
        allocate_page();
        allocate_page();
        allocate_page();
        ASSERT_EQ(4, pager.page_count());

        PageRef *page;
        for (uint32_t n = 1; n < 4; ++n) {
            ASSERT_OK(pager.acquire(Id(n), page));
            pager.release(page);
            ASSERT_EQ(4, pager.page_count());
        }
        // Attempt to skip page 4.
        ASSERT_TRUE(pager.acquire(Id(5), page).is_corruption());
    });
}

TEST_F(PagerTests, NOOP)
{
    m_ctx.writer([](auto &, auto &) {});
    m_ctx.reader([](auto &, auto &, auto) {});
    ASSERT_OK(m_ctx.pager->checkpoint(kCheckpointRestart, nullptr));
    ASSERT_OK(m_ctx.pager->checkpoint(kCheckpointFull, nullptr));
    ASSERT_OK(m_ctx.pager->checkpoint(kCheckpointPassive, nullptr));
    m_ctx.writer([](auto &pager, auto &) {
        pager.set_status(Status::ok());
    });

    uint64_t file_size;
    // Database size is 0 before the first checkpoint.
    ASSERT_OK(m_ctx.file->get_size(file_size));
    ASSERT_EQ(file_size, 0);
}

TEST_F(PagerTests, Commit)
{
    reopen(Options::kLockNormal);
    m_ctx.writer([this](auto &pager, auto &) {
        PageRef *page;
        allocate_page(page);
        alter_page(*page);
        pager.release(page);
        allocate_page(page);
        pager.release(page);
        ASSERT_OK(pager.commit());
    });
    m_ctx.reader([this](auto &, auto &, bool) {
        ASSERT_EQ(read_page(0), 1);
        ASSERT_EQ(read_page(1), 0);
    });
}

TEST_F(PagerTests, Commit2)
{
    for (int iteration = 0; iteration < 6; ++iteration) {
        reopen(iteration < 3 ? Options::kLockNormal : Options::kLockExclusive);
        m_ctx.writer([this](auto &pager, auto &) {
            // Alter each page.
            for (size_t i = 0; i < kManyPages; ++i) {
                PageRef *page;
                allocate_page(page);
                alter_page(*page);

                pager.release(
                    page,
                    // kNoCache should be ignored since the page is dirty.
                    Pager::kNoCache);
            }
            // Alter every other page, drop the rest.
            for (size_t i = 0; i < kManyPages; ++i) {
                PageRef *page;
                ASSERT_OK(pager.acquire(m_page_ids[i], page));
                alter_page(*page);
                // Discard even-numbered updates.
                pager.release(page, i & 1 ? Pager::kKeep : Pager::kDiscard);
            }
            ASSERT_OK(pager.commit());
            ASSERT_TRUE(pager.assert_state());
        });
        if (iteration % 3 > 0) {
            // Make sure we actually have all the data we need in the WAL. The root page is
            // not in the WAL, but it is blank anyway.
            ASSERT_OK(m_ctx.file->resize(0));
            // Transfer the lost pages back.
            ASSERT_OK(m_ctx.pager->checkpoint(iteration % 3 == 1
                                                  ? kCheckpointRestart
                                                  : kCheckpointPassive,
                                              nullptr));
            // Everything should be back in the database file. The next reader shouldn't read
            // any pages from the WAL.
            open_wal_if_present();
            ASSERT_OK(m_wal_file->resize(0));
        }
        m_ctx.reader([this](auto &, auto &, bool) {
            for (size_t i = 0; i < kManyPages; ++i) {
                ASSERT_EQ(read_page(i), 1 + (i & 1));
            }
        });
    }
}

TEST_F(PagerTests, Commit3)
{
    size_t target = 0;
    for (size_t iteration = 0; iteration < 3; ++iteration) {
        m_ctx.writer([this, &target](auto &pager, auto &) {
            for (size_t i = 0; i < kManyPages; ++i) {
                PageRef *page;
                allocate_page(page);
                alter_page(*page);
                pager.release(page);
                ++target;
            }
            ASSERT_OK(pager.commit());
        });
    }
    m_ctx.reader([this, target](auto &, auto &, auto) {
        for (size_t i = 0; i < target; ++i) {
            ASSERT_EQ(1, read_page(i));
        }
    });
}

TEST_F(PagerTests, Rollback)
{
    reopen(Options::kLockNormal);
    size_t page_count = 0;
    m_ctx.writer([this, &page_count](auto &pager, auto &) {
        for (size_t i = 0; i < kManyPages; ++i) {
            PageRef *page;
            allocate_page(page);
            alter_page(*page);
            pager.release(page);

            if (i == kManyPages / 2) {
                ASSERT_OK(pager.commit());
                page_count = pager.page_count();
            }
        }
    });
    m_ctx.reader([this, page_count](auto &pager, auto &, auto) {
        ASSERT_EQ(pager.page_count(), page_count);
        for (size_t i = 0; i < kManyPages; ++i) {
            ASSERT_EQ(i <= kManyPages / 2, read_page(i));
        }
    });
}

TEST_F(PagerTests, Rollback2)
{
    for (int iteration = 0; iteration < 6; ++iteration) {
        reopen(iteration < 3 ? Options::kLockNormal : Options::kLockExclusive);
        size_t page_count = 0;
        m_ctx.writer([this, &page_count](auto &pager, auto &) {
            for (size_t i = 0; i < kManyPages; ++i) {
                PageRef *page;
                allocate_page(page);
                alter_page(*page);
                pager.release(page);

                if (i == kManyPages / 2) {
                    ASSERT_OK(pager.commit());
                    page_count = pager.page_count();
                }
            }
        });
        if (iteration % 3 > 0) {
            ASSERT_OK(m_ctx.file->resize(0));
            ASSERT_OK(m_ctx.pager->checkpoint(iteration % 3 == 1
                                                  ? kCheckpointRestart
                                                  : kCheckpointPassive,
                                              nullptr));
            open_wal_if_present();
            ASSERT_OK(m_wal_file->resize(0));
        }
        m_ctx.reader([this, page_count](auto &pager, auto &, auto) {
            ASSERT_EQ(pager.page_count(), page_count);
            for (size_t i = 0; i < kManyPages; ++i) {
                ASSERT_EQ(i <= kManyPages / 2, read_page(i) != 0);
            }
        });
    }
}

TEST_F(PagerTests, Truncation)
{
    m_ctx.writer([this](auto &pager, auto &) {
        for (size_t i = 0; i < kManyPages; ++i) {
            allocate_page();
        }
        for (size_t i = 0; i < kManyPages; ++i) {
            alter_page(i);
        }
        pager.set_page_count(m_page_ids.at(kManyPages / 2).value);
        ASSERT_OK(pager.commit());
    });

    ASSERT_OK(m_ctx.pager->checkpoint(kCheckpointRestart, nullptr));

    uint64_t file_size;
    ASSERT_OK(m_ctx.file->get_size(file_size));
    ASSERT_EQ(file_size, TEST_PAGE_SIZE * m_page_ids.at(kManyPages / 2).value);

    m_ctx.reader([this](auto &, auto &, bool) {
        for (size_t i = 0; i < kManyPages; ++i) {
            EXPECT_EQ(i <= kManyPages / 2, read_page(i) != 0) << i;
        }
    });
}

TEST_F(PagerTests, ReportsOutOfRangePages)
{
    m_ctx.writer([](auto &pager, auto &) {
        PageRef *page;
        ASSERT_NOK(pager.acquire(Id(100), page));
        ASSERT_NOK(pager.acquire(Id(200), page));
        ASSERT_NOK(pager.acquire(Id(300), page));
    });
}

TEST_F(PagerTests, MovePage)
{
    static constexpr uint32_t kSpecialValue = 123'456;
    static constexpr uint32_t kNumPages = 32;
    m_ctx.writer([](auto &pager, auto &) {
        for (uint32_t i = 0; i < kNumPages; ++i) {
            PageRef *pg;
            ASSERT_OK(pager.allocate(pg));
            pager.mark_dirty(*pg);
            put_u32(pg->data, pg->page_id.value);
            pager.release(pg, Pager::kDiscard);
        }
        PageRef *pg;
        ASSERT_OK(pager.get_unused_page(pg));
        pager.mark_dirty(*pg);
        put_u32(pg->data, kSpecialValue);

        pager.move_page(*pg, Id(3));
        for (; pg->page_id.value != kNumPages;) {
            pager.move_page(*pg, Id(pg->page_id.value + 1));
        }
        ASSERT_EQ(get_u32(pg->data), kSpecialValue);

        pager.release(pg);
        ASSERT_OK(pager.commit());
    });
    m_ctx.reader([](auto &pager, auto &, bool) {
        PageRef *pg;
        ASSERT_OK(pager.acquire(Id(kNumPages), pg));
        ASSERT_EQ(get_u32(pg->data), kSpecialValue);
        pager.release(pg);
    });
}

#ifndef NDEBUG
TEST_F(PagerTests, DeathTest)
{
    ASSERT_EQ(m_ctx.pager->mode(), Pager::kOpen);
    ASSERT_DEATH((void)m_ctx.pager->commit(), "");
    ASSERT_DEATH((void)m_ctx.pager->page_count(), "");

    ASSERT_DEATH((void)m_ctx.pager->begin_writer(), "");
    ASSERT_OK(m_ctx.pager->lock_reader(nullptr));
    ASSERT_DEATH((void)m_ctx.pager->checkpoint(kCheckpointRestart, nullptr), "");

    m_ctx.writer([](auto &pager, auto &) {
        PageRef *a, *b;
        ASSERT_OK(pager.allocate(a));
        ASSERT_OK(pager.allocate(b));
        ASSERT_DEATH(pager.move_page(*a, b->page_id), "");
        pager.release(a);
        pager.release(b);
    });
}
#endif // NDEBUG

class MultiPagerTests : public PagerTests
{
public:
    PagerContext m_ctx2;

    auto SetUp() -> void override
    {
        delete m_env;
        m_env = &default_env();

        PagerTests::SetUp();
        m_ctx2.open(*m_env, Options::kLockNormal);
    }

    auto TearDown() -> void override
    {
        // Don't delete the default Env.
        m_env = nullptr;
    }
};

TEST_F(MultiPagerTests, Changed1)
{
    m_ctx.writer([](auto &pager, auto &) {
        ASSERT_OK(pager.commit());
    });

    m_ctx2.reader([](auto &pager, auto &file, auto changed) {
        ASSERT_EQ(pager.page_count(), 1); // Writer committed the initialized root
        ASSERT_TRUE(changed);             // Database was altered by m_ctx.pager.

        uint64_t file_size;
        ASSERT_OK(file.get_size(file_size));
        ASSERT_EQ(file_size, 0);
    });
}

TEST_F(MultiPagerTests, Changed2)
{
    m_ctx.writer([](auto &, auto &) {});
    m_ctx.writer([](auto &, auto &) {});
    m_ctx.writer([](auto &, auto &) {});

    m_ctx2.reader([](auto &pager, auto &file, auto changed) {
        ASSERT_TRUE(changed);
        ASSERT_EQ(pager.page_count(), 0); // No commit: page count stays 0

        uint64_t file_size;
        ASSERT_OK(file.get_size(file_size));
        ASSERT_EQ(file_size, 0);
    });
}

TEST_F(MultiPagerTests, ReaderAndWriter)
{
    m_ctx.writer([this](auto &, auto &) {
        allocate_page();

        m_ctx2.reader([](auto &, auto &, auto changed) {
            ASSERT_TRUE(changed);
        });

        ASSERT_OK(m_ctx.pager->commit());
        m_ctx.pager->finish();
    });
}

TEST_F(MultiPagerTests, MissingWalNonEmpty)
{
    m_ctx.writer([this](auto &pager, auto &) {
        allocate_page();
        ASSERT_OK(pager.commit());
    });
    m_ctx.reader([](auto &pager, auto &, bool) {
        ASSERT_GT(pager.page_count(), 1);
    });

    // m_ctx.file (and m_ctx.file) is still locked, so there won't be a checkpoint and the WAL will
    // remain on disk.
    m_ctx.pager->close();
    ASSERT_OK(m_env->remove_file(m_wal_name.c_str()));

    uint64_t file_size;
    ASSERT_OK(m_ctx.file->get_size(file_size));
    ASSERT_EQ(file_size, 0);

    open_pager(Options::kLockNormal);
    m_ctx.reader([](auto &pager, auto &, bool) {
        // WAL was unlinked without being checkpointed first. Lost the initialized root.
        ASSERT_EQ(pager.page_count(), 0);
    });
}

TEST_F(MultiPagerTests, MissingWalEmpty)
{
    m_ctx.writer([](auto &, auto &) {});
    m_ctx.reader([](auto &pager, auto &, bool) {
        ASSERT_EQ(pager.page_count(), 0);
    });

    m_ctx.pager->close();
    ASSERT_OK(m_env->remove_file(m_wal_name.c_str()));

    uint64_t file_size;
    ASSERT_OK(m_ctx.file->get_size(file_size));
    ASSERT_EQ(file_size, 0);

    m_ctx2.open(*m_env, Options::kLockNormal);
    m_ctx2.reader([](auto &pager, auto &, bool) {
        ASSERT_EQ(pager.page_count(), 0);
    });
}

} // namespace calicodb::test
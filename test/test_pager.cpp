// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "common.h"
#include "encoding.h"
#include "fake_env.h"
#include "freelist.h"
#include "logging.h"
#include "pager.h"
#include "temp.h"
#include "test.h"
#include "unique_ptr.h"
#include "wal.h"
#include <filesystem>

namespace calicodb::test
{

// Buffer manager tests were adapted from LevelDB.
class BufmgrTests : public testing::Test
{
public:
    static constexpr uint32_t kCacheSize = 1'000;
    Dirtylist m_dirtylist;
    Stat m_stat;
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

class PagerTests : public testing::Test
{
protected:
    static constexpr size_t kManyPages = kMinFrameCount * 100;

    Env *m_env;
    File *m_wal_file = nullptr;
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
        delete m_wal_file;
        delete m_env;

        EXPECT_EQ(Alloc::bytes_used(), 0);
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
        delete exchange(m_file, nullptr);
        delete exchange(m_wal_file, nullptr);

        File *file = nullptr;
        File *wal_file = nullptr;
        auto s = m_env->new_file(
            "db",
            Env::kCreate | Env::kReadWrite,
            file);
        if (!s.is_ok()) {
            ADD_FAILURE() << s.message();
            return false;
        }
        const Pager::Parameters param = {
            "db",
            "wal",
            file,
            m_env,
            nullptr,
            &m_status,
            &m_stat,
            nullptr,
            TEST_PAGE_SIZE,
            kMinFrameCount,
            Options::kSyncNormal,
            lock_mode,
            true,
        };
        s = Pager::open(param, m_pager);
        if (s.is_ok()) {
            s = m_pager->start_reader();
            m_pager->finish();
        }
        if (s.is_ok()) {
            s = m_env->new_file("wal", Env::kReadWrite, wal_file);
        }
        if (s.is_ok()) {
            m_file = file;
            m_wal_file = wal_file;
        } else {
            ADD_FAILURE() << s.message();
            delete file;
            return false;
        }
        return true;
    }
    auto close() -> void
    {
        Alloc::delete_object(m_pager);
        m_pager = nullptr;
    }

    std::vector<Id> m_page_ids;
    auto allocate_page(PageRef *&page_out) -> Id
    {
        EXPECT_OK(m_pager->allocate(page_out));
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
        m_pager->release(page);
        return id;
    }
    auto alter_page(PageRef &page) -> void
    {
        m_pager->mark_dirty(page);
        const auto value = get_u32(page.data + TEST_PAGE_SIZE - 4);
        put_u32(page.data + TEST_PAGE_SIZE - 4, value + 1);
    }
    auto alter_page(size_t index) -> void
    {
        PageRef *page;
        EXPECT_OK(m_pager->acquire(m_page_ids.at(index), page));
        alter_page(*page);
        m_pager->release(page);
    }
    auto read_page(const PageRef &page) -> uint32_t
    {
        return get_u32(page.data + TEST_PAGE_SIZE - 4);
    }
    auto read_page(size_t index) -> uint32_t
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
        if (m_pager->page_count() == 0) {
            m_pager->initialize_root();
        }
        fn();
        m_pager->finish();
    }
};

TEST_F(PagerTests, AllocatePage)
{
    pager_update([this] {
        // Root already exists.
        ASSERT_EQ(Id(2), allocate_page());
        ASSERT_EQ(Id(3), allocate_page());
        ASSERT_EQ(Id(4), allocate_page());
        ASSERT_EQ(4, m_pager->page_count());
    });
}

TEST_F(PagerTests, AcquirePage)
{
    pager_update([this] {
        allocate_page();
        allocate_page();
        allocate_page();
        ASSERT_EQ(4, m_pager->page_count());

        PageRef *page;
        for (uint32_t n = 1; n < 4; ++n) {
            ASSERT_OK(m_pager->acquire(Id(n), page));
            m_pager->release(page);
            ASSERT_EQ(4, m_pager->page_count());
        }
        // Attempt to skip page 4.
        ASSERT_TRUE(m_pager->acquire(Id(5), page).is_corruption());
    });
}

TEST_F(PagerTests, NOOP)
{
    pager_update([] {});
    pager_view([] {});
    ASSERT_OK(m_pager->checkpoint(true));
    ASSERT_OK(m_pager->checkpoint(false));
    pager_update([this] {
        m_pager->set_page_count(0);
        m_pager->set_status(Status::ok());
    });

    size_t file_size;
    // Database size is 0 before the first checkpoint.
    ASSERT_OK(m_env->file_size("db", file_size));
    ASSERT_EQ(file_size, 0);
}

TEST_F(PagerTests, Commit)
{
    reopen(Options::kLockNormal);
    pager_update([this] {
        for (size_t i = 0; i < kManyPages; ++i) {
            PageRef *page;
            allocate_page(page);
            alter_page(*page);
            m_pager->release(page);
        }
        ASSERT_OK(m_pager->commit());
    });
    pager_view([this] {
        for (size_t i = 0; i < kManyPages; ++i) {
            ASSERT_EQ(read_page(i), 1);
        }
    });
}

TEST_F(PagerTests, Commit2)
{
    for (int iteration = 0; iteration < 6; ++iteration) {
        reopen(iteration < 3 ? Options::kLockNormal : Options::kLockExclusive);
        pager_update([this] {
            // Alter each page.
            for (size_t i = 0; i < kManyPages; ++i) {
                PageRef *page;
                allocate_page(page);
                alter_page(*page);

                m_pager->release(
                    page,
                    // kNoCache should be ignored since the page is dirty.
                    Pager::kNoCache);
            }
            // Alter every other page, drop the rest.
            for (size_t i = 0; i < kManyPages; ++i) {
                PageRef *page;
                ASSERT_OK(m_pager->acquire(m_page_ids[i], page));
                alter_page(*page);
                // Discard even-numbered updates.
                m_pager->release(page, i & 1 ? Pager::kKeep : Pager::kDiscard);
            }
            ASSERT_OK(m_pager->commit());
        });
        if (iteration % 3 > 0) {
            // Make sure we actually have all the data we need in the WAL. The root page is
            // not in the WAL, but it is blank anyway.
            ASSERT_OK(m_file->resize(0));
            // Transfer the lost pages back.
            ASSERT_OK(m_pager->checkpoint(iteration % 3 == 1));
            // Everything should be back in the database file. The next reader shouldn't read
            // any pages from the WAL.
            ASSERT_OK(m_wal_file->resize(0));
        }
        pager_view([this] {
            for (size_t i = 0; i < kManyPages; ++i) {
                ASSERT_EQ(read_page(i), 1 + (i & 1));
            }
        });
    }
}

TEST_F(PagerTests, Rollback)
{
    reopen(Options::kLockNormal);
    size_t page_count = 0;
    pager_update([this, &page_count] {
        for (size_t i = 0; i < kManyPages; ++i) {
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
    pager_view([this, page_count] {
        ASSERT_EQ(m_pager->page_count(), page_count);
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
        pager_update([this, &page_count] {
            for (size_t i = 0; i < kManyPages; ++i) {
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
            ASSERT_OK(m_file->resize(0));
            ASSERT_OK(m_pager->checkpoint(iteration % 3 == 1));
            ASSERT_OK(m_wal_file->resize(0));
        }
        pager_view([this, page_count] {
            ASSERT_EQ(m_pager->page_count(), page_count);
            for (size_t i = 0; i < kManyPages; ++i) {
                ASSERT_EQ(i <= kManyPages / 2, read_page(i) != 0);
            }
        });
    }
}

TEST_F(PagerTests, Truncation)
{
    pager_update([this] {
        for (size_t i = 0; i < kManyPages; ++i) {
            allocate_page();
        }
        for (size_t i = 0; i < kManyPages; ++i) {
            alter_page(i);
        }
        m_pager->set_page_count(m_page_ids.at(kManyPages / 2).value);
        ASSERT_OK(m_pager->commit());
    });

    ASSERT_OK(m_pager->checkpoint(true));

    size_t file_size;
    ASSERT_OK(m_env->file_size("db", file_size));
    ASSERT_EQ(file_size, TEST_PAGE_SIZE * m_page_ids.at(kManyPages / 2).value);

    pager_view([this] {
        for (size_t i = 0; i < kManyPages; ++i) {
            EXPECT_EQ(i <= kManyPages / 2, read_page(i) != 0) << i;
        }
    });
}

TEST_F(PagerTests, ReportsOutOfRangePages)
{
    pager_update([this] {
        PageRef *page;
        ASSERT_NOK(m_pager->acquire(Id(100), page));
        ASSERT_NOK(m_pager->acquire(Id(200), page));
        ASSERT_NOK(m_pager->acquire(Id(300), page));
    });
}

TEST_F(PagerTests, MovePage)
{
    static constexpr uint32_t kSpecialValue = 123'456;
    static constexpr uint32_t kNumPages = 32;
    pager_update([this] {
        for (uint32_t i = 0; i < kNumPages; ++i) {
            PageRef *pg;
            ASSERT_OK(m_pager->allocate(pg));
            m_pager->mark_dirty(*pg);
            put_u32(pg->data, pg->page_id.value);
            m_pager->release(pg, Pager::kDiscard);
        }
        PageRef *pg;
        ASSERT_OK(m_pager->get_unused_page(pg));
        m_pager->mark_dirty(*pg);
        put_u32(pg->data, kSpecialValue);

        m_pager->move_page(*pg, Id(3));
        for (; pg->page_id.value != kNumPages;) {
            m_pager->move_page(*pg, Id(pg->page_id.value + 1));
        }
        ASSERT_EQ(get_u32(pg->data), kSpecialValue);

        m_pager->release(pg);
        ASSERT_OK(m_pager->commit());
    });
    pager_view([this] {
        PageRef *pg;
        ASSERT_OK(m_pager->acquire(Id(kNumPages), pg));
        ASSERT_EQ(get_u32(pg->data), kSpecialValue);
        m_pager->release(pg);
    });
}

#ifndef NDEBUG
TEST_F(PagerTests, DeathTest)
{
    ASSERT_EQ(m_pager->mode(), Pager::kOpen);
    ASSERT_DEATH((void)m_pager->commit(), "");

    ASSERT_DEATH((void)m_pager->start_writer(), "");
    ASSERT_OK(m_pager->start_reader());
    ASSERT_DEATH((void)m_pager->checkpoint(true), "");

    pager_update([this] {
        PageRef *a, *b;
        ASSERT_OK(m_pager->allocate(a));
        ASSERT_OK(m_pager->allocate(b));
        ASSERT_DEATH(m_pager->move_page(*a, b->page_id), "");
        m_pager->release(a);
        m_pager->release(b);
    });
}
#endif // NDEBUG

using WalComponents = std::tuple<Env *, Wal *, File *>;
using MakeWal = WalComponents (*)(Wal::Parameters);

auto make_temporary_wal(Wal::Parameters param) -> WalComponents
{
    param.env = new_temp_env(kMaxPageSize);
    EXPECT_NE(param.env, nullptr);
    EXPECT_OK(param.env->new_file("db", Env::kCreate | Env::kReadWrite,
                                  param.db_file));
    return {param.env, new_temp_wal(param), param.db_file};
}

auto make_persistent_wal(Wal::Parameters param) -> WalComponents
{
    Wal *wal;
    EXPECT_OK(param.env->new_file("db", Env::kCreate | Env::kReadWrite,
                                  param.db_file));
    EXPECT_OK(Wal::open(param, wal));
    return {param.env, wal, param.db_file};
}

class WalTests : public testing::TestWithParam<MakeWal>
{
public:
    const std::string m_filename;
    File *m_db_file = nullptr;
    Env *m_env = nullptr;
    Wal *m_wal = nullptr;
    Stat m_stat;

    std::default_random_engine m_rng;
    std::vector<uint32_t> m_temp;
    std::vector<uint32_t> m_perm;
    char m_scratch[TEST_PAGE_SIZE];

    explicit WalTests()
        : m_filename(testing::TempDir() + "calicodb_wal_tests")
    {
        std::filesystem::remove_all(m_filename);
    }

    ~WalTests() override
    {
        Alloc::delete_object(m_wal);
        delete m_db_file;
        if (m_env != &Env::default_env()) {
            delete m_env;
        }
        std::filesystem::remove_all(m_filename);
    }

    auto SetUp() -> void override
    {
        const Wal::Parameters param = {
            m_filename.c_str(),
            &Env::default_env(),
            nullptr,
            nullptr,
            &m_stat,
            nullptr,
            Options::kSyncNormal,
            Options::kLockNormal,
        };
        std::tie(m_env, m_wal, m_db_file) = GetParam()(param);
    }

    auto rollback() -> void
    {
        m_wal->rollback(
            [](auto *object, auto page_id) {
                auto &self = *static_cast<WalTests *>(object);
                const auto i = page_id.as_index();
                self.m_temp.at(i) = self.m_perm.at(i);
            },
            this);

        ASSERT_EQ(m_temp, m_perm);
        m_temp = m_perm;
    }

    struct WriteOptions {
        size_t db_size = 0;
        size_t truncate = 0;
        bool commit = false;
        bool sort_pages = false;
        bool omit_some = false;
    };
    auto write_batch(const WriteOptions &options) -> Status
    {
        std::vector<UniquePtr<PageRef>> pages;
        const size_t min_r = !options.omit_some;
        size_t occupied = 0;
        pages.reserve(options.db_size);
        for (size_t i = 0; i < options.db_size; ++i) {
            PageRef *page = nullptr;
            if (std::uniform_int_distribution<size_t>(min_r, 8)(m_rng) ||
                (occupied == 0 && i + 1 == options.db_size)) {
                page = PageRef::alloc(TEST_PAGE_SIZE);
                EXPECT_NE(page, nullptr);
                std::memset(page->data, 0, TEST_PAGE_SIZE);
                ++occupied;
            }
            pages.emplace_back(page);
        }

        std::vector<uint32_t> ks(pages.size());
        std::iota(begin(ks), end(ks), 1);
        auto vs = ks;
        std::shuffle(begin(ks), end(ks), m_rng);
        std::shuffle(begin(vs), end(vs), m_rng);
        if (m_temp.size() < pages.size()) {
            // Unoccupied pages have values of 0.
            m_temp.resize(pages.size());
        }
        Dirtylist dirtylist;
        for (size_t i = 0; i < pages.size(); ++i) {
            if (pages[i]) {
                pages[i]->page_id.value = ks.at(i);
                m_temp.at(ks.at(i) - 1) = vs.at(i);
                put_u32(pages[i]->data, vs.at(i));
                dirtylist.add(*pages[i]);
            }
        }

        auto *dirty = dirtylist.begin();
        if (options.sort_pages) {
            dirty = dirtylist.sort();
        } else {
            for (auto *p = dirty; p != dirtylist.end(); p = p->next_entry) {
                p->dirty = p->next_entry == dirtylist.end() ? nullptr : p->next_entry;
            }
        }
        EXPECT_NE(dirty, nullptr);
        auto s = m_wal->write(
            dirty->get_page_ref(),
            TEST_PAGE_SIZE,
            options.truncate ? options.truncate
                             : m_temp.size() * options.commit);
        if (s.is_ok()) {
            if (options.truncate) {
                m_temp.resize(options.truncate);
            }
            if (options.commit) {
                m_perm = m_temp;
            }
        }
        return s;
    }

    auto read_batch(size_t n) -> Status
    {
        char buffer[TEST_PAGE_SIZE] = {};
        for (size_t i = 0; i < n; ++i) {
            char *page = buffer;
            auto s = m_wal->read(Id::from_index(i), TEST_PAGE_SIZE, page);
            if (!s.is_ok()) {
                return s;
            } else if (page) {
                // Found in the WAL.
                EXPECT_EQ(m_temp.at(i), get_u32(page));
            } else if (i < m_temp.size()) {
                // Not found, but should exist: read from the database file.
                Slice result;
                s = m_db_file->read(i * TEST_PAGE_SIZE, TEST_PAGE_SIZE, buffer, &result);
                if (!s.is_ok()) {
                    return s;
                } else if (result.size() != TEST_PAGE_SIZE) {
                    ADD_FAILURE() << "incomplete read: read " << result.size() << '/'
                                  << TEST_PAGE_SIZE << " bytes";
                    return Status::io_error();
                }
                EXPECT_EQ(m_temp.at(i), get_u32(buffer));
            }
        }
        return Status::ok();
    }

    auto expect_missing(Id id) const -> void
    {
        char buffer[TEST_PAGE_SIZE];

        char *page = buffer;
        ASSERT_OK(m_wal->read(id, TEST_PAGE_SIZE, page));
        ASSERT_EQ(page, nullptr);
    }

    template <class Callback>
    auto with_reader(const Callback &cb) -> Status
    {
        bool _;
        auto s = m_wal->start_reader(_);
        if (s.is_ok()) {
            s = cb();
            m_wal->finish_reader();
        }
        return s;
    }

    template <class Callback>
    auto with_writer(const Callback &cb) -> Status
    {
        return with_reader([this, &cb] {
            auto s = m_wal->start_writer();
            if (s.is_ok()) {
                s = cb();
                m_wal->finish_writer();
            }
            return s;
        });
    }

    struct RunOptions : WriteOptions {
        size_t commit_interval = 1;
        size_t rollback_interval = 1;
        size_t ckpt_reset_interval = 1;
    };
    auto run_operations(const RunOptions &options)
    {
        static constexpr size_t kMinPages = 10;
        static constexpr size_t kMaxPages = kMinPages * 100;
        RandomGenerator random;
        for (size_t i = 1; i < 1'234; ++i) {
            ASSERT_OK(with_writer([this, i, &random, &options] {
                auto opt = options;
                opt.db_size = random.Next(kMinPages, kMaxPages);
                opt.commit = i % opt.commit_interval == 0;
                const auto r = random.Next(1, kMaxPages);
                if (opt.commit && r < opt.db_size) {
                    opt.truncate = r;
                }
                auto s = write_batch(opt);
                if (s.is_ok() && !opt.commit && i % opt.rollback_interval == 0) {
                    rollback();
                }
                return s;
            }));
            ASSERT_OK(m_wal->checkpoint(i % options.ckpt_reset_interval == 0, m_scratch, TEST_PAGE_SIZE));
            ASSERT_OK(with_reader([this] {
                return read_batch(kMaxPages);
            }));
        }
    }
};

TEST_P(WalTests, OpenAndClose)
{
    // Do nothing.
}

TEST_P(WalTests, EmptyTransaction)
{
    ASSERT_OK(with_reader([] { return Status::ok(); }));
    ASSERT_OK(with_writer([] { return Status::ok(); }));
}

TEST_P(WalTests, EmptyCheckpoint)
{
    ASSERT_OK(with_reader([] { return Status::ok(); }));

    // Checkpoint cannot be run until the WAL index is created the first time a
    // transaction is started.
    ASSERT_OK(m_wal->checkpoint(false, m_scratch, TEST_PAGE_SIZE));
    ASSERT_OK(m_wal->checkpoint(true, m_scratch, TEST_PAGE_SIZE));
}

TEST_P(WalTests, Commit)
{
    ASSERT_OK(with_writer([this] {
        WriteOptions opt;
        opt.commit = true;
        opt.db_size = 9;
        return write_batch(opt);
    }));
    ASSERT_OK(with_reader([this] {
        expect_missing(Id(10));
        return read_batch(10);
    }));
}

TEST_P(WalTests, Truncate)
{
    ASSERT_OK(with_writer([this] {
        WriteOptions opt;
        opt.commit = true;
        opt.db_size = 10;
        opt.truncate = 8;
        return write_batch(opt);
    }));
    ASSERT_OK(m_wal->checkpoint(true, m_scratch, TEST_PAGE_SIZE));
    ASSERT_OK(with_reader([this] {
        expect_missing(Id(9));
        expect_missing(Id(10));
        return read_batch(10);
    }));
}

TEST_P(WalTests, ReadsAndWrites)
{
    static constexpr size_t kNumPages = 1'000;
    for (size_t i = 0; i < 10; ++i) {
        ASSERT_OK(with_writer([this, i] {
            WriteOptions opt;
            opt.commit = true;
            opt.db_size = kNumPages / 10 * (i + 1);
            opt.sort_pages = i % 1;
            opt.omit_some = i % 2;
            return write_batch(opt);
        }));
        ASSERT_OK(m_wal->checkpoint(i < 5, m_scratch, TEST_PAGE_SIZE));
        ASSERT_OK(with_reader([this] {
            return read_batch(kNumPages);
        }));
    }
}

TEST_P(WalTests, Rollback)
{
    for (size_t i = 0; i < 10; ++i) {
        for (size_t j = 0; j < 2; ++j) {
            // Commit when j == 0, rollback when j == 1.
            ASSERT_OK(with_writer([this, i, j] {
                WriteOptions opt;
                opt.commit = j == 0;
                opt.db_size = (i + 1) * 10;
                opt.sort_pages = i % 1;
                opt.omit_some = j % 1;
                auto s = write_batch(opt);
                if (s.is_ok() && j) {
                    rollback();
                }
                return s;
            }));
        }
        ASSERT_OK(with_reader([this] {
            return read_batch(100);
        }));
    }
}

TEST_P(WalTests, SanityCheck)
{
    run_operations(RunOptions());
}

TEST_P(WalTests, Operations_1)
{
    RunOptions options;
    options.commit_interval = 4;
    run_operations(RunOptions());
}

TEST_P(WalTests, Operations_2)
{
    RunOptions options;
    options.commit_interval = 4;
    options.rollback_interval = 2;
    run_operations(RunOptions());
}

INSTANTIATE_TEST_SUITE_P(
    TemporaryWalTests,
    WalTests,
    testing::Values(make_temporary_wal));

INSTANTIATE_TEST_SUITE_P(
    PersistentWalTests,
    WalTests,
    testing::Values(make_persistent_wal));

} // namespace calicodb::test
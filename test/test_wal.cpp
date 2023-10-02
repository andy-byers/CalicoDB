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
#include <filesystem>

namespace calicodb::test
{

using WalComponents = std::tuple<Env *, Wal *, File *>;
using MakeWal = WalComponents (*)(WalOptionsExtra, const char *);

static Stats s_stat;

auto make_temporary_wal(WalOptionsExtra options, const char *) -> WalComponents
{
    options.env = new_temp_env(kMaxPageSize);
    EXPECT_NE(options.env, nullptr);
    EXPECT_OK(options.env->new_file("db", Env::kCreate | Env::kReadWrite,
                                    options.db));
    return {options.env, new_temp_wal(options, TEST_PAGE_SIZE), options.db};
}

auto make_persistent_wal(WalOptionsExtra options, const char *filename) -> WalComponents
{
    EXPECT_OK(options.env->new_file("db", Env::kCreate | Env::kReadWrite,
                                    options.db));
    auto *wal = new_default_wal(options, filename);
    EXPECT_NE(wal, nullptr);
    EXPECT_OK(wal->open(reinterpret_cast<const WalOptions &>(options), filename));
    return {options.env, wal, options.db};
}

class WalTests : public testing::TestWithParam<MakeWal>
{
public:
    const std::string m_filename;
    File *m_db_file = nullptr;
    Env *m_env = nullptr;
    Wal *m_wal = nullptr;

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
        Mem::delete_object(m_wal);
        delete m_db_file;
        if (m_env != &default_env()) {
            delete m_env;
        }
        std::filesystem::remove_all(m_filename);
    }

    auto SetUp() -> void override
    {
        const WalOptionsExtra param = {
            {&default_env(),
             nullptr,
             &s_stat},
            nullptr,
            Options::kSyncNormal,
            Options::kLockNormal,
        };
        std::tie(m_env, m_wal, m_db_file) = GetParam()(param, m_filename.c_str());
    }

    auto rollback() -> void
    {
        m_wal->rollback(
            [](auto *object, auto page_id) {
                auto &self = *static_cast<WalTests *>(object);
                const auto i = page_id - 1;
                if (i < self.m_perm.size()) {
                    self.m_temp.at(i) = self.m_perm[i];
                }
            },
            this);

        m_temp.resize(m_perm.size());
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
        WalPagesImpl pg(*dirty->get_page_ref());
        EXPECT_NE(dirty, nullptr);
        auto s = m_wal->write(
            pg,
            TEST_PAGE_SIZE,
            options.truncate ? options.truncate
                             : m_temp.size() * options.commit);
        if (s.is_ok()) {
            if (options.truncate) {
                m_temp.resize(options.truncate);
            }
            if (options.commit) {
                m_perm = m_temp; // Commit
            } else {
                m_temp = m_perm; // Rollback
            }
        }
        return s;
    }

    auto read_batch(size_t n) -> Status
    {
        char buffer[TEST_PAGE_SIZE] = {};
        for (size_t i = 0; i < n; ++i) {
            char *page = buffer;
            auto s = m_wal->read(static_cast<uint32_t>(i + 1), TEST_PAGE_SIZE, page);
            if (!s.is_ok()) {
                return s;
            } else if (page) {
                // Found in the WAL.
                EXPECT_EQ(m_temp.at(i), get_u32(page));
            } else if (i < m_temp.size()) {
                // Not found, but should exist: read from the database file.
                s = m_db_file->read_exact(i * TEST_PAGE_SIZE, TEST_PAGE_SIZE, buffer);
                if (!s.is_ok()) {
                    return s;
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
        ASSERT_OK(m_wal->read(id.value, TEST_PAGE_SIZE, page));
        ASSERT_EQ(page, nullptr);
    }

    template <class Callback>
    auto with_reader(const Callback &cb) -> Status
    {
        bool _;
        auto s = m_wal->start_read(_);
        if (s.is_ok()) {
            s = cb();
            m_wal->finish_read();
        }
        return s;
    }

    template <class Callback>
    auto with_writer(const Callback &cb) -> Status
    {
        return with_reader([this, &cb] {
            auto s = m_wal->start_write();
            if (s.is_ok()) {
                s = cb();
                m_wal->finish_write();
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
            ASSERT_OK(m_wal->checkpoint(i % options.ckpt_reset_interval == 0 ? kCheckpointRestart : kCheckpointPassive,
                                        m_scratch, TEST_PAGE_SIZE, nullptr, nullptr));
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
    ASSERT_OK(m_wal->checkpoint(kCheckpointPassive, m_scratch, TEST_PAGE_SIZE, nullptr, nullptr));
    ASSERT_OK(m_wal->checkpoint(kCheckpointRestart, m_scratch, TEST_PAGE_SIZE, nullptr, nullptr));
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
    ASSERT_OK(m_wal->checkpoint(kCheckpointRestart, m_scratch, TEST_PAGE_SIZE, nullptr, nullptr));
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
        ASSERT_OK(m_wal->checkpoint(i < 5 ? kCheckpointRestart : kCheckpointPassive, m_scratch, TEST_PAGE_SIZE, nullptr, nullptr));
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
    run_operations(options);
}

TEST_P(WalTests, Operations_2)
{
    RunOptions options;
    options.commit_interval = 4;
    options.rollback_interval = 2;
    run_operations(options);
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
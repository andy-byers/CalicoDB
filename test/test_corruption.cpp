// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/cursor.h"
#include "calicodb/db.h"
#include "common.h"
#include "fake_env.h"
#include "header.h"
#include "test.h"

namespace calicodb::test
{

class CorruptionTests : public testing::Test
{
protected:
    static constexpr size_t kN = 1'234;
    int m_status_counters[3] = {};
    const std::string m_filename;
    std::string m_junk;
    std::string m_backup;
    FakeEnv m_env;
    Options m_options;

    explicit CorruptionTests()
        : m_filename("/fake_database"),
          m_junk("0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxy")
    {
        m_options.env = &m_env;
        m_options.page_size = TEST_PAGE_SIZE;
        m_options.create_if_missing = true;

        while (m_junk.size() < 256) {
            m_junk.append(m_junk);
        }
        m_junk.resize(256);
    }

    ~CorruptionTests() override = default;

    auto SetUp() -> void override
    {
        DB *db;
        ASSERT_OK(DB::open(m_options, m_filename.c_str(), db));
        ASSERT_OK(db->update([](auto &tx) {
            BucketPtr b;
            auto &main = tx.main_bucket();
            EXPECT_OK(test_create_bucket_if_missing(main, "b1", b));
            for (size_t i = 0; i < kN; ++i) {
                EXPECT_OK(b->put(numeric_key(i), numeric_key(i)));
                EXPECT_OK(b->put("*" + numeric_key(i), numeric_key(i)));
            }
            for (size_t i = 0; i < kN; ++i) {
                EXPECT_OK(b->erase("*" + numeric_key(i)));
            }
            return Status::ok();
        }));
        delete db;

        m_backup = *m_env.get_file_contents(m_filename.c_str());
    }

    auto database_file() const -> std::string &
    {
        auto *file = m_env.get_file_contents(m_filename.c_str());
        EXPECT_NE(file, nullptr) << "database has not been created";
        return *file;
    }

    auto open_database() const
    {
        DB *db;
        // DB::open() doesn't touch the database file (unless it needs to run a checkpoint, which
        // is not the case here), so it will never detect corruption.
        EXPECT_OK(DB::open(m_options, m_filename.c_str(), db));
        return std::unique_ptr<DB>(db);
    }

    auto set_normal_contents() const -> std::string &
    {
        auto &file = database_file();
        file = m_backup; // Refresh data
        return file;
    }

    auto set_corrupted_contents(size_t iteration) const
    {
        auto &file = set_normal_contents();
        const auto offset = iteration * m_junk.size();
        if (offset + m_junk.size() > file.size()) {
            return false;
        }
        std::memcpy(file.data() + offset, m_junk.data(), m_junk.size());
        return true;
    }

    // Make sure the given status has an expected status code. Allowed status codes are
    // kOK (corruption was not detected), kInvalidArgument (database file does not appear
    // to be a CalicoDB database), and kCorruption (corruption was detected).
    auto check_status(const Status &s)
    {
        // Only 1 of the following s.is_*() will ever be true.
        const auto index = s.is_ok() +
                           s.is_invalid_argument() * 2 +
                           s.is_corruption() * 3;
        ASSERT_GT(index, 0) << s.message();
        ++m_status_counters[index - 1];
    }

    auto run_read_transaction(const DB &db)
    {
        // Scan through "b1".
        return db.view([](const auto &tx) {
            BucketPtr b1;
            auto s = test_open_bucket(tx, "b1", b1);
            if (s.is_ok()) {
                auto c = test_new_cursor(*b1);
                c->seek_first();
                while (c->is_valid()) {
                    c->next();
                }
                s = c->status();
            }
            return s;
        });
    }

    auto run_write_transaction(DB &db)
    {
        // Transfer records from "b1" to "b2"
        return db.update([](auto &tx) {
            BucketPtr b1, b2;
            auto &main = tx.main_bucket();
            auto s = test_open_bucket(main, "b1", b1);
            if (s.is_ok()) {
                s = test_create_bucket_if_missing(main, "b2", b2);
            }
            if (!s.is_ok()) {
                return s;
            }
            auto c1 = test_new_cursor(*b1);
            c1->seek_first();
            while (s.is_ok() && c1->is_valid()) {
                s = b2->put(c1->key(), c1->value());
                c1->next();
            }
            return s;
        });
    }

    auto test_corrupted_database()
    {
        auto db = open_database();

        auto s = run_read_transaction(*db);
        check_status(s);

        s = run_write_transaction(*db);
        check_status(s);

        s = db->update([](auto &tx) {
            BucketPtr b1;
            auto &main = tx.main_bucket();
            auto s = test_open_bucket(main, "b1", b1);
            if (!s.is_ok()) {
                return s;
            }
            // Clear b1.
            auto c1 = test_new_cursor(*b1);
            c1->seek_first();
            while (c1->is_valid()) {
                s = b1->erase(*c1);
                EXPECT_EQ(s, c1->status());
            }
            if (s.is_ok()) {
                s = main.drop_bucket("b1");
                b1.reset(); // Free b1's pages
            }
            if (s.is_ok()) {
                s = tx.vacuum();
            }
            return s;
        });
        check_status(s);

        s = db->view([](const auto &tx) {
            BucketPtr b2;
            auto &main = tx.main_bucket();
            auto s = test_open_bucket(main, "b2", b2);
            if (!s.is_ok()) {
                return s;
            }
            // Scan b2 backwards.
            auto c2 = test_new_cursor(*b2);
            c2->seek_last();
            while (c2->is_valid()) {
                c2->previous();
            }
            return s;
        });
        check_status(s);
    }
};

TEST_F(CorruptionTests, GenericCorruption)
{
    for (size_t i = 0; set_corrupted_contents(i); ++i) {
        test_corrupted_database();
    }
    TEST_LOG << "StatusCounters:\n"
             << "kOK:              " << m_status_counters[0] << '\n'
             << "kInvalidArgument: " << m_status_counters[1] << '\n'
             << "kCorruption:      " << m_status_counters[2] << '\n';
}

TEST_F(CorruptionTests, CorruptedFormatString)
{
    auto &file = set_normal_contents();
    std::strcpy(file.data(), "calicoDB format 1"); // 'C' -> 'c' at 0

    auto s = run_read_transaction(*open_database());
    ASSERT_TRUE(s.is_invalid_argument()) << s.message();
}

TEST_F(CorruptionTests, IncorrectFormatVersion)
{
    auto &file = set_normal_contents();
    ++file[FileHdr::kFmtVersionOffset];

    auto s = run_read_transaction(*open_database());
    ASSERT_TRUE(s.is_invalid_argument()) << s.message();
}

TEST_F(CorruptionTests, CorruptedPageSize1)
{
    auto &file = set_normal_contents();
    FileHdr::put_page_size(file.data(), kMinPageSize / 2);

    auto s = run_read_transaction(*open_database());
    ASSERT_TRUE(s.is_corruption()) << s.message();
}

TEST_F(CorruptionTests, CorruptedPageSize2)
{
    auto &file = set_normal_contents();
    FileHdr::put_page_size(file.data(), kMaxPageSize + 1);

    auto s = run_read_transaction(*open_database());
    ASSERT_TRUE(s.is_corruption()) << s.message();
}

TEST_F(CorruptionTests, CorruptedPageSize3)
{
    auto &file = set_normal_contents();
    FileHdr::put_page_size(file.data(), kMinPageSize + 1);

    auto s = run_read_transaction(*open_database());
    ASSERT_TRUE(s.is_corruption()) << s.message();
}

TEST_F(CorruptionTests, CorruptedLargestRoot)
{
    auto &file = set_normal_contents();
    FileHdr::put_largest_root(file.data(), Id::null());

    auto s = run_write_transaction(*open_database());
    ASSERT_TRUE(s.is_corruption()) << s.message();
}

TEST_F(CorruptionTests, CorruptedFreelistHead)
{
    auto &file = set_normal_contents();
    FileHdr::put_freelist_head(file.data(), Id(1'234'567'890));

    auto s = run_write_transaction(*open_database());
    ASSERT_TRUE(s.is_corruption()) << s.message();
}

TEST_F(CorruptionTests, CorruptedFreelistLength)
{
    auto &file = set_normal_contents();
    const auto freelist_len = FileHdr::get_freelist_length(file.data());
    FileHdr::put_freelist_length(file.data(), freelist_len * 2);

    auto s = run_write_transaction(*open_database());
    ASSERT_TRUE(s.is_corruption()) << s.message();
}

TEST_F(CorruptionTests, CorruptedNodeType)
{
    auto &file = set_normal_contents();
    file[FileHdr::kSize] = '\xFF';

    auto s = run_read_transaction(*open_database());
    ASSERT_TRUE(s.is_corruption()) << s.message();
}

TEST_F(CorruptionTests, CorruptedCellCount)
{
    auto &file = set_normal_contents();
    NodeHdr::put_cell_count(file.data() + FileHdr::kSize, 0xFFFF);

    auto s = run_read_transaction(*open_database());
    ASSERT_TRUE(s.is_corruption()) << s.message();
}

TEST_F(CorruptionTests, CorruptedCellAreaStart)
{
    auto &file = set_normal_contents();
    NodeHdr::put_cell_start(file.data() + FileHdr::kSize, TEST_PAGE_SIZE + 1);

    auto s = run_read_transaction(*open_database());
    ASSERT_TRUE(s.is_corruption()) << s.message();
}

TEST_F(CorruptionTests, CorruptedFreelistStart)
{
    auto &file = set_normal_contents();
    NodeHdr::put_free_start(file.data() + FileHdr::kSize, TEST_PAGE_SIZE + 1);

    auto s = run_read_transaction(*open_database());
    ASSERT_TRUE(s.is_corruption()) << s.message();
}

TEST_F(CorruptionTests, CorruptedFragmentCount)
{
    auto &file = set_normal_contents();
    NodeHdr::put_frag_count(file.data() + FileHdr::kSize, 0xFF);

    auto s = run_read_transaction(*open_database());
    ASSERT_TRUE(s.is_corruption()) << s.message();
}

TEST_F(CorruptionTests, CorruptedNextID)
{
    auto &file = set_normal_contents();
    // Page 3 is the root page of "b1", which contains many records already. It should have split
    // already, becoming an internal node with a "next ID" field.
    const auto page3 = file.data() + TEST_PAGE_SIZE * 2;
    ASSERT_EQ(NodeHdr::kInternal, NodeHdr::get_type(page3));
    NodeHdr::put_next_id(page3, Id::null());

    auto s = run_read_transaction(*open_database());
    ASSERT_TRUE(s.is_corruption()) << s.message();
}

} // namespace calicodb::test
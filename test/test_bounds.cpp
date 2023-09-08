// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/cursor.h"
#include "calicodb/db.h"
#include "common.h"
#include "test.h"
#include "utils.h"
#include <filesystem>
#include <gtest/gtest.h>

namespace calicodb::test
{
#if 0  // TODO: These tests use quite a bit of memory and may not be a good idea to run in CI right now.
class BoundaryValueTests : public testing::Test
{
protected:
    static constexpr size_t kLargeValueLength =
        std::numeric_limits<uint32_t>::max();
    const std::string m_filename;
    const char *m_backing;
    const Slice m_largest_slice;
    const Slice m_overflow_slice;
    DB *m_db = nullptr;

    explicit BoundaryValueTests()
        : m_filename(testing::TempDir() + "db"),
          m_backing(new char[kLargeValueLength + 1]()),
          m_largest_slice(m_backing, kLargeValueLength),
          m_overflow_slice(m_backing, kLargeValueLength + 1)
    {
        (void)DB::destroy(Options(), m_filename);
    }

    ~BoundaryValueTests() override
    {
        delete m_db;
        delete[] m_backing;
    }

    auto SetUp() -> void override
    {
        ASSERT_OK(DB::open(Options(), m_filename, m_db));
    }
};

TEST_F(BoundaryValueTests, BoundaryPayload)
{
    ASSERT_OK(m_db->run(WriteOptions(), [this](auto &tx) {
        TestCursor c;
        auto s = test_create_and_open_bucket(tx, BucketOptions(), "bucket", c);
        if (s.is_ok()) {
            // Put a maximally-sized record.
            s = tx.put(*c, m_largest_slice, m_largest_slice);
        }
        return s;
    }));

    ASSERT_OK(m_db->run(ReadOptions(), [this](const auto &tx) {
        TestCursor c;
        auto s = test_open_bucket(tx, "bucket", b);
        if (s.is_ok()) {
            std::string value;
            s = tx.get(*c, m_largest_slice, &value);
            EXPECT_EQ(value, m_largest_slice);
        }
        return s;
    }));
}

TEST_F(BoundaryValueTests, OverflowPayload)
{
    ASSERT_OK(m_db->run(WriteOptions(), [this](auto &tx) {
        TestCursor c;
        auto s = test_create_and_open_bucket(tx, BucketOptions(), "bucket", c);
        if (s.is_ok()) {
            EXPECT_TRUE((s = tx.put(*c, m_overflow_slice, "v")).is_invalid_argument()) << to_string(s);
            EXPECT_TRUE((s = tx.put(*c, "k", m_overflow_slice)).is_invalid_argument()) << to_string(s);
        }
        return Status::ok();
    }));
}
#endif // 0
class StressTests : public testing::Test
{
protected:
    const std::string m_filename;
    DB *m_db = nullptr;

    explicit StressTests()
        : m_filename(testing::TempDir() + "calicodb_stress_tests")
    {
        std::filesystem::remove_all(m_filename);
        std::filesystem::remove_all(m_filename + to_string(kDefaultWalSuffix));
        std::filesystem::remove_all(m_filename + to_string(kDefaultShmSuffix));
    }

    ~StressTests() override
    {
        delete m_db;
    }

    auto SetUp() -> void override
    {
        ASSERT_OK(DB::open(Options(), m_filename.c_str(), m_db));
    }
};

TEST_F(StressTests, LotsOfBuckets)
{
    // There isn't really a limit on the number of buckets one can create. Just create a
    // bunch of them.
    static constexpr size_t kNumBuckets = 100'000;
    ASSERT_OK(m_db->run(WriteOptions(), [](auto &tx) {
        Status s;
        for (size_t i = 0; s.is_ok() && i < kNumBuckets; ++i) {
            TestCursor c;
            const auto name = numeric_key(i);
            s = test_create_and_open_bucket(tx, BucketOptions(), to_slice(name), c);
            if (s.is_ok()) {
                s = tx.put(*c, to_slice(name), to_slice(name));
            }
        }
        return s;
    }));
    ASSERT_OK(m_db->run(ReadOptions(), [](const auto &tx) {
        Status s;
        for (size_t i = 0; s.is_ok() && i < kNumBuckets; ++i) {
            TestCursor c;
            const auto name = numeric_key(i);
            s = test_open_bucket(tx, to_slice(name), c);
            if (s.is_ok()) {
                c->seek_first();
                EXPECT_TRUE(c->is_valid());
                EXPECT_EQ(to_slice(name), c->key());
                EXPECT_EQ(to_slice(name), c->value());
            }
        }
        return s;
    }));
}

TEST_F(StressTests, CursorLimit)
{
    ASSERT_OK(m_db->run(WriteOptions(), [](auto &tx) {
        Status s;
        std::vector<TestCursor> cursors(1'000);
        for (size_t i = 0; s.is_ok() && i < cursors.size(); ++i) {
            s = test_create_and_open_bucket(tx, BucketOptions(), "bucket", cursors[i]);
            if (s.is_ok()) {
                const auto name = numeric_key(i);
                s = tx.put(*cursors[i], to_slice(name), to_slice(name));
            } else {
                break;
            }
        }

        for (size_t i = 0; s.is_ok() && i < cursors.size(); ++i) {
            cursors[i]->seek_first();
            for (size_t j = 0; cursors[i]->is_valid() && j < i; ++j) {
                cursors[i]->next();
            }
            EXPECT_TRUE(cursors[i]->is_valid());
            EXPECT_EQ(to_string(cursors[i]->key()), numeric_key(i));
            EXPECT_EQ(to_string(cursors[i]->value()), numeric_key(i));
        }
        return s;
    }));
}

TEST_F(StressTests, LargeVacuum)
{
    static constexpr size_t kNumRecords = 1'234;
    static constexpr size_t kTotalBuckets = 2'500;
    static constexpr size_t kDroppedBuckets = kTotalBuckets / 10;
    ASSERT_OK(m_db->run(WriteOptions(), [](auto &tx) {
        Status s;
        for (size_t i = 0; s.is_ok() && i < kTotalBuckets; ++i) {
            TestCursor c;
            const auto name = numeric_key(i);
            s = test_create_and_open_bucket(tx, BucketOptions(), to_slice(name), c);
            for (size_t j = 0; s.is_ok() && j < kNumRecords; ++j) {
                const auto name2 = numeric_key(j);
                s = tx.put(*c, to_slice(name2), to_slice(name2));
            }
        }
        for (size_t i = 0; s.is_ok() && i < kDroppedBuckets; ++i) {
            const auto name2 = numeric_key(i);
            s = tx.drop_bucket(to_slice(name2));
        }
        if (s.is_ok()) {
            // Run a vacuum while there are many buckets open.
            s = tx.vacuum();
        }
        return s;
    }));
    ASSERT_OK(m_db->run(ReadOptions(), [](const auto &tx) {
        Status s;
        for (size_t i = 0; s.is_ok() && i < kTotalBuckets; ++i) {
            TestCursor c;
            const auto name = numeric_key(i);
            s = test_open_bucket(tx, to_slice(name), c);
            if (i < kDroppedBuckets) {
                EXPECT_TRUE(s.is_invalid_argument()) << s.message();
                s = Status::ok();
            } else if (s.is_ok()) {
                c->seek_first();
                for (size_t j = 0; j < kNumRecords; ++j) {
                    const auto name2 = numeric_key(j);
                    EXPECT_TRUE(c->is_valid());
                    EXPECT_EQ(to_slice(name2), c->key());
                    EXPECT_EQ(to_slice(name2), c->value());
                    c->next();
                }
            }
        }
        return s;
    }));
}

} // namespace calicodb::test

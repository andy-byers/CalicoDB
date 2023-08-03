// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/cursor.h"
#include "calicodb/db.h"
#include "common.h"
#include "test.h"
#include "utils.h"
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
    ASSERT_OK(m_db->update([this](auto &tx) {
        Bucket b;
        auto s = tx.create_bucket(BucketOptions(), "bucket", &b);
        if (s.is_ok()) {
            // Put a maximally-sized record.
            s = tx.put(b, m_largest_slice, m_largest_slice);
        }
        return s;
    }));

    ASSERT_OK(m_db->view([this](const auto &tx) {
        Bucket b;
        auto s = tx.open_bucket("bucket", b);
        if (s.is_ok()) {
            std::string value;
            s = tx.get(b, m_largest_slice, &value);
            EXPECT_EQ(value, m_largest_slice);
        }
        return s;
    }));
}

TEST_F(BoundaryValueTests, OverflowPayload)
{
    ASSERT_OK(m_db->update([this](auto &tx) {
        Bucket b;
        auto s = tx.create_bucket(BucketOptions(), "bucket", &b);
        if (s.is_ok()) {
            EXPECT_TRUE((s = tx.put(b, m_overflow_slice, "v")).is_invalid_argument()) << s.to_string();
            EXPECT_TRUE((s = tx.put(b, "k", m_overflow_slice)).is_invalid_argument()) << s.to_string();
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
        : m_filename(testing::TempDir() + "db")
    {
        (void)DB::destroy(Options(), m_filename);
    }

    ~StressTests() override
    {
        delete m_db;
    }

    auto SetUp() -> void override
    {
        ASSERT_OK(DB::open(Options(), m_filename, m_db));
    }
};

TEST_F(StressTests, LotsOfBuckets)
{
    // There isn't really a limit on the number of buckets one can create. Just create a
    // bunch of them.
    static constexpr size_t kNumBuckets = 100'000;
    ASSERT_OK(m_db->update([](auto &tx) {
        Status s;
        for (size_t i = 0; s.is_ok() && i < kNumBuckets; ++i) {
            Bucket b;
            const auto name = numeric_key(i);
            s = tx.create_bucket(BucketOptions(), name, &b);
            if (s.is_ok()) {
                s = tx.put(b, name, name);
            }
        }
        return s;
    }));
    ASSERT_OK(m_db->view([](auto &tx) {
        Status s;
        for (size_t i = 0; s.is_ok() && i < kNumBuckets; ++i) {
            Bucket b;
            const auto name = numeric_key(i);
            s = tx.open_bucket(name, b);
            if (s.is_ok()) {
                auto *c = tx.new_cursor(b);
                c->seek_first();
                EXPECT_TRUE(c->is_valid());
                EXPECT_EQ(name, c->key());
                EXPECT_EQ(name, c->value());
                delete c;
            }
        }
        return s;
    }));
}

TEST_F(StressTests, CursorLimit)
{
    ASSERT_OK(m_db->update([](auto &tx) {
        Bucket b;
        auto s = tx.create_bucket(BucketOptions(), "bucket", &b);
        std::vector<Cursor *> cursors;
        for (size_t i = 0; s.is_ok() && i < 1'000; ++i) {
            auto *c = tx.new_cursor(b);
            if (c) {
                cursors.emplace_back(c);
                s = tx.put(b, numeric_key(i), numeric_key(i));
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
            EXPECT_EQ(cursors[i]->key(), numeric_key(i));
            EXPECT_EQ(cursors[i]->value(), numeric_key(i));
        }
        for (const auto *c : cursors) {
            delete c;
        }
        return s;
    }));
}

TEST_F(StressTests, LargeVacuum)
{
    static constexpr size_t kNumRecords = 1'234;
    static constexpr size_t kTotalBuckets = 2'500;
    static constexpr size_t kDroppedBuckets = kTotalBuckets / 10;
    ASSERT_OK(m_db->update([](auto &tx) {
        Status s;
        for (size_t i = 0; s.is_ok() && i < kTotalBuckets; ++i) {
            Bucket b;
            const auto name = numeric_key(i);
            s = tx.create_bucket(BucketOptions(), name, &b);
            for (size_t j = 0; s.is_ok() && j < kNumRecords; ++j) {
                s = tx.put(b, numeric_key(j), numeric_key(j));
            }
        }
        for (size_t i = 0; s.is_ok() && i < kDroppedBuckets; ++i) {
            s = tx.drop_bucket(numeric_key(i));
        }
        if (s.is_ok()) {
            // Run a vacuum while there are many buckets open.
            s = tx.vacuum();
        }
        return s;
    }));
    ASSERT_OK(m_db->view([](auto &tx) {
        Status s;
        for (size_t i = 0; s.is_ok() && i < kTotalBuckets; ++i) {
            Bucket b;
            s = tx.open_bucket(numeric_key(i), b);
            if (i < kDroppedBuckets) {
                EXPECT_TRUE(s.is_invalid_argument()) << s.to_string();
                s = Status::ok();
            } else if (s.is_ok()) {
                auto *c = tx.new_cursor(b);
                c->seek_first();
                for (size_t j = 0; j < kNumRecords; ++j) {
                    EXPECT_TRUE(c->is_valid());
                    EXPECT_EQ(numeric_key(j), c->key());
                    EXPECT_EQ(numeric_key(j), c->value());
                    c->next();
                }
                delete c;
            }
        }
        return s;
    }));
}

} // namespace calicodb::test

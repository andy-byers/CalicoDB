// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/db.h"
#include "common.h"
#include "encoding.h"
#include "internal.h"
#include "test.h"
#include <filesystem>
#include <gtest/gtest.h>

namespace calicodb::test
{

// These tests use a lot of memory, occasionally crashing GitHub C/I runners.
#ifndef CALICODB_CI
class BoundaryValueTests : public testing::Test
{
protected:
    static constexpr size_t kMaxLen = kMaxAllocation;
    const std::string m_filename;
    char *const m_backing;
    Options m_options;
    DB *m_db = nullptr;

    explicit BoundaryValueTests()
        : m_filename(get_full_filename(testing::TempDir() + "db")),
          m_backing(new char[kMaxLen + 1]())
    {
        remove_calicodb_files(m_filename);
        m_options.auto_checkpoint = 0;
        m_options.page_size = kMaxPageSize;
    }

    ~BoundaryValueTests() override
    {
        delete m_db;
        delete[] m_backing;

        // The files left by this test can be very large. Make sure to clean up.
        remove_calicodb_files(m_filename);
    }

    auto payload(uint32_t size) -> Slice
    {
        EXPECT_LE(size, kMaxLen + 1);
        if (size >= sizeof(uint32_t)) {
            put_u32(m_backing, size);
            put_u32(m_backing + size - sizeof(uint32_t), size);
        }
        return {m_backing, size};
    }

    auto test_boundary_payload(bool test_key, bool test_value) -> void
    {
        const uint32_t key_size = test_key * kMaxLen;
        const uint32_t value_size = test_value * kMaxLen;

        ASSERT_OK(DB::open(m_options, m_filename.c_str(), m_db));
        ASSERT_OK(m_db->update([=](auto &tx) {
            TestCursor c;
            auto s = test_create_and_open_bucket(tx, BucketOptions(), "bucket", c);
            if (s.is_ok()) {
                s = tx.put(*c, payload(key_size), payload(value_size));
            }
            return s;
        }));

        ASSERT_OK(m_db->checkpoint(kCheckpointPassive, nullptr));

        ASSERT_OK(m_db->view([=](const auto &tx) {
            TestCursor c;
            auto s = test_open_bucket(tx, "bucket", c);
            if (s.is_ok()) {
                c->find(payload(key_size));
                EXPECT_TRUE(c->is_valid()) << c->status().message();
                EXPECT_EQ(c->value(), payload(value_size));
            }
            return s;
        }));
    }

    auto test_overflow_payload(bool test_key, bool test_value) -> void
    {
        const uint32_t key_size = test_key * (kMaxLen + 1);
        const uint32_t value_size = test_value * (kMaxLen + 1);

        ASSERT_OK(DB::open(m_options, m_filename.c_str(), m_db));
        ASSERT_OK(m_db->update([=](auto &tx) {
            TestCursor c;
            auto s = test_create_and_open_bucket(tx, BucketOptions(), "bucket", c);
            if (s.is_ok()) {
                EXPECT_TRUE((s = tx.put(*c, payload(key_size), payload(value_size))).is_invalid_argument()) << s.message();
            }
            return Status::ok();
        }));
    }

    auto test_32_bit_overflow(bool auto_checkpoint) -> void
    {
        // Keep the number of iterations low and the payload size high. Otherwise, the WAL grows
        // to be way too large, since we retain many versions of each page. Still, these settings
        // create a ~10 GB WAL.
        static constexpr size_t kNumIterations = 5;
        static constexpr size_t kPayloadSize = 1'000'000'000;
        auto buffer = std::make_unique<char[]>(kPayloadSize);

        // Make a database file that is larger than 4 GiB. Offsets to some locations within the
        // file, as well as the file size itself, should overflow a 32-bit unsigned integer.
        static_assert(kNumIterations * kPayloadSize > std::numeric_limits<uint32_t>::max());

        Options options;
        options.page_size = kMaxPageSize;
        options.auto_checkpoint = auto_checkpoint ? options.auto_checkpoint : 0;
        ASSERT_OK(DB::open(options, m_filename.c_str(), m_db));

        for (size_t i = 0; i < kNumIterations; ++i) {
            ASSERT_OK(m_db->update([&buffer, i](auto &tx) {
                TestCursor c;
                auto s = test_create_and_open_bucket(tx, BucketOptions(), "b", c);
                if (s.is_ok()) {
                    put_u64(buffer.get(), i);
                    s = tx.put(*c,
                               Slice(buffer.get(), kPayloadSize),
                               Slice(buffer.get(), kPayloadSize));
                }
                return s;
            }));
        }
        ASSERT_OK(m_db->checkpoint(kCheckpointRestart, nullptr));
        ASSERT_GT(std::filesystem::file_size(m_filename), kPayloadSize * kNumIterations);
    }
};

TEST_F(BoundaryValueTests, BoundaryBucketName)
{
    ASSERT_OK(DB::open(m_options, m_filename.c_str(), m_db));
    ASSERT_OK(m_db->update([=](auto &tx) {
        TestCursor c;
        return test_create_and_open_bucket(tx, BucketOptions(), payload(kMaxLen), c);
    }));

    ASSERT_OK(m_db->checkpoint(kCheckpointPassive, nullptr));

    ASSERT_OK(m_db->view([=](const auto &tx) {
        TestCursor c;
        return test_open_bucket(tx, payload(kMaxLen), c);
    }));
}

TEST_F(BoundaryValueTests, OverflowBucketName)
{
    ASSERT_OK(DB::open(m_options, m_filename.c_str(), m_db));
    ASSERT_NOK(m_db->update([=](auto &tx) {
        TestCursor c;
        return test_create_and_open_bucket(tx, BucketOptions(), payload(kMaxLen + 1), c);
    }));

    ASSERT_OK(m_db->checkpoint(kCheckpointPassive, nullptr));

    ASSERT_NOK(m_db->view([=](const auto &tx) {
        TestCursor c;
        return test_open_bucket(tx, payload(kMaxLen + 1), c);
    }));
}

TEST_F(BoundaryValueTests, BoundaryKey)
{
    test_boundary_payload(true, false);
}

TEST_F(BoundaryValueTests, BoundaryValue)
{
    test_boundary_payload(false, true);
}

TEST_F(BoundaryValueTests, BoundaryRecord)
{
    test_boundary_payload(true, true);
}

TEST_F(BoundaryValueTests, OverflowKey)
{
    test_overflow_payload(true, false);
}

TEST_F(BoundaryValueTests, OverflowValue)
{
    test_overflow_payload(false, true);
}

TEST_F(BoundaryValueTests, OverflowRecord)
{
    test_overflow_payload(true, true);
}

TEST_F(BoundaryValueTests, Overflow32Bits1)
{
    // Checkpoint the data incrementally.
    test_32_bit_overflow(true);
}

TEST_F(BoundaryValueTests, Overflow32Bits2)
{
    // Checkpoint all the data at once.
    test_32_bit_overflow(false);
}
#endif // CALICODB_CI

class StressTests : public testing::Test
{
protected:
    const std::string m_filename;
    DB *m_db = nullptr;

    explicit StressTests()
        : m_filename(get_full_filename(testing::TempDir() + "calicodb_stress_tests"))
    {
        remove_calicodb_files(m_filename);
    }

    ~StressTests() override
    {
        delete m_db;

        // The files left by this test can be very large. Make sure to clean up.
        remove_calicodb_files(m_filename);
    }
};

TEST_F(StressTests, LotsOfBuckets)
{
    // There isn't really a limit on the number of buckets one can create. Just create a
    // bunch of them.
    static constexpr size_t kNumBuckets = 100'000;
    ASSERT_OK(DB::open(Options(), m_filename.c_str(), m_db));
    ASSERT_OK(m_db->update([](auto &tx) {
        Status s;
        for (size_t i = 0; s.is_ok() && i < kNumBuckets; ++i) {
            TestCursor c;
            const auto name = numeric_key(i);
            s = test_create_and_open_bucket(tx, BucketOptions(), name, c);
            if (s.is_ok()) {
                s = tx.put(*c, name, name);
            }
        }
        return s;
    }));
    ASSERT_OK(m_db->view([](const auto &tx) {
        Status s;
        for (size_t i = 0; s.is_ok() && i < kNumBuckets; ++i) {
            TestCursor c;
            const auto name = numeric_key(i);
            s = test_open_bucket(tx, name, c);
            if (s.is_ok()) {
                c->seek_first();
                EXPECT_TRUE(c->is_valid());
                EXPECT_EQ(name, c->key());
                EXPECT_EQ(name, c->value());
            }
        }
        return s;
    }));
}

TEST_F(StressTests, CursorLimit)
{
    ASSERT_OK(DB::open(Options(), m_filename.c_str(), m_db));
    ASSERT_OK(m_db->update([](auto &tx) {
        Status s;
        std::vector<TestCursor> cursors(1'000);
        for (size_t i = 0; s.is_ok() && i < cursors.size(); ++i) {
            s = test_create_and_open_bucket(tx, BucketOptions(), "bucket", cursors[i]);
            if (s.is_ok()) {
                const auto name = numeric_key(i);
                s = tx.put(*cursors[i], name, name);
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
    ASSERT_OK(DB::open(Options(), m_filename.c_str(), m_db));
    ASSERT_OK(m_db->update([](auto &tx) {
        Status s;
        for (size_t i = 0; s.is_ok() && i < kTotalBuckets; ++i) {
            TestCursor c;
            const auto name = numeric_key(i);
            s = test_create_and_open_bucket(tx, BucketOptions(), name, c);
            for (size_t j = 0; s.is_ok() && j < kNumRecords; ++j) {
                const auto name2 = numeric_key(j);
                s = tx.put(*c, name2, name2);
            }
        }
        for (size_t i = 0; s.is_ok() && i < kDroppedBuckets; ++i) {
            const auto name2 = numeric_key(i);
            s = tx.drop_bucket(name2);
        }
        if (s.is_ok()) {
            // Run a vacuum while there are many buckets open.
            s = tx.vacuum();
        }
        return s;
    }));
    ASSERT_OK(m_db->view([](const auto &tx) {
        Status s;
        for (size_t i = 0; s.is_ok() && i < kTotalBuckets; ++i) {
            TestCursor c;
            const auto name = numeric_key(i);
            s = test_open_bucket(tx, name, c);
            if (i < kDroppedBuckets) {
                EXPECT_TRUE(s.is_invalid_argument()) << s.message();
                s = Status::ok();
            } else if (s.is_ok()) {
                c->seek_first();
                for (size_t j = 0; j < kNumRecords; ++j) {
                    const auto name2 = numeric_key(j);
                    EXPECT_TRUE(c->is_valid());
                    EXPECT_EQ(name2, c->key());
                    EXPECT_EQ(name2, c->value());
                    c->next();
                }
            }
        }
        return s;
    }));
}

} // namespace calicodb::test

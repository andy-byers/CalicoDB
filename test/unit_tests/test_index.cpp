// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/env.h"
#include "calicodb/slice.h"
#include "fake_env.h"
#include "logging.h"
#include "unit_tests.h"
#include <gtest/gtest.h>
#include <random>

namespace calicodb
{

class HashIndexTestBase : public EnvTestHarness<FakeEnv>
{
protected:
    explicit HashIndexTestBase()
    {
        EXPECT_OK(m_env.new_file(kShmFilename, Env::kCreate, m_shm));
        m_index = new HashIndex(m_header, m_shm);
    }

    ~HashIndexTestBase() override
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

class HashIndexTests : public HashIndexTestBase, public testing::Test
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
                const U32 key = random.Next(1, kNumTestFrames);
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

class HashIteratorTests : public HashIndexTestBase, public testing::Test
{
protected:
    ~HashIteratorTests() override = default;
};

#ifndef NDEBUG
TEST_F(HashIteratorTests, EmptyIndexDeathTest)
{
    HashIterator itr(*m_index);
    ASSERT_DEATH((void)itr.init(), kExpectationMatcher);
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
                append(m_num_pages - i);
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

} // namespace calicodb
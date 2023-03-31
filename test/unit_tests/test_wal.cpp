// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/env.h"
#include "calicodb/slice.h"
#include "crc.h"
#include "hash_index.h"
#include "logging.h"
#include "tools.h"
#include "unit_tests.h"
#include <array>
#include <gtest/gtest.h>

namespace calicodb
{

class HashIndexTestBase
{
protected:
    explicit HashIndexTestBase()
        : m_index(m_header)
    {
        std::memset(&m_header, 0, sizeof(m_header));
    }

    virtual ~HashIndexTestBase() = default;

    auto append(U32 key)
    {
        ASSERT_OK(m_index.assign(key, ++m_header.max_frame));
    }

    HashIndexHeader m_header;
    HashIndex m_index;
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
    ASSERT_OK(m_index.lookup(1, min_frame, value));
    ASSERT_FALSE(value);
    ASSERT_OK(m_index.lookup(2, min_frame, value));
    ASSERT_EQ(value, 2);
    ASSERT_OK(m_index.lookup(3, min_frame, value));
    ASSERT_EQ(value, 3);
    ASSERT_OK(m_index.lookup(4, min_frame, value));
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
    ASSERT_OK(m_index.lookup(1, min_frame, value));
    ASSERT_FALSE(value);
    ASSERT_OK(m_index.lookup(4'999, min_frame, value));
    EXPECT_FALSE(value);
    ASSERT_OK(m_index.lookup(5'000, min_frame, value));
    ASSERT_EQ(value, 5'000);
    ASSERT_OK(m_index.lookup(5'500, min_frame, value));
    ASSERT_EQ(value, 5'500);
    ASSERT_OK(m_index.lookup(5'501, min_frame, value));
    ASSERT_FALSE(value);
    ASSERT_OK(m_index.lookup(10'000, min_frame, value));
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
    m_index.cleanup();
    m_header.max_frame = 4;

    ASSERT_OK(m_index.lookup(1, 1, value));
    ASSERT_EQ(value, 1);
    ASSERT_OK(m_index.lookup(2, 1, value));
    ASSERT_EQ(value, 2);
    ASSERT_OK(m_index.lookup(3, 1, value));
    ASSERT_EQ(value, 3);
    ASSERT_OK(m_index.lookup(4, 1, value));
    ASSERT_EQ(value, 4);

    m_header.max_frame = 2;
    m_index.cleanup();
    m_header.max_frame = 4;

    ASSERT_OK(m_index.lookup(1, 1, value));
    ASSERT_EQ(value, 1);
    ASSERT_OK(m_index.lookup(2, 1, value));
    ASSERT_EQ(value, 2);
    ASSERT_OK(m_index.lookup(3, 1, value));
    ASSERT_FALSE(value);
    ASSERT_OK(m_index.lookup(4, 1, value));
    ASSERT_FALSE(value);
}

TEST_F(HashIndexTests, ReadsAndWrites)
{
    std::vector<U32> keys;
    // Write 2 full index tables + a few extra entries.
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
        ASSERT_EQ(m_index.fetch(value), key);
        U32 current;
        ASSERT_OK(m_index.lookup(key, lower, current));
        if (m_header.max_frame < value || value < lower) {
            ASSERT_FALSE(current);
        } else {
            CHECK_EQ(current, value);
        }
        ++value;
    }
}

TEST_F(HashIndexTests, SimulateUsage)
{
    static constexpr std::size_t kNumTestFrames = 10'000;

    tools::RandomGenerator random;
    std::map<U32, U32> simulated;

    for (std::size_t iteration = 0; iteration < 2; ++iteration) {
        U32 lower = 1;
        for (std::size_t frame = 1; frame <= kNumTestFrames; ++frame) {
            if (const auto r = random.Next(10); r == 0) {
                // Run a commit. The calls that validate the page-frame mapping below
                // will ignore frames below "lower". This is not exactly how the WAL works,
                // we actually use 2 index headers, 1 in the index, and 1 in memory. The
                // in-index header's max_frame is used as the position of the last commit.
                lower = m_header.max_frame + 1;
                simulated.clear();
            } else {
                // Perform a write, but only if the page does not already exist in a frame
                // in the range "lower" to "m_header.max_frame", inclusive.
                U32 value;
                const U32 key = random.Next(1, kNumTestFrames);
                ASSERT_OK(m_index.lookup(key, lower, value));
                if (value < lower) {
                    append(key);
                    simulated.insert_or_assign(key, m_header.max_frame);
                }
            }
        }
        U32 result;
        for (const auto &[key, value] : simulated) {
            ASSERT_OK(m_index.lookup(key, lower, result));
            CHECK_EQ(result, value);
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
    ASSERT_DEATH(HashIterator itr(m_index), "expect");
}
#endif // NDEBUG

class HashIteratorParamTests
    : public HashIndexTestBase,
      public testing::TestWithParam<std::tuple<std::size_t, std::size_t>>
{
protected:
    HashIteratorParamTests()
        : m_num_copies(std::get<0>(GetParam())),
          m_num_pages(std::get<1>(GetParam()))
    {
    }

    ~HashIteratorParamTests() override = default;

    auto test_reordering_and_deduplication()
    {
        m_header.max_frame = 0;
        m_index.cleanup();

        for (std::size_t d = 0; d < m_num_copies; ++d) {
            for (std::size_t i = 0; i < m_num_pages; ++i) {
                append(m_num_pages - i);
            }
        }
        HashIterator itr(m_index);
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

static constexpr std::size_t kTestEntryCounts[] = {1, 2, 3, 5, 10, 100, 10'000, 100'000};

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

class RandomDirtyListBuilder
{
public:
    explicit RandomDirtyListBuilder(std::size_t page_size)
        : m_page_size(page_size),
          m_random(page_size * 32)
    {
    }

    // NOTE: Invalidates dirty lists previously obtained through this method. The pgno vector must not
    //       have any duplicate page numbers.
    auto build(const std::vector<U32> &pgno, std::vector<CacheEntry> &out) -> void
    {
        CALICODB_EXPECT_FALSE(pgno.empty());
        out.resize(pgno.size());

        for (std::size_t i = 0; i < out.size(); ++i) {
            while (pgno[i] * m_page_size > m_pages.size()) {
                m_pages.append(std::string(m_page_size, '\0'));
            }
            std::memcpy(
                m_pages.data() + (pgno[i] - 1) * m_page_size,
                m_random.Generate(m_page_size).data(),
                m_page_size);

            out[i].page_id = Id(pgno[i]);
            out[i].is_dirty = true;
            if (i != 0) {
                out[i].prev = &out[i - 1];
            }
            if (i < out.size() - 1) {
                out[i].next = &out[i + 1];
            }
        }
        for (auto &d : out) {
            d.page = m_pages.data() + d.page_id.as_index() * m_page_size;
        }
    }

    [[nodiscard]] auto data() const -> Slice
    {
        return m_pages;
    }

private:
    std::string m_pages;
    tools::RandomGenerator m_random;
    std::size_t m_page_size = 0;
};

class WalTestBase : public InMemoryTest
{
protected:
    static constexpr std::size_t kPageSize = kMinPageSize;
    static constexpr std::size_t kWalHeaderSize = 32;
    static constexpr std::size_t kFrameSize = kPageSize + 24;

    WalTestBase()
    {
        Wal::Parameters param = {
            .filename = kFilename,
            .page_size = kPageSize,
            .env = env.get(),
        };
        EXPECT_OK(Wal::open(param, m_wal));
    }

    ~WalTestBase() override
    {
        close();
    }

    auto close() -> void
    {
        ASSERT_OK(Wal::close(m_wal));
        ASSERT_EQ(m_wal, nullptr);
    }

    Wal *m_wal = nullptr;
};

class WalTests
    : public WalTestBase,
      public testing::Test
{
protected:
    ~WalTests() override = default;
};

TEST_F(WalTests, EmptyWalIsRemovedOnClose)
{
    ASSERT_TRUE(env->file_exists(kFilename));
    close();
    ASSERT_FALSE(env->file_exists(kFilename));
}

class WalParamTests
    : public WalTestBase,
      public testing::TestWithParam<std::tuple<bool, std::size_t, std::size_t>>
{
protected:
    static constexpr std::size_t kPageSize = kMinPageSize;
    static constexpr std::size_t kWalHeaderSize = 32;
    static constexpr std::size_t kFrameSize = kPageSize + 24;

    WalParamTests()
        : m_builder(kPageSize),
          m_saved(kPageSize)
    {
    }

    ~WalParamTests() override = default;

    auto test_write_and_read_back() -> void
    {
        const auto commit = std::get<0>(GetParam());
        const auto duplicates = std::get<1>(GetParam());
        const auto num_pages = std::get<2>(GetParam());

        for (std::size_t iteration = 0; iteration < duplicates; ++iteration) {
            std::vector<U32> pgno(num_pages);
            std::default_random_engine rng(42);
            std::iota(begin(pgno), end(pgno), 1);
            std::shuffle(begin(pgno), end(pgno), rng);

            std::vector<CacheEntry> dirty;
            m_builder.build(pgno, dirty);
            auto db_data = m_builder.data();
            auto db_size = db_data.size() * commit;
            ASSERT_OK(m_wal->write(&dirty[0], db_size));
            char buffer[kPageSize];
            for (auto pg : pgno) {
                ASSERT_OK(m_wal->read(Id(pg), buffer));
                const Slice from_wal(buffer, kPageSize);
                const auto most_recent =
                    db_data.range((pg - 1) * kPageSize, kPageSize);
                CHECK_EQ(from_wal.to_string(), most_recent.to_string());
            }

            if (commit) {
                m_saved = m_builder;
            }
        }
    }

    RandomDirtyListBuilder m_builder;
    RandomDirtyListBuilder m_saved;
};

TEST_P(WalParamTests, WriteAndReadBack)
{
    test_write_and_read_back();
}

INSTANTIATE_TEST_SUITE_P(
    WalParamTests,
    WalParamTests,
    ::testing::Values(
        std::make_tuple(false, 1, 1),
        std::make_tuple(false, 1, 2),
        std::make_tuple(false, 1, 3),
        std::make_tuple(false, 1, 10),
        std::make_tuple(false, 1, 100),
        std::make_tuple(false, 1, 10'000),
        std::make_tuple(false, 1, 20'000),
        std::make_tuple(false, 5, 1),
        std::make_tuple(false, 5, 2),
        std::make_tuple(false, 5, 3),
        std::make_tuple(false, 5, 10),
        std::make_tuple(false, 5, 100),
        std::make_tuple(false, 5, 10'000),
        std::make_tuple(false, 5, 20'000),
        std::make_tuple(true, 1, 1),
        std::make_tuple(true, 1, 2),
        std::make_tuple(true, 1, 3),
        std::make_tuple(true, 1, 10),
        std::make_tuple(true, 1, 100),
        std::make_tuple(true, 1, 10'000),
        std::make_tuple(true, 1, 20'000),
        std::make_tuple(true, 5, 1),
        std::make_tuple(true, 5, 2),
        std::make_tuple(true, 5, 3),
        std::make_tuple(true, 5, 10),
        std::make_tuple(true, 5, 100),
        std::make_tuple(true, 5, 10'000),
        std::make_tuple(true, 5, 20'000)));

} // namespace calicodb
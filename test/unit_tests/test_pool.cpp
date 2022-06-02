
#include <numeric>

#include <gtest/gtest.h>

#include "utils/layout.h"
#include "page/page.h"
#include "pool/buffer_pool.h"
#include "pool/frame.h"
#include "pool/in_memory.h"
#include "wal/interface.h"
#include "fakes.h"

namespace cub {

class BufferPoolTestsBase: public testing::Test {
public:
    static constexpr Size frame_count {32};

    explicit BufferPoolTestsBase(std::unique_ptr<ReadWriteMemory> file)
    {
        memory = file->memory();
        WALHarness harness {0x100};

        pool = std::make_unique<BufferPool>(BufferPool::Parameters{
            std::move(file),
            std::move(harness.reader),
            std::move(harness.writer),
            LSN {1'000},
            0x10,
            0,
            0x100,
        });
    }

    ~BufferPoolTestsBase() override = default;

    Random random {0};
    SharedMemory memory;
    std::unique_ptr<IBufferPool> pool;
};

class BufferPoolTests: public BufferPoolTestsBase {
public:
    BufferPoolTests(): BufferPoolTestsBase {std::make_unique<ReadWriteMemory>()} {}

    ~BufferPoolTests() override = default;
};

TEST_F(BufferPoolTests, FreshBufferPoolIsEmpty)
{
    ASSERT_EQ(pool->page_count(), 0);
}

TEST_F(BufferPoolTests, AllocationInceasesPageCount)
{
    (void)pool->allocate(PageType::EXTERNAL_NODE);
    ASSERT_EQ(pool->page_count(), 1);
    (void)pool->allocate(PageType::EXTERNAL_NODE);
    ASSERT_EQ(pool->page_count(), 2);
    (void)pool->allocate(PageType::EXTERNAL_NODE);
    ASSERT_EQ(pool->page_count(), 3);
}


TEST_F(BufferPoolTests, AllocateReturnsCorrectPage)
{
    auto page = pool->allocate(PageType::EXTERNAL_NODE);
    ASSERT_EQ(page.id(), PID::root());
    ASSERT_EQ(page.type(), PageType::EXTERNAL_NODE);
    ASSERT_TRUE(page.is_dirty());
}

TEST_F(BufferPoolTests, AcquireReturnsCorrectPage)
{
    (void) pool->allocate(PageType::EXTERNAL_NODE);
    auto page = pool->acquire(PID::root(), true);
    ASSERT_EQ(page.id(), PID::root());
    ASSERT_EQ(page.type(), PageType::EXTERNAL_NODE);
}

TEST_F(BufferPoolTests, PagesGetsReleasedOnDestruction)
{
    (void) pool->allocate(PageType::EXTERNAL_NODE);
    (void) pool->allocate(PageType::EXTERNAL_NODE);

    auto page = pool->acquire(PID {1}, true);

    // This should cause page 1 to be released.
    page = pool->acquire(PID {2}, true);

    // We should be okay to acquire page 1 again. If it wasn't released, this will fail.
    (void) pool->acquire(PID {1}, true);
}

TEST_F(BufferPoolTests, AcquireMultipleWritablePagesDeathTest)
{
    auto page = pool->allocate(PageType::EXTERNAL_NODE);
    ASSERT_DEATH(auto unused = pool->acquire(PID::root(), true), "Expect");
}

TEST_F(BufferPoolTests, AcquireReadableAndWritablePagesDeathTest)
{
    auto page = pool->allocate(PageType::EXTERNAL_NODE);
    ASSERT_DEATH(auto unused = pool->acquire(PID::root(), false), "Expect");
}

auto write_to_page(Page &page, const std::string &message) -> void
{
    const auto offset = PageLayout::content_offset(page.id());
    CUB_EXPECT_LE(offset + message.size(), page.size());
    page.write(_b(message), offset);
}

template<class Page> auto read_from_page(const Page &page, Size size) -> std::string
{
    const auto offset = PageLayout::content_offset(page.id());
    CUB_EXPECT_LE(offset + size, page.size());
    auto message = std::string(size, '\x00');
    page.read(_b(message), offset);
    return message;
}

TEST_F(BufferPoolTests, PageDataPersistsBetweenAcquires)
{
    {
        auto in_page = pool->allocate(PageType::EXTERNAL_NODE);
        write_to_page(in_page, "Hello, world!");
    }

    auto out_page = pool->acquire(PID::root(), false);
    ASSERT_EQ(read_from_page(out_page, 13), "Hello, world!");
}

TEST_F(BufferPoolTests, PageDataPersistsAfterEviction)
{
    const auto n = frame_count * 2;
    for (Index i {}; i < n; ++i) {
        auto in_page = pool->allocate(PageType::EXTERNAL_NODE);
        write_to_page(in_page, "Hello, world!");
    }
    for (Index i {}; i < n; ++i) {
        auto out_page = pool->acquire(PID {i + 1}, false);
        ASSERT_EQ(read_from_page(out_page, 13), "Hello, world!");
    }
}

template<class Test> auto run_sanity_check(Test &test, Size num_iterations)
{
    for (Index i {}; i < num_iterations; ++i) {
        if (const auto r = test.random.next_int(1); r == 0) {
            auto page = test.pool->allocate(PageType::EXTERNAL_NODE);
            write_to_page(page, std::to_string(page.id().value));
        } else if (test.pool->page_count()) {
            const auto id = test.random.next_int(1UL, test.pool->page_count());
            const auto result = std::to_string(id);
            auto page = test.pool->acquire(PID {id}, false);
            ASSERT_EQ(read_from_page(page, result.size()), result);
        }
    }
}

TEST_F(BufferPoolTests, SanityCheck)
{
    run_sanity_check(*this, 1'000);
}

class InMemoryTests: public testing::Test {
public:
    static constexpr Size page_size = 0x200;

    InMemoryTests()
        : pool {std::make_unique<InMemory>(page_size)} {}

    Random random {0};
    std::unique_ptr<InMemory> pool;
};

TEST_F(InMemoryTests, FreshPoolIsEmpty)
{
    ASSERT_EQ(pool->page_count(), 0);
}

TEST_F(InMemoryTests, SanityCheck)
{
    run_sanity_check(*this, 1'000);
}

TEST_F(InMemoryTests, AbortDiscardsChangesSincePreviousCommit)
{
    {
        auto page_1 = pool->allocate(PageType::EXTERNAL_NODE);
        write_to_page(page_1, "1");
        auto page_2 = pool->allocate(PageType::EXTERNAL_NODE);
        write_to_page(page_2, "2");
    }
    pool->commit();
    {
        auto page_3 = pool->allocate(PageType::EXTERNAL_NODE);
        write_to_page(page_3, "3");
        auto page_4 = pool->allocate(PageType::EXTERNAL_NODE);
        write_to_page(page_4, "4");
    }
    pool->abort();

    auto page_1 = pool->acquire(PID {1}, false);
    auto page_2 = pool->acquire(PID {2}, false);
    auto page_3 = pool->acquire(PID {3}, false);
    auto page_4 = pool->acquire(PID {4}, false);
    ASSERT_EQ(read_from_page(page_1, 1), "1");
    ASSERT_EQ(read_from_page(page_2, 1), "2");
    ASSERT_NE(read_from_page(page_3, 1), "3");
    ASSERT_NE(read_from_page(page_4, 1), "4");
}

} // cub
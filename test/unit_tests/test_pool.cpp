
#include <numeric>
#include <gtest/gtest.h>
#include "page/page.h"
#include "pool/buffer_pool.h"
#include "pool/frame.h"
#include "pool/in_memory.h"
#include "utils/layout.h"
#include "utils/logging.h"
#include "wal/interface.h"
#include "fakes.h"

namespace calico {

//TEST(PagerSetupTests, ReportsOutOfRangeFrameCount)
//{
//    ASSERT_THROW(Pager({std::make_unique<ReadWriteMemory>(), 0x100, MINIMUM_FRAME_COUNT - 1}), std::invalid_argument);
//    ASSERT_THROW(Pager({std::make_unique<ReadWriteMemory>(), 0x100, MAXIMUM_FRAME_COUNT + 1}), std::invalid_argument);
//}

class PagerTests: public testing::Test {
public:
    static constexpr Size frame_count {32};
    static constexpr Size page_size {0x100};

    explicit PagerTests()
        : bank {std::make_unique<MemoryBank>("PagerTests")}
    {
        file = bank->open_memory(DATA_NAME, Mode::CREATE | Mode::READ_WRITE, 0666);
        memory = file->shared_memory();
        pager = std::make_unique<Pager>(Pager::Parameters{
            file->open_reader(),
            file->open_writer(),
            logging::create_sink("", 0),
            page_size,
            frame_count,
        });
    }

    ~PagerTests() override = default;

    Random random {0};
    SharedMemory memory;
    std::unique_ptr<MemoryBank> bank;
    std::unique_ptr<Memory> file;
    std::unique_ptr<Pager> pager;
};

TEST_F(PagerTests, FreshPagerHasAllFramesAvailable)
{
    ASSERT_EQ(pager->available(), frame_count);
}

TEST_F(PagerTests, FreshPagerIsSetUpCorrectly)
{
    ASSERT_EQ(pager->page_size(), page_size);
}

TEST_F(PagerTests, PagerKeepsTrackOfAvailableCount)
{
    auto frame = pager->pin(PID::root());
    ASSERT_EQ(pager->available(), frame_count - 1);
    pager->discard(std::move(*frame));
    ASSERT_EQ(pager->available(), frame_count);
}

TEST_F(PagerTests, CannotPinMorePagesThanAvailable)
{
    Index i {ROOT_ID_VALUE};
    for (; i <= frame_count; )
        (void)pager->pin(PID {i++});
    ASSERT_EQ(pager->pin(PID {i}), std::nullopt);
}

TEST_F(PagerTests, TruncateResizesUnderlyingFile)
{
    pager->unpin(*pager->pin(PID::root()));
    ASSERT_NE(memory.memory().size(), page_size);
    pager->truncate(0);
    ASSERT_EQ(memory.memory().size(), 0);
}

class BufferPoolTestsBase: public testing::Test {
public:
    static constexpr Size frame_count {32};
    static constexpr Size page_size {0x100};

    LSN static_lsn {LSN::max()};

    explicit BufferPoolTestsBase(std::unique_ptr<MemoryBank> bank)
    {
        auto file = bank->open_memory(DATA_NAME, Mode::CREATE | Mode::READ_WRITE, 0666);
        memory = file->shared_memory();
        pool = std::make_unique<BufferPool>(BufferPool::Parameters{
            *bank, // Will open a file that shares memory with `memory` member.
            nullptr,
            nullptr,
            logging::create_sink("", 0),
            static_lsn,
            frame_count,
            0,
            page_size,
            false,
        });
    }

    ~BufferPoolTestsBase() override = default;

    Random random {0};
    SharedMemory memory;
    std::unique_ptr<IBufferPool> pool;
};

class BufferPoolTests: public BufferPoolTestsBase {
public:
    BufferPoolTests(): BufferPoolTestsBase {std::make_unique<MemoryBank>("BufferPoolTests")} {}

    ~BufferPoolTests() override = default;
};

TEST_F(BufferPoolTests, FreshBufferPoolIsEmpty)
{
    ASSERT_EQ(pool->page_count(), 0);
}

TEST_F(BufferPoolTests, FreshPoolIsSetUpCorrectly)
{
    ASSERT_EQ(pool->page_size(), page_size);
    ASSERT_EQ(pool->block_size(), 0);
    ASSERT_EQ(pool->hit_ratio(), 0.0);
    ASSERT_EQ(pool->flushed_lsn(), static_lsn);
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

TEST_F(BufferPoolTests, LoadsFileHeaderFields)
{
    std::string backing(FileLayout::HEADER_SIZE, '\x00');
    FileHeader header {stob(backing)};
    header.set_page_count(123);
    header.set_flushed_lsn(LSN {456});
    pool->load_header(header);

    ASSERT_EQ(pool->page_count(), 123);
}

TEST_F(BufferPoolTests, SavesFileHeaderFields)
{
    std::string backing(FileLayout::HEADER_SIZE, '\x00');
    FileHeader header {stob(backing)};

    (void)pool->allocate(PageType::EXTERNAL_NODE);
    pool->save_header(header);
    ASSERT_EQ(header.page_count(), 1);
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
    CALICO_EXPECT_LE(offset + message.size(), page.size());
    page.write(stob(message), offset);
}

template<class Page> auto read_from_page(const Page &page, Size size) -> std::string
{
    const auto offset = PageLayout::content_offset(page.id());
    CALICO_EXPECT_LE(offset + size, page.size());
    auto message = std::string(size, '\x00');
    page.read(stob(message), offset);
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

template<class Test> auto run_sanity_check(Test &test, Size num_iterations) -> Size
{
    Size num_updates {};
    for (Index i {}; i < num_iterations; ++i) {
        if (test.random.next_int(1) == 0) {
            auto page = test.pool->allocate(PageType::EXTERNAL_NODE);
            write_to_page(page, std::to_string(page.id().value));
            num_updates++;
        } else if (test.pool->page_count()) {
            const auto id = test.random.next_int(Size {1}, test.pool->page_count());
            const auto result = std::to_string(id);
            auto page = test.pool->acquire(PID {id}, false);
            EXPECT_EQ(read_from_page(page, result.size()), result);
        }
    }
    return num_updates;
}

TEST_F(BufferPoolTests, SanityCheck)
{
    run_sanity_check(*this, 1'000);
}

TEST_F(BufferPoolTests, KeepsTrackOfHitRatio)
{
    run_sanity_check(*this, 10);
    ASSERT_NE(pool->hit_ratio(), 0.0);
}

class InMemoryTests: public testing::Test {
public:
    static constexpr Size page_size = 0x200;

    InMemoryTests()
        : pool {std::make_unique<InMemory>(page_size, true, logging::create_sink("", 0))} {}

    Random random {0};
    std::unique_ptr<InMemory> pool;
};

TEST_F(InMemoryTests, FreshInMemoryPoolIsEmpty)
{
    ASSERT_EQ(pool->page_count(), 0);
}

TEST_F(InMemoryTests, FreshInMemoryPoolIsSetUpCorrectly)
{
    ASSERT_EQ(pool->page_size(), page_size);
    ASSERT_EQ(pool->block_size(), page_size);
    ASSERT_EQ(pool->hit_ratio(), 1.0);
    ASSERT_EQ(pool->flushed_lsn(), LSN::null());
}

TEST_F(InMemoryTests, StubMethodsDoNothing)
{
    ASSERT_TRUE(pool->recover());
    ASSERT_TRUE(pool->try_flush());
    ASSERT_TRUE(pool->try_flush_wal());
    pool->purge();
}

TEST_F(InMemoryTests, LoadsRequiredFileHeaderFields)
{
    std::string backing(FileLayout::HEADER_SIZE, '\x00');
    FileHeader header {stob(backing)};
    header.set_page_count(123);
    header.set_flushed_lsn(LSN {456});
    pool->load_header(header);

    ASSERT_EQ(pool->page_count(), 123);
    ASSERT_EQ(pool->flushed_lsn(), LSN::null());
}

TEST_F(InMemoryTests, SavesRequiredFileHeaderFields)
{
    std::string backing(FileLayout::HEADER_SIZE, '\x00');
    FileHeader header {stob(backing)};

    (void)pool->allocate(PageType::EXTERNAL_NODE);
    pool->save_header(header);
    ASSERT_EQ(header.page_count(), 1);
    ASSERT_EQ(header.flushed_lsn(), LSN::null());
}

TEST_F(InMemoryTests, SanityCheck)
{
    run_sanity_check(*this, 1'000);
}

TEST_F(InMemoryTests, HitRatioIsAlwaysOne)
{
    run_sanity_check(*this, 10);
    ASSERT_EQ(pool->hit_ratio(), 1.0);
}

TEST_F(InMemoryTests, FlushedLSNIsAlwaysNull)
{
    run_sanity_check(*this, 10);
    ASSERT_TRUE(pool->flushed_lsn().is_null());
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

} // calico
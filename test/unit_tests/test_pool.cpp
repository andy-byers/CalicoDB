
#include <numeric>
#include <gtest/gtest.h>
#include "page/page.h"
#include "page/file_header.h"
#include "pool/buffer_pool.h"
#include "pool/memory_pool.h"
#include "pool/pager.h"
#include "utils/layout.h"
#include "utils/logging.h"
#include "fakes.h"

namespace cco {

using namespace page;
using namespace utils;

class PagerTests: public testing::Test {
public:
    static constexpr Size frame_count {32};
    static constexpr Size page_size {0x100};

    explicit PagerTests()
        : home {std::make_unique<FakeDirectory>("PagerTests")}
    {
        file = home->open_fake_file(DATA_NAME, Mode::CREATE | Mode::READ_WRITE, 0666);
        memory = file->shared_memory();
        pager = Pager::open({
            std::move(file),
            page_size,
            frame_count,
        }).value();
    }

    ~PagerTests() override = default;

    Random random {0};
    SharedMemory memory;
    std::unique_ptr<FakeDirectory> home;
    std::unique_ptr<FakeFile> file;
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

TEST_F(PagerTests, PagerCreatesExtraPagesOnDemand)
{
    Index i {ROOT_ID_VALUE};
    for (; i < frame_count * 2; )
        ASSERT_TRUE(pager->pin(PID {i++}));
}

TEST_F(PagerTests, TruncateResizesUnderlyingFile)
{
    ASSERT_TRUE(pager->unpin(*pager->pin(PID::root())));
    ASSERT_NE(memory.memory().size(), page_size);
    ASSERT_TRUE(pager->truncate(0));
    ASSERT_EQ(memory.memory().size(), 0);
}

class BufferPoolTestsBase: public testing::Test {
public:
    static constexpr Size frame_count {32};
    static constexpr Size page_size {0x100};

    explicit BufferPoolTestsBase(std::unique_ptr<FakeDirectory> memory_home):
          home {std::move(memory_home)}
    {
        auto file = home->open_fake_file(DATA_NAME, Mode::CREATE | Mode::READ_WRITE, 0666);
        memory = file->shared_memory();
        pool = *BufferPool::open({
            *home,
            create_sink(),
            LSN::null(),
            frame_count,
            0,
            page_size,
            false,
        });
    }

    ~BufferPoolTestsBase() override = default;

    Random random {0};
    SharedMemory memory;
    std::unique_ptr<FakeDirectory> home;
    std::unique_ptr<IBufferPool> pool;
};

class BufferPoolTests: public BufferPoolTestsBase {
public:
    BufferPoolTests():
          BufferPoolTestsBase {std::make_unique<FakeDirectory>("BufferPoolTests")} {}

    ~BufferPoolTests() override = default;
};

TEST_F(BufferPoolTests, FreshBufferPoolIsEmpty)
{
    ASSERT_EQ(pool->page_count(), 0);
}

TEST_F(BufferPoolTests, FreshPoolIsSetUpCorrectly)
{
    ASSERT_EQ(pool->page_size(), page_size);
    ASSERT_EQ(pool->hit_ratio(), 0.0);
}

TEST_F(BufferPoolTests, AllocationInceasesPageCount)
{
    ASSERT_TRUE(pool->release(*pool->allocate()));
    ASSERT_EQ(pool->page_count(), 1);
    ASSERT_TRUE(pool->release(*pool->allocate()));
    ASSERT_EQ(pool->page_count(), 2);
    ASSERT_TRUE(pool->release(*pool->allocate()));
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

    ASSERT_TRUE(pool->release(*pool->allocate()));
    pool->save_header(header);
    ASSERT_EQ(header.page_count(), 1);
}

TEST_F(BufferPoolTests, AllocateReturnsCorrectPage)
{
    auto page = pool->allocate();
    ASSERT_EQ(page->id(), PID::root());

}

TEST_F(BufferPoolTests, AcquireReturnsCorrectPage)
{
    ASSERT_TRUE(pool->release(*pool->allocate()));
    auto page = pool->acquire(PID::root(), true);
    ASSERT_EQ(page->id(), PID::root());
}

TEST_F(BufferPoolTests, AcquireMultipleWritablePagesDeathTest)
{
    auto page = pool->allocate();
    ASSERT_DEATH(auto unused = pool->acquire(PID::root(), true), "Expect");
}

TEST_F(BufferPoolTests, AcquireReadableAndWritablePagesDeathTest)
{
    auto page = pool->allocate();
    ASSERT_DEATH(auto unused = pool->acquire(PID::root(), false), "Expect");
}

auto write_to_page(Page &page, const std::string &message) -> void
{
    const auto offset = PageLayout::content_offset(page.id());
    CCO_EXPECT_LE(offset + message.size(), page.size());
    page.write(stob(message), offset);
}

auto read_from_page(const Page &page, Size size) -> std::string
{
    const auto offset = PageLayout::content_offset(page.id());
    CCO_EXPECT_LE(offset + size, page.size());
    auto message = std::string(size, '\x00');
    page.read(stob(message), offset);
    return message;
}

TEST_F(BufferPoolTests, PageDataPersistsBetweenAcquires)
{
    auto in_page = pool->allocate();
    write_to_page(*in_page, "Hello, world!");
    ASSERT_TRUE(pool->release(std::move(*in_page)));
    auto out_page = pool->acquire(PID::root(), false);
    ASSERT_EQ(read_from_page(*out_page, 13), "Hello, world!");
    ASSERT_TRUE(pool->release(std::move(*out_page)));
}

TEST_F(BufferPoolTests, PageDataPersistsAfterEviction)
{
    const auto n = frame_count * 2;
    for (Index i {}; i < n; ++i) {
        auto in_page = pool->allocate();
        write_to_page(*in_page, "Hello, world!");
        ASSERT_TRUE(pool->release(std::move(*in_page)));
    }
    for (Index i {}; i < n; ++i) {
        auto out_page = pool->acquire(PID {i + 1}, false);
        ASSERT_EQ(read_from_page(*out_page, 13), "Hello, world!");
        ASSERT_TRUE(pool->release(std::move(*out_page)));
    }
}

template<class Test> auto run_sanity_check(Test &test, Size num_iterations) -> Size
{
    Size num_updates {};
    for (Index i {}; i < num_iterations; ++i) {
        if (test.random.next_int(1) == 0) {
            auto page = test.pool->allocate();
            write_to_page(*page, std::to_string(page->id().value));
            num_updates++;
            EXPECT_TRUE(test.pool->release(std::move(*page)));
        } else if (test.pool->page_count()) {
            const auto id = test.random.next_int(Size {1}, test.pool->page_count());
            const auto result = std::to_string(id);
            auto page = test.pool->acquire(PID {id}, false);
            EXPECT_EQ(read_from_page(*page, result.size()), result);
            EXPECT_TRUE(test.pool->release(std::move(*page)));
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

class MemoryPoolTests: public testing::Test {
public:
    static constexpr Size page_size = 0x200;

    MemoryPoolTests(): 
          pool {std::make_unique<MemoryPool>(page_size, true, create_sink())} {}

    Random random {0};
    std::unique_ptr<MemoryPool> pool;
};

TEST_F(MemoryPoolTests, FreshMemoryPoolIsEmpty)
{
    ASSERT_EQ(pool->page_count(), 0);
}

TEST_F(MemoryPoolTests, FreshMemoryPoolIsSetUpCorrectly)
{
    ASSERT_EQ(pool->page_size(), page_size);
    ASSERT_EQ(pool->hit_ratio(), 1.0);
}

TEST_F(MemoryPoolTests, StubMethodsWork)
{
    ASSERT_TRUE(pool->flush());
    pool->purge();
}

TEST_F(MemoryPoolTests, LoadsRequiredFileHeaderFields)
{
    std::string backing(FileLayout::HEADER_SIZE, '\x00');
    FileHeader header {stob(backing)};
    header.set_page_count(123);
    pool->load_header(header);

    ASSERT_EQ(pool->page_count(), 123);
}

TEST_F(MemoryPoolTests, SavesRequiredFileHeaderFields)
{
    std::string backing(FileLayout::HEADER_SIZE, '\x00');
    FileHeader header {stob(backing)};

    ASSERT_TRUE(pool->release(*pool->allocate()));
    pool->save_header(header);
    ASSERT_EQ(header.page_count(), 1);
}

TEST_F(MemoryPoolTests, SanityCheck)
{
    run_sanity_check(*this, 1'000);
}

TEST_F(MemoryPoolTests, HitRatioIsAlwaysOne)
{
    run_sanity_check(*this, 10);
    ASSERT_EQ(pool->hit_ratio(), 1.0);
}

} // calico

#include <numeric>

#include <gtest/gtest.h>

#include "utils/layout.h"
#include "page/page.h"
#include "pool/buffer_pool.h"
#include "pool/frame.h"
#include "pool/pager.h"
#include "wal/interface.h"
#include "fakes.h"

namespace cub {

class BufferPoolTestsBase: public testing::Test {
public:
    BufferPoolTestsBase(std::unique_ptr<ReadWriteMemory> file)
    {
        m_memory = file->memory();
        WALHarness harness {0x100};

        m_pool = std::make_unique<BufferPool>(BufferPool::Parameters{
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

    static auto write_to_page(Page &page, const std::string &message) -> void
    {
        const auto offset = PageLayout::content_offset(page.id());
        CUB_EXPECT_LE(offset + message.size(), page.size());
        page.write(to_bytes(message), offset);
    }

    template<class Page> static auto read_from_page(const Page &page, Size size) -> std::string
    {
        const auto offset = PageLayout::content_offset(page.id());
        CUB_EXPECT_LE(offset + size, page.size());
        auto message = std::string(size, '\x00');
        page.read(to_bytes(message), offset);
        return message;
    }
    
    SharedMemory m_memory;
    std::unique_ptr<IBufferPool> m_pool;
    Size m_frame_count{32};
};

class BufferPoolTests: public BufferPoolTestsBase {
public:
    BufferPoolTests(): BufferPoolTestsBase{std::make_unique<ReadWriteMemory>()} {}

    ~BufferPoolTests() override = default;
};

TEST_F(BufferPoolTests, AllocatesPage)
{
    auto page = m_pool->allocate(PageType::EXTERNAL_NODE);
    ASSERT_EQ(page.id(), PID::root());
    ASSERT_EQ(page.type(), PageType::EXTERNAL_NODE);
    ASSERT_TRUE(page.is_dirty());
}

TEST_F(BufferPoolTests, AcquiresPage)
{
    (void)m_pool->allocate(PageType::EXTERNAL_NODE);
    
    auto page = m_pool->acquire(PID::root(), true);
    ASSERT_EQ(page.id(), PID::root());
    ASSERT_EQ(page.type(), PageType::EXTERNAL_NODE);
}

TEST_F(BufferPoolTests, PageDataPersistsAfterRelease)
{
    {
        auto in_page = m_pool->allocate(PageType::EXTERNAL_NODE);
        write_to_page(in_page, "Hello, world!");
    }

    auto out_page = m_pool->acquire(PID::root(), false);
    ASSERT_EQ(read_from_page(out_page, 13), "Hello, world!");
}

TEST_F(BufferPoolTests, PageDataPersistsAfterEviction)
{
    const auto n = m_frame_count * 2;
    for (Index i{}; i < n; ++i) {
        auto in_page = m_pool->allocate(PageType::EXTERNAL_NODE);
        write_to_page(in_page, "Hello, world!");
    }
    for (Index i{}; i < n; ++i) {
        auto out_page = m_pool->acquire(PID {i + 1}, false);
        ASSERT_EQ(read_from_page(out_page, 13), "Hello, world!");
    }
}

TEST_F(BufferPoolTests, SanityCheck)
{
    Random random {0};
    for (Index i {}; i < 1'000; ++i) {
        if (const auto r = random.next_int(1); r == 0) {
            auto page = m_pool->allocate(PageType::EXTERNAL_NODE);
            write_to_page(page, "Hello, world!");
        } else if (m_pool->page_count()) {
            const auto id = random.next_int(1UL, m_pool->page_count());
            auto page = m_pool->acquire(PID {id}, false);
            ASSERT_EQ(read_from_page(page, 13), "Hello, world!");
        }
    }
}

} // Cub

#include <gtest/gtest.h>

#include "page/page.h"
#include "pool/frame.h"
#include "pool/cache.h"

namespace {

using namespace cub;

class PageCacheTests: public testing::Test {
public:
    const LSN LARGE_LSN {1'000'000'000};

    ~PageCacheTests() override = default;

    auto make_frame(PID page_id, LSN page_lsn = LSN::null()) -> Frame
    {
        m_backing.emplace_back();
        m_backing.back().resize(m_frame_size);

        Frame frame {m_frame_size};
        frame.reset(page_id);

        if (!page_lsn.is_null()) {
            auto page = frame.borrow(nullptr, true);
            page.set_lsn(page_lsn);
            frame.synchronize(page);
        }
        return frame;
    }

    Size m_frame_size {0x100};
    std::vector<std::string> m_backing;
    PageCache m_cache;
};

TEST_F(PageCacheTests, PutFrame)
{
    m_cache.put(make_frame(PID::root()));
    ASSERT_TRUE(m_cache.contains(PID::root()));
    ASSERT_EQ(m_cache.size(), 1);
}

TEST_F(PageCacheTests, ExtractFrame)
{
    m_cache.put(make_frame(PID::root()));
    ASSERT_EQ(m_cache.extract(PID::root())->page_id(), PID::root());
    ASSERT_EQ(m_cache.size(), 0);
}

TEST_F(PageCacheTests, EvictFromEmptyCacheDoesNothing)
{
    ASSERT_EQ(m_cache.evict(LARGE_LSN), std::nullopt);
}

TEST_F(PageCacheTests, EvictUntilEmpty)
{
    m_cache.put(make_frame(PID::root()));
    ASSERT_NE(m_cache.evict(LARGE_LSN), std::nullopt);
    ASSERT_EQ(m_cache.evict(LARGE_LSN), std::nullopt);
    ASSERT_EQ(m_cache.size(), 0);
}

TEST_F(PageCacheTests, OnlyEvictsFlushedDirtyPages)
{
    const auto make_dirty_frame = [&](Index i) {
        auto frame = make_frame(PID {i}, LSN {i});
        auto page = frame.borrow(nullptr, true);
        page.mut_range(0)[0] = page.range(0)[0];
        frame.synchronize(page);
        EXPECT_TRUE(frame.is_dirty());
        return frame;
    };
    m_cache.put(make_dirty_frame(1));
    m_cache.put(make_dirty_frame(2));
    m_cache.put(make_dirty_frame(3));

    const auto frame_1 = m_cache.evict(LSN {1});
    ASSERT_EQ(frame_1->page_id(), PID {1});
    ASSERT_EQ(m_cache.evict(LSN {1}), std::nullopt);

    const auto frame_2 = m_cache.evict(LSN {2});
    ASSERT_EQ(frame_2->page_id(), PID {2});
    ASSERT_EQ(m_cache.evict(LSN {2}), std::nullopt);

    const auto frame_3 = m_cache.evict(LSN {3});
    ASSERT_EQ(frame_3->page_id(), PID {3});
    ASSERT_EQ(m_cache.size(), 0);
}

} // <anonymous>


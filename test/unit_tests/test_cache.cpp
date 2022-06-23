
#include <gtest/gtest.h>

#include "page/page.h"
#include "pool/frame.h"
#include "pool/page_cache.h"

namespace {

using namespace calico;

TEST(CacheTests, FreshCacheIsEmpty)
{
    LruCache<int, int> cache;
    ASSERT_TRUE(cache.is_empty());
    ASSERT_EQ(cache.size(), 0);
}

TEST(CacheTests, a)
{
    LruCache<int, int> cache;
    cache.put(1, 1);
    cache.put(2, 2);
    cache.put(3, 3);
    auto a = cache.evict();
    auto b = cache.evict();
    auto c = cache.evict();
    auto d = cache.evict();
    ASSERT_EQ(*a, 1);
    ASSERT_EQ(*b, 2);
    ASSERT_EQ(*c, 3);
    ASSERT_EQ(d, std::nullopt);
}

class PageCacheTests: public testing::Test {
public:
    const LSN LARGE_LSN {1'000'000'000};

    ~PageCacheTests() override = default;

    auto cache_put(Frame frame)
    {
        const auto id = frame.page_id();
        m_cache.put(id, std::move(frame));
    }

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
    cache_put(make_frame(PID::root()));
    ASSERT_TRUE(m_cache.contains(PID::root()));
    ASSERT_EQ(m_cache.size(), 1);
}

TEST_F(PageCacheTests, ExtractFrame)
{
    cache_put(make_frame(PID::root()));
    ASSERT_EQ(m_cache.extract(PID::root())->page_id(), PID::root());
    ASSERT_EQ(m_cache.size(), 0);
}

TEST_F(PageCacheTests, EvictFromEmptyCacheDoesNothing)
{
    ASSERT_EQ(m_cache.evict(), std::nullopt);
}

TEST_F(PageCacheTests, EvictUntilEmpty)
{
    cache_put(make_frame(PID::root()));
    ASSERT_NE(m_cache.evict(), std::nullopt);
    ASSERT_EQ(m_cache.evict(), std::nullopt);
    ASSERT_EQ(m_cache.size(), 0);
}

} // <anonymous>


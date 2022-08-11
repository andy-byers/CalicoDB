
#include <numeric>
#include <gtest/gtest.h>
#include "fakes.h"
#include "page/page.h"
#include "pool/basic_pager.h"
#include "pool/framer.h"
#include "utils/layout.h"
#include "utils/logging.h"

namespace cco {

TEST(UniqueCacheTests, NewCacheIsEmpty)
{
    impl::UniqueCache<int, int> cache;
    ASSERT_TRUE(cache.is_empty());
    ASSERT_EQ(cache.size(), 0);
}

TEST(UniqueCacheTests, CanGetEntry)
{
    impl::UniqueCache<int, int> cache;
    cache.put(4, 2);
    ASSERT_EQ(cache.get(4)->second, 2);
}

TEST(UniqueCacheTests, DuplicateKeyDeathTest)
{
    impl::UniqueCache<int, int> cache;
    cache.put(4, 2);
    ASSERT_DEATH(cache.put(4, 2), "Expect");
}

TEST(UniqueCacheTests, CannotEvictFromEmptyCache)
{
    impl::UniqueCache<int, int> cache;
    ASSERT_EQ(cache.evict(), std::nullopt);
}

TEST(UniqueCacheTests, CannotGetNonexistentValue)
{
    impl::UniqueCache<int, int> cache;
    ASSERT_EQ(cache.get(0), cache.end());
}

TEST(UniqueCacheTests, FifoCacheEvictsLastInElement)
{
    UniqueFifoCache<int, int> cache;
    cache.put(0, 0);
    cache.put(1, 1);
    cache.put(2, 2);
    ASSERT_EQ(cache.evict().value(), 0);
    ASSERT_EQ(cache.evict().value(), 1);
    ASSERT_EQ(cache.evict().value(), 2);
}

TEST(UniqueCacheTests, LruCacheEvictsLeastRecentlyUsedElement)
{
    UniqueLruCache<int, int> cache;
    cache.put(0, 0);
    cache.put(1, 1);
    cache.put(2, 2);
    ASSERT_EQ(cache.get(0)->second, 0);
    ASSERT_EQ(cache.get(1)->second, 1);
    ASSERT_EQ(cache.evict().value(), 2);
    ASSERT_EQ(cache.evict().value(), 0);
    ASSERT_EQ(cache.evict().value(), 1);
}

TEST(UniqueCacheTests, ExistenceCheckDoesNotCountAsUsage)
{
    UniqueLruCache<int, int> cache;
    cache.put(0, 0);
    cache.put(1, 1);
    cache.put(2, 2);
    ASSERT_TRUE(cache.contains(0));
    ASSERT_TRUE(cache.contains(1));
    ASSERT_EQ(cache.evict().value(), 0);
    ASSERT_EQ(cache.evict().value(), 1);
    ASSERT_EQ(cache.evict().value(), 2);
}

class PageCacheTests: public testing::Test {
public:
    ~PageCacheTests() override = default;

    Registry cache;
};

TEST_F(PageCacheTests, HotEntriesAreFoundLast)
{
    cache.put(PageId {11UL}, FrameId {11UL});
    cache.put(PageId {12UL}, FrameId {12UL});
    cache.put(PageId {13UL}, FrameId {13UL});
    cache.put(PageId {1UL}, FrameId {1UL});
    cache.put(PageId {2UL}, FrameId {2UL});
    cache.put(PageId {3UL}, FrameId {3UL});
    ASSERT_EQ(cache.size(), 6);

    // Reference these entries again, causing them to be placed in the hot cache.
    ASSERT_EQ(cache.get(PageId {11UL})->second.frame_id, 11);
    ASSERT_EQ(cache.get(PageId {12UL})->second.frame_id, 12);
    ASSERT_EQ(cache.get(PageId {13UL})->second.frame_id, 13);

    Index i {}, j {};

    const auto callback = [&i, &j](auto page_id, auto frame_id, auto) {
        EXPECT_EQ(page_id, frame_id);
        EXPECT_EQ(page_id, i + (j >= 3)*10 + 1) << "The cache entries should have been visited in order {1, 2, 3, 11, 12, 13}";
        j++;
        i = j % 3;
        return false;
    };

    auto itr = cache.find_entry(callback);
    ASSERT_EQ(itr, cache.end());
}

class PagerTests: public testing::Test {
public:
    explicit PagerTests()
        : home {std::make_unique<HeapStorage>()}
    {
        std::unique_ptr<RandomAccessEditor> file;
        RandomAccessEditor *temp {};
        EXPECT_TRUE(home->open_random_access_editor(DATA_FILENAME, &temp).is_ok());
        file.reset(temp);

        pager = Framer::open(std::move(file), 0x100, 8).value();
    }

    ~PagerTests() override = default;

    Random random {0};
    std::unique_ptr<HeapStorage> home;
    std::unique_ptr<Framer> pager;
};

TEST_F(PagerTests, NewPagerIsSetUpCorrectly)
{
    ASSERT_EQ(pager->available(), 8);
    ASSERT_EQ(pager->page_count(), 0);
    ASSERT_TRUE(pager->flushed_lsn().is_null());
}

TEST_F(PagerTests, KeepsTrackOfAvailableFrames)
{
    auto frame_id = pager->pin(PageId::base()).value();
    ASSERT_EQ(pager->available(), 7);
    pager->discard(frame_id);
    ASSERT_EQ(pager->available(), 8);
}

TEST_F(PagerTests, PinFailsWhenNoFramesAreAvailable)
{
    for (Index i {1}; i <= 8; i++)
        ASSERT_TRUE(pager->pin(PageId {i}));
    const auto r = pager->pin(PageId {9UL});
    ASSERT_FALSE(r.has_value());
    ASSERT_TRUE(r.error().is_not_found()) << "Unexpected Error: " << r.error().what();

    const auto s = pager->unpin(FrameId {1UL});
    ASSERT_TRUE(s.is_ok()) << "Error: " << s.what();
    ASSERT_TRUE(pager->pin(PageId {9UL}));
}

//class BufferPoolTestsBase: public testing::Test {
//public:
//    static constexpr Size frame_count {32};
//    static constexpr Size page_size {0x100};
//
//    explicit BufferPoolTestsBase(std::unique_ptr<MockDirectory> memory_home):
//          home {std::move(memory_home)}
//    {
//        EXPECT_CALL(*home, open_file).Times(1);
//
//        pool = *BufferPool::open({
//            *home,
//            create_sink(),
//            SequenceNumber::null(),
//            frame_count,
//            0,
//            page_size,
//            false,
//        });
//
//        mock = home->get_mock_data_file();
//    }
//
//    ~BufferPoolTestsBase() override = default;
//
//    Random random {0};
//    MockFile *mock;
//    std::unique_ptr<MockDirectory> home;
//    std::unique_ptr<BufferPool> pool;
//};
//
//class BufferPoolTests: public BufferPoolTestsBase {
//public:
//    BufferPoolTests():
//          BufferPoolTestsBase {std::make_unique<MockDirectory>("BufferPoolTests")} {}
//
//    ~BufferPoolTests() override = default;
//};
//
//TEST_F(BufferPoolTests, FreshBufferPoolIsEmpty)
//{
//    ASSERT_EQ(pool->page_count(), 0);
//}
//
//TEST_F(BufferPoolTests, FreshPoolIsSetUpCorrectly)
//{
//    ASSERT_EQ(pool->page_size(), page_size);
//    ASSERT_EQ(pool->hit_ratio(), 0.0);
//}
//
//TEST_F(BufferPoolTests, AllocationInceasesPageCount)
//{
//    ASSERT_TRUE(pool->release(*pool->allocate()));
//    ASSERT_EQ(pool->page_count(), 1);
//    ASSERT_TRUE(pool->release(*pool->allocate()));
//    ASSERT_EQ(pool->page_count(), 2);
//    ASSERT_TRUE(pool->release(*pool->allocate()));
//    ASSERT_EQ(pool->page_count(), 3);
//}
//
//TEST_F(BufferPoolTests, AllocateReturnsCorrectPage)
//{
//    auto page = pool->allocate();
//    ASSERT_EQ(page->id(), PageId::base());
//}
//
//TEST_F(BufferPoolTests, AcquireReturnsCorrectPage)
//{
//    ASSERT_TRUE(pool->release(*pool->allocate()));
//    auto page = pool->acquire(PageId::base(), true);
//    ASSERT_EQ(page->id(), PageId::base());
//}
//
//TEST_F(BufferPoolTests, AcquireMultipleWritablePagesDeathTest)
//{
//    auto page = pool->allocate();
//    ASSERT_DEATH(auto unused = pool->acquire(PageId::base(), true), "Expect");
//}
//
//TEST_F(BufferPoolTests, AcquireReadableAndWritablePagesDeathTest)
//{
//    auto page = pool->allocate();
//    ASSERT_DEATH(auto unused = pool->acquire(PageId::base(), false), "Expect");
//}
//
//auto write_to_page(Page &page, const std::string &message) -> void
//{
//    const auto offset = PageLayout::content_offset(page.id());
//    CCO_EXPECT_LE(offset + message.size(), page.size());
//    page.write(stob(message), offset);
//}
//
//auto read_from_page(const Page &page, Size size) -> std::string
//{
//    const auto offset = PageLayout::content_offset(page.id());
//    CCO_EXPECT_LE(offset + size, page.size());
//    auto message = std::string(size, '\x00');
//    page.read(stob(message), offset);
//    return message;
//}
//
//TEST_F(BufferPoolTests, PageDataPersistsBetweenAcquires)
//{
//    auto in_page = pool->allocate();
//    write_to_page(*in_page, "Hello, world!");
//    ASSERT_TRUE(pool->release(std::move(*in_page)));
//    auto out_page = pool->acquire(PageId::base(), false);
//    ASSERT_EQ(read_from_page(*out_page, 13), "Hello, world!");
//    ASSERT_TRUE(pool->release(std::move(*out_page)));
//}
//
//TEST_F(BufferPoolTests, PageDataPersistsAfterEviction)
//{
//    const auto n = frame_count * 2;
//    for (Index i {}; i < n; ++i) {
//        auto in_page = pool->allocate();
//        write_to_page(*in_page, "Hello, world!");
//        ASSERT_TRUE(pool->release(std::move(*in_page)));
//    }
//    for (Index i {}; i < n; ++i) {
//        auto out_page = pool->acquire(PageId {i + 1}, false);
//        ASSERT_EQ(read_from_page(*out_page, 13), "Hello, world!");
//        ASSERT_TRUE(pool->release(std::move(*out_page)));
//    }
//}
//
//template<class Test> auto run_sanity_check(Test &test, Size num_iterations) -> Size
//{
//    Size num_updates {};
//    for (Index i {}; i < num_iterations; ++i) {
//        if (test.random.next_int(1) == 0) {
//            auto page = test.pool->allocate();
//            write_to_page(*page, std::to_string(page->id().value));
//            num_updates++;
//            EXPECT_TRUE(test.pool->release(std::move(*page)));
//        } else if (test.pool->page_count()) {
//            const auto id = test.random.next_int(Size {1}, test.pool->page_count());
//            const auto result = std::to_string(id);
//            auto page = test.pool->acquire(PageId {id}, false);
//            EXPECT_EQ(read_from_page(*page, result.size()), result);
//            EXPECT_TRUE(test.pool->release(std::move(*page)));
//        }
//    }
//    return num_updates;
//}
//
//TEST_F(BufferPoolTests, SanityCheck)
//{
//    run_sanity_check(*this, 1'000);
//}
//
//TEST_F(BufferPoolTests, KeepsTrackOfHitRatio)
//{
//    run_sanity_check(*this, 10);
//    ASSERT_NE(pool->hit_ratio(), 0.0);
//}
//
//class MemoryPoolTests: public testing::Test {
//public:
//    static constexpr Size page_size = 0x200;
//
//    MemoryPoolTests():
//          pool {std::make_unique<MemoryPool>(page_size, true)} {}
//
//    Random random {0};
//    std::unique_ptr<MemoryPool> pool;
//};
//
//TEST_F(MemoryPoolTests, FreshMemoryPoolIsEmpty)
//{
//    ASSERT_EQ(pool->page_count(), 0);
//}
//
//TEST_F(MemoryPoolTests, FreshMemoryPoolIsSetUpCorrectly)
//{
//    ASSERT_EQ(pool->page_size(), page_size);
//    ASSERT_EQ(pool->hit_ratio(), 1.0);
//}
//
//TEST_F(MemoryPoolTests, StubMethodsWork)
//{
//    ASSERT_TRUE(pool->flush());
//}
//
//TEST_F(MemoryPoolTests, SanityCheck)
//{
//    run_sanity_check(*this, 1'000);
//}
//
//TEST_F(MemoryPoolTests, HitRatioIsAlwaysOne)
//{
//    run_sanity_check(*this, 10);
//    ASSERT_EQ(pool->hit_ratio(), 1.0);
//}

} // cco
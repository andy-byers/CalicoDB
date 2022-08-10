//
//#include "fakes.h"
//#include "page/file_header.h"
//#include "page/page.h"
//#include "pool/buffer_pool.h"
//#include "pool/memory_pool.h"
//#include "pool/pager.h"
//#include "utils/layout.h"
//#include "utils/logging.h"
//#include <gtest/gtest.h>
//#include <numeric>
//
//namespace cco {
//
//
//TEST(PagerOpenTests, A)
//{
//    auto home = std::make_unique<FakeDirectory>("PagerOpenTests");
//    auto file = home->open_fake_file(DATA_NAME, Mode::CREATE | Mode::READ_WRITE, 0666);
//    auto memory = file->shared_memory();
//    auto pager = Pager::open({
//        std::move(file),
//        SequenceNumber::base(),
//        0x100,
//        16,
//    });
//
//}
//
//class PagerTests: public testing::Test {
//public:
//    explicit PagerTests()
//        : home {std::make_unique<FakeDirectory>("PagerTests")}
//    {
//        file = home->open_fake_file(DATA_NAME, Mode::CREATE | Mode::READ_WRITE, 0666);
//        memory = file->shared_memory();
//    }
//
//    auto setup(Size page_size, Size frame_count) -> Result<void>
//    {
//        return Pager::open({
//            std::move(file),
//            SequenceNumber::base(),
//            page_size,
//            frame_count,
//        }).and_then([this](std::unique_ptr<Pager> p) -> Result<void> {
//            pager = std::move(p);
//            return {};
//        });
//    }
//
//    ~PagerTests() override = default;
//
//    Random random {0};
//    SharedMemory memory;
//    std::unique_ptr<FakeDirectory> home;
//    std::unique_ptr<FakeFile> file;
//    std::unique_ptr<Pager> pager;
//};
//
//TEST_F(PagerTests, NewPagerIsSetUpCorrectly)
//{
//    setup(0x100, 16);
//    ASSERT_EQ(pager->available(), 16);
//    ASSERT_EQ(pager->page_size(), 0x100);
//}
//
//TEST_F(PagerTests, KeepsTrackOfAvailableCount)
//{
//    setup(0x100, 16);
//    auto frame = pager->pin(PageId::base());
//    ASSERT_EQ(pager->available(), 15);
//    pager->discard(std::move(*frame));
//    ASSERT_EQ(pager->available(), 16);
//}
//
//TEST_F(PagerTests, PagerCreatesExtraPagesOnDemand)
//{
//    setup(0x100, 16);
//    for (Index i {ROOT_ID_VALUE}; i < 32; i++)
//        ASSERT_TRUE(pager->pin(PageId {i}));
//}
//
//TEST_F(PagerTests, TruncateResizesUnderlyingFile)
//{
//    setup(0x100, 16);
//    ASSERT_TRUE(pager->unpin(*pager->pin(PageId::base())));
//    ASSERT_NE(memory.memory().size(), 0x100);
//    ASSERT_TRUE(pager->truncate(0));
//    ASSERT_EQ(memory.memory().size(), 0);
//}
//
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
//
//} // cco
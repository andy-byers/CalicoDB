
#include "fakes.h"
#include "page/page.h"
#include "pager/basic_pager.h"
#include "pager/framer.h"
#include "pager/registry.h"
#include "unit_tests.h"
#include "utils/layout.h"
#include "utils/logging.h"
#include "wal/disabled_wal.h"
#include <gtest/gtest.h>
#include <numeric>

namespace calico {

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
    ASSERT_DEATH(cache.put(4, 2), EXPECTATION_MATCHER);
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

class PageRegistryTests : public testing::Test {
public:
    ~PageRegistryTests() override = default;

    PageRegistry registry;
};

TEST_F(PageRegistryTests, HotEntriesAreFoundLast)
{
    registry.put(PageId {11UL}, FrameNumber {11UL});
    registry.put(PageId {12UL}, FrameNumber {12UL});
    registry.put(PageId {13UL}, FrameNumber {13UL});
    registry.put(PageId {1UL}, FrameNumber {1UL});
    registry.put(PageId {2UL}, FrameNumber {2UL});
    registry.put(PageId {3UL}, FrameNumber {3UL});
    ASSERT_EQ(registry.size(), 6);

    // Reference these entries again, causing them to be placed in the hot cache.
    ASSERT_EQ(registry.get(PageId {11UL})->second.frame_id, 11UL);
    ASSERT_EQ(registry.get(PageId {12UL})->second.frame_id, 12UL);
    ASSERT_EQ(registry.get(PageId {13UL})->second.frame_id, 13UL);

    Size i {}, j {};

    const auto callback = [&i, &j](auto page_id, auto frame_id, auto) {
        EXPECT_EQ(page_id, frame_id);
        EXPECT_EQ(page_id, i + (j >= 3)*10 + 1) << "The cache entries should have been visited in order {1, 2, 3, 11, 12, 13}";
        j++;
        i = j % 3;
        return false;
    };

    auto itr = registry.find_entry(callback);
    ASSERT_EQ(itr, registry.end());
}

class FramerTests : public testing::Test {
public:
    explicit FramerTests()
        : home {std::make_unique<HeapStorage>()}
    {
        std::unique_ptr<RandomEditor> file;
        RandomEditor *temp {};
        EXPECT_TRUE(home->open_random_editor(DATA_FILENAME, &temp).is_ok());
        file.reset(temp);

        framer = Framer::open(std::move(file), wal, 0x100, 8).value();
    }

    ~FramerTests() override = default;

    Random random {0};
    DisabledWriteAheadLog wal;
    std::unique_ptr<HeapStorage> home;
    std::unique_ptr<Framer> framer;
};

TEST_F(FramerTests, NewFramerIsSetUpCorrectly)
{
    ASSERT_EQ(framer->available(), 8);
    ASSERT_EQ(framer->page_count(), 0);
    ASSERT_TRUE(framer->flushed_lsn().is_null());
}

TEST_F(FramerTests, KeepsTrackOfAvailableFrames)
{
    auto frame_id = framer->pin(PageId::root()).value();
    ASSERT_EQ(framer->available(), 7);
    framer->discard(frame_id);
    ASSERT_EQ(framer->available(), 8);
}

TEST_F(FramerTests, PinFailsWhenNoFramesAreAvailable)
{
    for (Size i {1}; i <= 8; i++)
        ASSERT_TRUE(framer->pin(PageId {i}));
    const auto r = framer->pin(PageId {9UL});
    ASSERT_FALSE(r.has_value());
    ASSERT_TRUE(r.error().is_not_found()) << "Unexpected Error: " << r.error().what();

    const auto s = framer->unpin(FrameNumber {1UL}, false);
    ASSERT_TRUE(s.is_ok()) << "Error: " << s.what();
    ASSERT_TRUE(framer->pin(PageId {9UL}));
}

auto write_to_page(Page &page, const std::string &message) -> void
{
    const auto offset = PageLayout::content_offset(page.id());
    CALICO_EXPECT_LE(offset + message.size(), page.size());
    page.write(stob(message), offset);
}

[[nodiscard]]
auto read_from_page(const Page &page, Size size) -> std::string
{
    const auto offset = PageLayout::content_offset(page.id());
    CALICO_EXPECT_LE(offset + size, page.size());
    auto message = std::string(size, '\x00');
    page.read(stob(message), offset);
    return message;
}

class PagerTests : public testing::Test {
public:
    static constexpr Size frame_count {32};
    static constexpr Size page_size {0x100};
    std::string test_message {"Hello, world!"};

    explicit PagerTests()
          : wal {std::make_unique<DisabledWriteAheadLog>()},
            store {std::make_unique<MockStorage>()}
    {
        store->delegate_to_real();
        EXPECT_CALL(*store, open_random_editor).Times(1);
        EXPECT_CALL(*store, create_directory).Times(1);
        EXPECT_TRUE(store->create_directory("test").is_ok());
        pager = *BasicPager::open({
            "test",
            *store,
            *wal,
            create_sink(),
            frame_count,
            page_size,
        });
        mock = store->get_mock_random_editor("test/data");
    }

    ~PagerTests() override = default;
    
    [[nodiscard]]
    auto allocate_write(const std::string &message) const
    {
        auto r = pager->allocate();
        EXPECT_TRUE(r.has_value()) << "Error: " << r.error().what();
        write_to_page(*r, message);
        return std::move(*r);
    }
    
    [[nodiscard]]
    auto allocate_write_release(const std::string &message) const
    {
        auto page = allocate_write(message);
        const auto id = page.id();
        const auto s = pager->release(std::move(page));
        EXPECT_TRUE(s.is_ok()) << "Error: " << s.what();
        return id;
    }

    [[nodiscard]]
    auto acquire_write(PageId id, const std::string &message) const
    {
        auto r = pager->acquire(id, false);
        EXPECT_TRUE(r.has_value()) << "Error: " << r.error().what();
        write_to_page(*r, message);
        return std::move(*r);
    }

    auto acquire_write_release(PageId id, const std::string &message) const
    {
        auto page = acquire_write(id, message);
        const auto s = pager->release(std::move(page));
        EXPECT_TRUE(s.is_ok()) << "Error: " << s.what();
    }

    [[nodiscard]]
    auto acquire_read_release(PageId id, Size size) const
    {
        auto r = pager->acquire(id, false);
        EXPECT_TRUE(r.has_value()) << "Error: " << r.error().what();
        auto message = read_from_page(*r, size);
        EXPECT_TRUE(pager->release(std::move(*r)).is_ok());
        return message;
    }

    Random random {0};
    MockRandomEditor *mock;
    std::unique_ptr<WriteAheadLog> wal;
    std::unique_ptr<MockStorage> store;
    std::unique_ptr<Pager> pager;
};

TEST_F(PagerTests, NewPagerIsSetUpCorrectly)
{
    ASSERT_EQ(pager->page_count(), 0);
    ASSERT_EQ(pager->flushed_lsn(), SequenceId::null());
    ASSERT_TRUE(pager->status().is_ok());
}

TEST_F(PagerTests, AllocationInceasesPageCount)
{
    [[maybe_unused]] const auto a = allocate_write_release("a");
    ASSERT_EQ(pager->page_count(), 1);
    [[maybe_unused]] const auto b = allocate_write_release("b");
    ASSERT_EQ(pager->page_count(), 2);
    [[maybe_unused]] const auto c = allocate_write_release("c");
    ASSERT_EQ(pager->page_count(), 3);
}

TEST_F(PagerTests, FirstAllocationCreatesRootPage)
{
    auto id = allocate_write_release(test_message);
    ASSERT_EQ(id, PageId::root());
}

TEST_F(PagerTests, AcquireReturnsCorrectPage)
{
    const auto id = allocate_write_release(test_message);
    auto r = pager->acquire(id, false);
    ASSERT_EQ(id, r->id());
    ASSERT_EQ(id, PageId::root());
    EXPECT_TRUE(pager->release(std::move(*r)).is_ok());
}

TEST_F(PagerTests, MultipleWritersDeathTest)
{
    const auto page = allocate_write(test_message);
    ASSERT_DEATH(const auto same_page = pager->acquire(page.id(), true), EXPECTATION_MATCHER);
}

TEST_F(PagerTests, ReaderAndWriterDeathTest)
{
    const auto page = allocate_write(test_message);
    ASSERT_DEATH(const auto same_page = pager->acquire(page.id(), false), EXPECTATION_MATCHER);
}

TEST_F(PagerTests, MultipleReaders)
{
    const auto id = allocate_write_release(test_message);
    auto page_1a = pager->acquire(id, false).value();
    auto page_1b = pager->acquire(id, false).value();
    ASSERT_TRUE(pager->release(std::move(page_1a)).is_ok());
    ASSERT_TRUE(pager->release(std::move(page_1b)).is_ok());
}

TEST_F(PagerTests, PagesAreAutomaticallyReleased)
{
    // This line allocates a page, writes to it, then lets it go out of scope. The page should release itself in its destructor using the pointer it
    // stores back to the pager object. If it doesn't, we would not be able to acquire the same page as writable again (see MultipleWritersDeathTest).
    const auto id = allocate_write(test_message).id();
    ASSERT_EQ(acquire_read_release(id, test_message.size()), test_message);
}

TEST_F(PagerTests, PageDataPersistsInFrame)
{
    const auto id = allocate_write_release(test_message);
    ASSERT_EQ(acquire_read_release(id, test_message.size()), test_message);
}

TEST_F(PagerTests, PageDataPersistsInFile)
{
    using testing::_;
    using testing::AtLeast;
    EXPECT_CALL(*mock, write).Times(AtLeast(frame_count));
    EXPECT_CALL(*mock, read(_, 0)).Times(1); // Root page is read once.
    EXPECT_CALL(*mock, write(_, 0)).Times(1); // Root page is written once.
    const auto id = allocate_write_release(test_message);

    // Cause the root page to be evicted and written back, along with some other pages.
    while (pager->page_count() < frame_count * 2)
        [[maybe_unused]] auto unused = allocate_write_release("...");

    // Read the root page back from the file.
    ASSERT_EQ(acquire_read_release(id, test_message.size()), test_message);
}

[[nodiscard]]
auto generate_id_strings(Size n)
{
    std::vector<Size> id_ints(n);
    std::iota(begin(id_ints), end(id_ints), 1);

    std::vector<std::string> id_strs;
    std::transform(cbegin(id_ints), cend(id_ints), back_inserter(id_strs), [](auto id) {
        return fmt::format("{:06}", id);
    });
    return id_strs;
}

TEST_F(PagerTests, SanityCheck)
{
    const auto ids = generate_id_strings(500);

    using testing::AtLeast;
    EXPECT_CALL(*mock, read).Times(AtLeast(frame_count));
    EXPECT_CALL(*mock, write).Times(AtLeast(frame_count));

    for (const auto &id: ids)
        [[maybe_unused]] const auto unused = allocate_write_release(id);

    for (const auto &id: ids) { // NOTE: gtest assertion macros sometimes complain if braces are omitted.
        ASSERT_EQ(id, acquire_read_release(PageId {std::stoull(id)}, id.size()));
    }
}

} // cco
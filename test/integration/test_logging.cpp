#include <gtest/gtest.h>
#include <vector>
#include "fakes.h"
#include "tools.h"
#include "pool/buffer_pool.h"
#include "storage/directory.h"
#include "storage/system.h"
#include "wal/wal_reader.h"
#include "wal/wal_record.h"
#include "wal/wal_writer.h"
#include "utils/crc.h"
#include "utils/encoding.h"
#include "utils/layout.h"
#include "utils/logging.h"

namespace {
using namespace calico;

class LoggingTests: public testing::Test {
public:
    static constexpr Size CACHE_SIZE = 16;
    static constexpr Size BLOCK_SIZE = 0x200;

    LoggingTests()
    {
        Options options;
        options.page_size = BLOCK_SIZE;
        options.block_size = BLOCK_SIZE;
        FakeFilesHarness harness {options};
        auto sink = logging::create_sink("", 0);
        bank = std::move(harness.bank);
        pool = std::make_unique<BufferPool>(BufferPool::Parameters{
            *bank,
            std::make_unique<WALReader>(WALReader::Parameters {*bank, sink, options.block_size}),
            std::make_unique<WALWriter>(WALWriter::Parameters {*bank, sink, options.block_size}),
            sink,
            LSN::base(),
            CACHE_SIZE,
            0,
            options.page_size,
            0666,
            true,
        });
        pool_backing = harness.tree_backing;
        wal_backing = harness.wal_backing;
        tree_faults = harness.tree_faults;
        wal_reader_faults = harness.wal_reader_faults;
        wal_writer_faults = harness.wal_writer_faults;
    }

    auto recreate_components() -> void
    {
        wal_reader_faults = bank->open_memory(WAL_NAME, Mode::READ_ONLY, 0666)->faults();
        wal_writer_faults = bank->open_memory(WAL_NAME, Mode::READ_ONLY, 0666)->faults();
        pool.reset();
        auto sink = logging::create_sink("", 0);
        pool = std::make_unique<BufferPool>(BufferPool::Parameters{
            *bank,
            std::make_unique<WALReader>(WALReader::Parameters {*bank, sink, BLOCK_SIZE}),
            std::make_unique<WALWriter>(WALWriter::Parameters {*bank, sink, BLOCK_SIZE}),
            logging::create_sink("", 0),
            LSN::base(),
            CACHE_SIZE,
            0,
            BLOCK_SIZE,
            true,
        });
    }

    auto make_random_changes(Page &page, Size n)
    {
        const auto base = PageLayout::content_offset(page.id());
        for (Index i {}; i < n; ++i) {
            const auto offset = random.next_int(base, page.size() - 4);
            switch (random.next_int(2)) {
                case 0: page.put_u16(offset, random.next_int(std::numeric_limits<uint16_t>::max())); break;
                case 1: page.put_u32(offset, random.next_int(std::numeric_limits<uint32_t>::max())); break;
                case 2: page.write(stob(random_string(random, random.next_int(Size{1}, page.size() - offset))), offset); break;
            }
        }
    }

    static auto page_crc(const Page &page)
    {
        const auto content_offset = PageLayout::content_offset(page.id());
        const auto lsn_offset = PageLayout::header_offset(page.id()) + PageLayout::LSN_OFFSET;
        auto temp = btos(page.range(0));
        put_uint32(stob(temp).range(lsn_offset), 0);
        return crc_32(stob(temp).range(content_offset));
    }

    Random random {0};
    SharedMemory pool_backing;
    SharedMemory wal_backing;
    FaultControls tree_faults;
    FaultControls wal_reader_faults;
    FaultControls wal_writer_faults;
    std::unique_ptr<BufferPool> pool;
    std::unique_ptr<MemoryBank> bank;
};

TEST_F(LoggingTests, FreshBufferPoolIsEmpty)
{
    ASSERT_TRUE(pool_backing.memory().empty());
    ASSERT_TRUE(wal_backing.memory().empty());
}

TEST_F(LoggingTests, FlushingEmptyBufferPoolDoesNothing)
{
    ASSERT_FALSE(pool->try_flush());
    ASSERT_FALSE(pool->try_flush_wal());
    ASSERT_TRUE(pool_backing.memory().empty());
    ASSERT_TRUE(wal_backing.memory().empty());
}

TEST_F(LoggingTests, WALRecordsAreWrittenToDisk)
{
    // Alters the page, so a WAL record must be created and written.
    (void)pool->allocate(PageType::EXTERNAL_NODE);
    ASSERT_TRUE(pool->try_flush_wal());
    ASSERT_FALSE(wal_backing.memory().empty());
}

TEST_F(LoggingTests, WALRecordsMustBeFlushedBeforeDataPages)
{
    (void)pool->allocate(PageType::EXTERNAL_NODE);

    // The WAL record is not yet on disk (it is in the WALWriter tail buffer), so we shouldn't be able to
    // flush the corresponding data page.
    ASSERT_FALSE(pool->try_flush());

    // After the WAL is flushed, we can flush the data page.
    ASSERT_TRUE(pool->try_flush_wal());
    ASSERT_TRUE(pool->try_flush());
}

TEST_F(LoggingTests, AbortDiscardsChangesSincePreviousCommit)
{
    uint32_t crc {};
    {
        auto page = pool->allocate(PageType::EXTERNAL_NODE);
        crc = page_crc(page);
    }
    pool->commit();

    // We must let the page go out of scope before calling either commit() or abort(), as they rely on
    // all frames being unpinned.
    {
        auto page = pool->acquire(PID::root(), true);
        page.set_type(PageType::INTERNAL_NODE);
        make_random_changes(page, 10);
        ASSERT_NE(crc, page_crc(page));
    }
    pool->abort();

    auto page = pool->acquire(PID::root(), false);
    ASSERT_EQ(crc, page_crc(page));
}

auto setup_tests(IBufferPool &pool, Size num_records)
{
    for (Index i {}; i < num_records; ++i)
        (void)pool.allocate(PageType::INTERNAL_NODE);

    pool.commit();

    for (Index i {}; i < num_records; ++i) {
        auto page = pool.acquire(PID {i + 1}, true);
        page.set_type(PageType::EXTERNAL_NODE);
    }
    pool.try_flush_wal();
}

TEST_F(LoggingTests, IncompleteWAL)
{
    static constexpr auto NUM_RECORDS {1'000};
    setup_tests(*pool, NUM_RECORDS);

    // Get rid of all but the root page of the database. This forces us to use the WAL to recover,
    // but since we won't find a commit record, we will roll back.
    pool_backing.memory().resize(pool->page_size());

    // This should cause us to roll back to when all the pages were of external node type.
    pool->recover();

    for (Index i {}; i < NUM_RECORDS; ++i) {
        auto page = pool->acquire(PID {i + 1}, true);
        ASSERT_EQ(page.type(), PageType::INTERNAL_NODE);
    }
}

TEST_F(LoggingTests, IncompleteAndCorruptedWAL)
{
    static constexpr auto NUM_RECORDS {1'000};
    setup_tests(*pool, NUM_RECORDS);

    // Add some random bytes to the end of the WAL and get rid of all but the root page of the database.
    // This forces us to use the WAL to recover
    wal_backing.memory() += random.next_string(pool->block_size());
    pool_backing.memory().resize(pool->page_size());

    // This should cause us to roll back to when all the pages were of external node type.
    pool->recover();

    for (Index i {}; i < NUM_RECORDS; ++i) {
        auto page = pool->acquire(PID {i + 1}, true);
        ASSERT_EQ(page.type(), PageType::INTERNAL_NODE);
    }
}

} // <anonymous>
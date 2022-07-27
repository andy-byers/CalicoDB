#include <gtest/gtest.h>

#include "calico/options.h"
#include "calico/bytes.h"
#include "storage/directory.h"
#include "storage/file.h"
#include "utils/logging.h"
#include "utils/utils.h"
#include "wal/wal_manager.h"
#include "wal/wal_reader.h"
#include "wal/wal_record.h"
#include "wal/wal_writer.h"

#include "fakes.h"
#include "pool/buffer_pool.h"
#include "random.h"
#include "tools.h"
#include "utils/layout.h"

namespace {

using namespace cco;
using namespace cco;
using namespace cco;

struct TestWALOptions {
    std::string path;
    Size page_size {};
};

class WALReaderWriterTests: public testing::Test {
public:
    static constexpr Size PAGE_SIZE = 0x100;

    WALReaderWriterTests()
    {
        home = std::make_unique<FakeDirectory>("WALReaderWriterTests");
        reader = WALReader::open({nullptr, *home, create_sink(), PAGE_SIZE, LSN::null()}).value();
        writer = WALWriter::open({nullptr, *home, create_sink(), PAGE_SIZE, LSN::null()}).value();
        backing = home->get_shared("wal");
        faults = home->get_faults("wal");
    }

    ~WALReaderWriterTests() override = default;

    SharedMemory backing;
    FaultControls faults;
    std::unique_ptr<FakeDirectory> home;
    std::unique_ptr<IWALReader> reader;
    std::unique_ptr<IWALWriter> writer;
};

auto assert_records_are_siblings(const WALRecord &left, const WALRecord &right, Size split_offset, Size total_payload_size)
{
    ASSERT_EQ(left.lsn(), right.lsn());
    ASSERT_EQ(left.crc(), right.crc());
    ASSERT_NE(left.type(), WALRecord::Type::EMPTY);
    ASSERT_NE(right.type(), WALRecord::Type::EMPTY);
    ASSERT_TRUE(left.type() == WALRecord::Type::FIRST || left.type() == WALRecord::Type::MIDDLE);
    ASSERT_EQ(right.type(), WALRecord::Type::LAST);
    ASSERT_EQ(left.payload().data().size(), split_offset);
    ASSERT_EQ(right.payload().data().size(), total_payload_size - split_offset);
}

TEST_F(WALReaderWriterTests, PayloadEncoding)
{
    WALRecordGenerator generator {PAGE_SIZE};
    const auto record = generator.generate(0x10, 10);
    const auto update = record.payload().decode();
    ASSERT_EQ(update.changes.size(), 10);
}

TEST_F(WALReaderWriterTests, SingleSplit)
{
    WALRecordGenerator generator {PAGE_SIZE};
    auto left = generator.generate(0x10, 10);
    const auto payload_size = left.payload().data().size();
    const auto split_offset = payload_size / 2;

    auto right = left.split(split_offset);
    assert_records_are_siblings(left, right, split_offset, payload_size);
}

TEST_F(WALReaderWriterTests, MultipleSplits)
{
    WALRecordGenerator generator {PAGE_SIZE};
    auto left = generator.generate(0x10, 10);
    const auto payload_size = left.payload().data().size();
    const auto split_offset = payload_size / 3;

    auto middle = left.split(split_offset);
    assert_records_are_siblings(left, middle, split_offset, payload_size);

    auto right = middle.split(split_offset);
    assert_records_are_siblings(middle, right, split_offset, payload_size - split_offset);
}

TEST_F(WALReaderWriterTests, SingleMerge)
{
    WALRecordGenerator generator {PAGE_SIZE};
    auto left = generator.generate(0x10, 10);
    const auto lsn = left.lsn();
    const auto crc = left.crc();
    const auto payload = btos(left.payload().data());
    auto right = left.split(left.payload().data().size() / 2);

    ASSERT_TRUE(left.merge(right));
    ASSERT_EQ(left.lsn(), lsn);
    ASSERT_EQ(left.crc(), crc);
    ASSERT_EQ(left.type(), WALRecord::Type::FULL);
    ASSERT_EQ(btos(left.payload().data()), payload);
}

TEST_F(WALReaderWriterTests, MultipleMerges)
{
    WALRecordGenerator generator {PAGE_SIZE};
    auto left = generator.generate(0x10, 10);
    const auto lsn = left.lsn();
    const auto crc = left.crc();
    const auto payload = btos(left.payload().data());
    auto middle = left.split(payload.size() / 3);
    auto right = middle.split(payload.size() / 3);

    ASSERT_TRUE(left.merge(middle));
    ASSERT_TRUE(left.merge(right));
    ASSERT_EQ(left.lsn(), lsn);
    ASSERT_EQ(left.crc(), crc);
    ASSERT_EQ(left.type(), WALRecord::Type::FULL);
    ASSERT_EQ(btos(left.payload().data()), payload);
}

TEST_F(WALReaderWriterTests, EmptyFileBehavior)
{
    WALReader::Position start;
    ASSERT_TRUE(reader->read(start).error().is_not_found());
}

TEST_F(WALReaderWriterTests, WritesRecordCorrectly)
{
    WALRecordGenerator generator {PAGE_SIZE};
    const auto position = writer->append(generator.generate_small());
    ASSERT_TRUE(position.has_value());
    ASSERT_TRUE(position->block_id == 0 and position->offset == 0);
    ASSERT_TRUE(writer->flush());

    const auto &memory = backing.memory();
    WALRecord record;
    ASSERT_TRUE(record.read(stob(memory)));
    generator.validate_record(record, LSN::base());
}

TEST_F(WALReaderWriterTests, FlushedLSNReflectsLastFullRecord)
{
    WALRecordGenerator generator {PAGE_SIZE};
    ASSERT_TRUE(writer->append(generator.generate_small()));

    // Writing this record should cause a flush after the FIRST part is written. The last record we wrote should
    // then be on disk, and the LAST part of the current record should be in the tail buffer.
    ASSERT_TRUE(writer->append(generator.generate(PAGE_SIZE / 2 * 3, 1)));
    auto lsn = LSN::base();
    ASSERT_EQ(writer->flushed_lsn(), lsn++);
    ASSERT_TRUE(writer->flush());
    ASSERT_EQ(writer->flushed_lsn(), lsn);
}
//
//auto setup_read_fault_test(WALReaderWriterTests &test, WALRecordGenerator &generator, Size n)
//{
//    Random random {0};
//
//    for (Index i {}; i < n; ++i)
//        ASSERT_TRUE(test.writer->append(generator.generate(random.next_int(1UL, 500UL), 10)));
//
//    ASSERT_TRUE(test.writer->flush());
//}
//
//template<class Callable>
//auto call_until_error(Callable &&callable) -> Result<bool>
//{
//    static constexpr auto limit = 100'000;
//    for (Index i {}; i < limit; ++i) {
//        auto result = callable();
//        if (!result.has_value() || !result.value())
//            return result;
//    }
//    ADD_FAILURE() << "call limit (" << limit << ") exceeded";
//    return {};
//}

auto test_writes_then_reads(WALReaderWriterTests &test, const std::vector<Size> &sizes) -> void
{
    WALRecordGenerator generator {WALReaderWriterTests::PAGE_SIZE};
    std::vector<IWALManager::Position> positions;
    positions.reserve(sizes.size());

    for (auto size: sizes) {
        auto position = test.writer->append(generator.generate(size, std::min(16UL, size)));
        ASSERT_TRUE(position.has_value());
        positions.emplace_back(*position);
    }
    ASSERT_TRUE(test.writer->flush());

    auto lsn = LSN::base();
    for (auto position: positions) {
        auto record = test.reader->read(position);
        ASSERT_TRUE(record.has_value());
        generator.validate_record(*record, LSN {lsn.value++});
    }
}

TEST_F(WALReaderWriterTests, SingleSmallRecord)
{
    test_writes_then_reads(*this, {1});
}

TEST_F(WALReaderWriterTests, MultipleSmallRecords)
{
    test_writes_then_reads(*this, {1, 2, 1, 2, 1});
}

TEST_F(WALReaderWriterTests, LargeRecord)
{
    test_writes_then_reads(*this, {0x400});
}

TEST_F(WALReaderWriterTests, MultipleLargeRecords)
{
    test_writes_then_reads(*this, {0x400, 0x800, 0x1000, 0x1400, 0x1800});
}

TEST_F(WALReaderWriterTests, ExplorerStopsAtLastRecord)
{
    WALRecordGenerator generator {PAGE_SIZE};
    WALExplorer explorer {*reader};
    ASSERT_TRUE(writer->append(generator.generate_small()));
    ASSERT_TRUE(writer->append(generator.generate_small()));
    ASSERT_TRUE(writer->append(generator.generate_large()));
    ASSERT_TRUE(writer->flush());

    auto next = explorer.read_next().value();
    generator.validate_record(next.record, LSN {1});
    next = explorer.read_next().value();
    generator.validate_record(next.record, LSN {2});
    next = explorer.read_next().value();
    generator.validate_record(next.record, LSN {3});
    ASSERT_TRUE(explorer.read_next().error().is_not_found());
}

TEST_F(WALReaderWriterTests, ExploresIncompleteBlocks)
{
    WALRecordGenerator generator {PAGE_SIZE};

    ASSERT_TRUE(writer->append(generator.generate_small()));
    ASSERT_TRUE(writer->flush());

    ASSERT_TRUE(writer->append(generator.generate_small()));
    ASSERT_TRUE(writer->append(generator.generate_small()));
    ASSERT_TRUE(writer->flush());

    ASSERT_TRUE(writer->append(generator.generate_small()));
    ASSERT_TRUE(writer->append(generator.generate_small()));
    ASSERT_TRUE(writer->append(generator.generate_small()));
    ASSERT_TRUE(writer->flush());

    WALExplorer explorer {*reader};
    auto next = explorer.read_next();
    ASSERT_TRUE(next);
    generator.validate_record(next->record, LSN {1});
    next = explorer.read_next();
    ASSERT_TRUE(next);
    generator.validate_record(next->record, LSN {2});
    next = explorer.read_next();
    ASSERT_TRUE(next);
    generator.validate_record(next->record, LSN {3});
    next = explorer.read_next();
    ASSERT_TRUE(next);
    generator.validate_record(next->record, LSN {4});
    next = explorer.read_next();
    ASSERT_TRUE(next);
    generator.validate_record(next->record, LSN {5});
    next = explorer.read_next();
    ASSERT_TRUE(next);
    generator.validate_record(next->record, LSN {6});
    ASSERT_FALSE(explorer.read_next());
}

template<class Test> 
auto test_write_records_and_explore(Test &test, Size num_records, double large_fraction, double flush_fraction) -> void
{
    WALRecordGenerator generator {WALReaderWriterTests::PAGE_SIZE};

    auto make_choice = [&generator](double fraction) {
        return generator.random.next_real(1.0) < fraction;
    };

    for (Index i {}; i < num_records; ++i) {
        auto record = make_choice(large_fraction)
            ? generator.generate_large()
            : generator.generate_small();
        ASSERT_TRUE(test.writer->append(record));
        // Always flush on the last round.
        if (make_choice(flush_fraction) || i == num_records - 1) {
            ASSERT_TRUE(test.writer->flush());
        }
    }
    WALExplorer explorer {*test.reader};

    for (Index i {}; i < num_records; ++i) {
        auto next = explorer.read_next();
        ASSERT_TRUE(next) << "record " << i << " does not exist";
        ASSERT_TRUE(next->record.is_consistent()) << "record " << i << " is corrupted";
        generator.validate_record(next->record, LSN {i + ROOT_ID_VALUE});
    }
    ASSERT_FALSE(explorer.read_next());
}

TEST_F(WALReaderWriterTests, WriteAndTraverseSmallRecordsInCompleteBlocks)
{
    test_write_records_and_explore(*this, 250, 0.0, 0.0);
}

TEST_F(WALReaderWriterTests, WriteAndTraverseLargeRecordsInCompleteBlocks)
{
    test_write_records_and_explore(*this, 250, 1.0, 0.0);
}

TEST_F(WALReaderWriterTests, WriteAndTraverseMixedRecordsInCompleteBlocks)
{
    test_write_records_and_explore(*this, 250, 0.5, 0.0);
}

TEST_F(WALReaderWriterTests, WriteAndTraverseSmallRecordsInIncompleteBlocks)
{
    test_write_records_and_explore(*this, 250, 0.0, 0.5);
}

TEST_F(WALReaderWriterTests, WriteAndTraverseLargeRecordsInIncompleteBlocks)
{
    test_write_records_and_explore(*this, 250, 1.0, 0.5);
}

TEST_F(WALReaderWriterTests, WriteAndTraverseMixedRecordsInIncompleteBlocks)
{
    test_write_records_and_explore(*this, 250, 0.5, 0.5);
}

class RealWALReaderWriterTests: public testing::Test {
public:
    static constexpr Size PAGE_SIZE = 0x200;
    static constexpr auto BASE_PATH = "/tmp/calico_test_wal";

    RealWALReaderWriterTests()
    {
        std::error_code ignore;
        std::filesystem::remove_all(BASE_PATH, ignore);

        directory = Directory::open(BASE_PATH).value();
        writer = WALWriter::open({nullptr, *directory, create_sink(), PAGE_SIZE, LSN::base()}).value();
        reader = WALReader::open({nullptr, *directory, create_sink(), PAGE_SIZE, LSN::base()}).value();
    }

    ~RealWALReaderWriterTests() override = default;

    std::unique_ptr<IDirectory> directory;
    std::unique_ptr<IWALReader> reader;
    std::unique_ptr<IWALWriter> writer;
};

TEST_F(RealWALReaderWriterTests, WriteAndTraverseSmallRecordsInCompleteBlocks)
{
    test_write_records_and_explore(*this, 250, 0.0, 0.0);
}

TEST_F(RealWALReaderWriterTests, WriteAndTraverseLargeRecordsInCompleteBlocks)
{
    test_write_records_and_explore(*this, 250, 1.0, 0.0);
}

TEST_F(RealWALReaderWriterTests, WriteAndTraverseMixedRecordsInCompleteBlocks)
{
    test_write_records_and_explore(*this, 250, 0.5, 0.0);
}

TEST_F(RealWALReaderWriterTests, WriteAndTraverseSmallRecordsInIncompleteBlocks)
{
    test_write_records_and_explore(*this, 250, 0.0, 0.5);
}

TEST_F(RealWALReaderWriterTests, WriteAndTraverseLargeRecordsInIncompleteBlocks)
{
    test_write_records_and_explore(*this, 250, 1.0, 0.5);
}

TEST_F(RealWALReaderWriterTests, WriteAndTraverseMixedRecordsInIncompleteBlocks)
{
    test_write_records_and_explore(*this, 250, 0.5, 0.5);
}

class WALTests: public testing::Test {
public:
    static constexpr Size PAGE_SIZE = 0x200;

    ~WALTests() override = default;

    WALTests()
    {
        auto temp = std::make_unique<FakeDirectory>("WALReaderWriterTests");
        pool = BufferPool::open({*temp, create_sink(), LSN::null(), 16, 0, PAGE_SIZE, 0666, true}).value();
        wal_backing = temp->get_shared("wal");
        wal_faults = temp->get_faults("wal");
        data_backing = temp->get_shared("data");
        data_faults = temp->get_faults("data");
        home = std::move(temp);
    }

    auto allocate_page() -> Page
    {
        auto page = pool->allocate().value();
        const auto id = page.id().as_index();
        if (id >= pages_before.size()) {
            CCO_EXPECT_EQ(id, pages_before.size());
            pages_before.emplace_back(btos(page.view(0)));
            pages_after.emplace_back(PAGE_SIZE, '\x00');
        }
        return page;
    }

    auto alter_page(Page &page) -> void
    {
        const auto start = PageLayout::content_offset(page.id());
        const auto id = page.id().as_index();
        for (Size x {start}, dx {}; ; x += dx) {
            dx = random.next_int(20UL);
            if (x + dx > page.size())
                break;
            mem_copy(page.bytes(x, dx), stob(random.next_string(dx)));
            x += random.next_int(10UL, 30UL);
        }
        mem_copy(stob(pages_after.at(id)), page.view(0));
    }

    auto assert_page_is_same_as_before(const Page &page) -> void
    {
        // Skip the header which contains an LSN value that will be automatically incremented when a dirtied page
        // is released.
        const auto start = PageLayout::content_offset(page.id());
        ASSERT_TRUE(stob(pages_before.at(page.id().as_index())).range(start) == page.view(start));
    }

    auto assert_page_is_same_as_after(const Page &page) -> void
    {
        const auto start = PageLayout::content_offset(page.id());
        ASSERT_TRUE(stob(pages_after.at(page.id().as_index())).range(start) == page.view(start));
    }

    SharedMemory wal_backing;
    SharedMemory data_backing;
    FaultControls wal_faults;
    FaultControls data_faults;
    std::unique_ptr<IDirectory> home;
    std::unique_ptr<IBufferPool> pool;
    std::vector<std::string> pages_before;
    std::vector<std::string> pages_after;
    Random random {0};
};

TEST_F(WALTests, NewWALIsEmpty)
{
    ASSERT_FALSE(pool->can_commit());
}

TEST_F(WALTests, AllocationDoesNotAlterPage)
{
    ASSERT_TRUE(pool->release(allocate_page()));
    ASSERT_FALSE(pool->can_commit());
}

TEST_F(WALTests, UpdatesAreRegistered)
{
    auto page = allocate_page();
    alter_page(page);
    ASSERT_TRUE(pool->release(std::move(page)));
    ASSERT_TRUE(pool->can_commit());
    page = pool->acquire(PID::root(), false).value();
    assert_page_is_same_as_after(page);
}

TEST_F(WALTests, AbortRollsBackUpdates)
{
    auto page = allocate_page();
    alter_page(page);
    ASSERT_TRUE(pool->release(std::move(page)));
    ASSERT_TRUE(pool->abort());
    page = pool->acquire(PID::root(), false).value();
    assert_page_is_same_as_before(page);
}

TEST_F(WALTests, CommitIsACheckpoint)
{
    auto page = allocate_page();
    alter_page(page);
    ASSERT_TRUE(pool->release(std::move(page)));
    ASSERT_TRUE(pool->commit());
    ASSERT_FALSE(pool->abort());
    page = pool->acquire(PID::root(), false).value();
    assert_page_is_same_as_after(page);
}

TEST_F(WALTests, AbortSanityCheck)
{
    for (Index i {}; i < 100; ++i) {
        auto page = allocate_page();
        alter_page(page);
        ASSERT_TRUE(pool->release(std::move(page)));
    }
     ASSERT_TRUE(pool->abort());
    for (Index i {}; i < 100; ++i) {
        auto page = pool->acquire(PID::from_index(i), false).value();
        assert_page_is_same_as_before(page);
    }
}

TEST_F(WALTests, CommitSanityCheck)
{
    for (Index i {}; i < 100; ++i) {
        auto page = allocate_page();
        alter_page(page);
        ASSERT_TRUE(pool->release(std::move(page)));
    }
    ASSERT_TRUE(pool->commit());
    ASSERT_FALSE(pool->abort());
    for (Index i {}; i < 100; ++i) {
        auto page = pool->acquire(PID::from_index(i), false).value();
        assert_page_is_same_as_after(page);
    }
}


class MockWALTests: public WALTests {
public:
    static constexpr Size PAGE_SIZE = 0x200;

    ~MockWALTests() override = default;

    MockWALTests()
    {
        home = std::make_unique<MockDirectory>("WALReaderWriterTests");
        home_mock = dynamic_cast<MockDirectory*>(home.get());
    }

    auto setup(bool use_xact) -> void
    {
        EXPECT_CALL(*home_mock, open_file)
            .Times(2*use_xact + 1);

        pool = BufferPool::open({*home, create_sink(), LSN::null(), 16, 0, PAGE_SIZE, 0666, use_xact}).value();
        data_mock = home_mock->get_mock_file("data", Mode::CREATE | Mode::READ_WRITE);
        if (use_xact) {
            rwal_mock = home_mock->get_mock_file("wal", Mode::CREATE | Mode::READ_ONLY);
            wwal_mock = home_mock->get_mock_file("wal", Mode::CREATE | Mode::WRITE_ONLY | Mode::APPEND);
        }
    }

    MockDirectory *home_mock {};
    MockFile *rwal_mock {};
    MockFile *wwal_mock {};
    MockFile *data_mock {};
};

auto run_close_error_test(MockWALTests &test, MockFile &mock)
{
    using testing::Return;
    ON_CALL(mock, close)
        .WillByDefault(testing::Return(Err {Status::system_error("123")}));

    const auto r = test.pool->close();
    ASSERT_FALSE(r.has_value());
    ASSERT_TRUE(r.error().is_system_error());
    ASSERT_EQ(r.error().what(), "123");
}

TEST_F(MockWALTests, asdfgh)
{
    setup(true);

}

TEST_F(MockWALTests, DataFileCloseErrorIsPropagated)
{
    setup(true);
    run_close_error_test(*this, *data_mock);
}

TEST_F(MockWALTests, WALReaderFileCloseErrorIsPropagated)
{
    setup(true);
    run_close_error_test(*this, *rwal_mock);
}

TEST_F(MockWALTests, WALWriterFileCloseErrorIsPropagated)
{
    setup(true);
    run_close_error_test(*this, *wwal_mock);
}

TEST_F(MockWALTests, CannotCommitEmptyTransaction)
{
    setup(true);
    ASSERT_TRUE(pool->commit().error().is_logic_error());
}

TEST_F(MockWALTests, CannotAbortEmptyTransaction)
{
    setup(true);
    ASSERT_TRUE(pool->abort().error().is_logic_error());
}

TEST_F(MockWALTests, SystemErrorIsPropagated)
{
    using testing::_;
    setup(true);

    ON_CALL(*data_mock, write(_, _))
        .WillByDefault(testing::Return(Err {Status::system_error("123")}));

    // We should never call read() during page allocation. We would hit EOF anyway.
    EXPECT_CALL(*data_mock, read(_, _))
        .Times(0);

    for (; ; ) {
        auto p = pool->allocate();
        if (!p.has_value())
            break;
        p->set_type(PageType::INTERNAL_NODE);
        p->set_lsn(LSN {123});
        auto r = pool->release(std::move(*p));
        if (!r.has_value())
            break;
    }
    ASSERT_TRUE(pool->status().is_system_error());
    ASSERT_EQ(pool->status().what(), "123");
}

TEST_F(MockWALTests, CannotAbortIfNotUsingTransactions)
{
    setup(false);
    ASSERT_TRUE(pool->abort().error().is_logic_error());
}

} // <anonymous>
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

namespace {

using namespace cco;
using namespace cco::page;
using namespace cco::utils;

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
    ASSERT_EQ(reader->record(), std::nullopt);
    ASSERT_FALSE(reader->decrement().value());
    ASSERT_FALSE(reader->increment().value());
}

TEST_F(WALReaderWriterTests, WritesRecordCorrectly)
{
    WALRecordGenerator generator {PAGE_SIZE};
    ASSERT_TRUE(writer->append(generator.generate_small()));
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

auto test_writes_then_reads(WALReaderWriterTests &test, const std::vector<Size> &sizes) -> void
{
    WALRecordGenerator generator {WALReaderWriterTests::PAGE_SIZE};

    for (auto size: sizes)
        ASSERT_TRUE(test.writer->append(generator.generate(size, 10)));
    ASSERT_TRUE(test.writer->flush());
    ASSERT_TRUE(test.reader->reset());

    auto lsn = LSN::base();
    std::for_each(sizes.begin(), sizes.end(), [&generator, &lsn, &test](Size) {
        ASSERT_NE(test.reader->record(), std::nullopt);
        generator.validate_record(*test.reader->record(), LSN {lsn.value++});
        ASSERT_TRUE(test.reader->increment().has_value());
    });
}

TEST_F(WALReaderWriterTests, SingleSmallRecord)
{
    test_writes_then_reads(*this, {1});
}

TEST_F(WALReaderWriterTests, MultipleSmallRecords)
{
    test_writes_then_reads(*this, {1, 2, 3, 4, 5});
}

TEST_F(WALReaderWriterTests, LargeRecord)
{
    test_writes_then_reads(*this, {0x400});
}

TEST_F(WALReaderWriterTests, MultipleLargeRecords)
{
    test_writes_then_reads(*this, {0x400, 0x800, 0x1000, 0x1400, 0x1800});
}

TEST_F(WALReaderWriterTests, CursorStopsAtLastRecord)
{
    WALRecordGenerator generator {PAGE_SIZE};
    ASSERT_TRUE(writer->append(generator.generate_small()));
    ASSERT_TRUE(writer->append(generator.generate_small()));
    ASSERT_TRUE(writer->append(generator.generate_small()));
    ASSERT_TRUE(writer->flush());

    ASSERT_TRUE(reader->reset());
    generator.validate_record(*reader->record(), LSN {1});
    ASSERT_TRUE(reader->increment().value());
    generator.validate_record(*reader->record(), LSN {2});
    ASSERT_TRUE(reader->increment().value());
    generator.validate_record(*reader->record(), LSN {3});
    ASSERT_FALSE(reader->increment().value());
}

TEST_F(WALReaderWriterTests, TraversesIncompleteBlocks)
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

    ASSERT_TRUE(reader->reset());
    generator.validate_record(*reader->record(), LSN {1});
    ASSERT_TRUE(reader->increment().value());
    generator.validate_record(*reader->record(), LSN {2});
    ASSERT_TRUE(reader->increment().value());
    generator.validate_record(*reader->record(), LSN {3});
    ASSERT_TRUE(reader->increment().value());
    generator.validate_record(*reader->record(), LSN {4});
    ASSERT_TRUE(reader->increment().value());
    generator.validate_record(*reader->record(), LSN {5});
    ASSERT_TRUE(reader->increment().value());
    generator.validate_record(*reader->record(), LSN {6});
    ASSERT_FALSE(reader->increment().value());
}

TEST_F(WALReaderWriterTests, TraverseBackwardWithinBlock)
{
    WALRecordGenerator generator {PAGE_SIZE};
    ASSERT_TRUE(writer->append(generator.generate_small()));
    ASSERT_TRUE(writer->append(generator.generate_small()));
    ASSERT_TRUE(writer->append(generator.generate_small()));
    ASSERT_TRUE(writer->flush());

    ASSERT_TRUE(reader->reset());
    generator.validate_record(*reader->record(), LSN {1});
    ASSERT_TRUE(reader->increment().value());
    generator.validate_record(*reader->record(), LSN {2});
    ASSERT_TRUE(reader->increment().value());
    generator.validate_record(*reader->record(), LSN {3});
    ASSERT_FALSE(reader->increment().value());

    generator.validate_record(*reader->record(), LSN {3});
    ASSERT_TRUE(reader->decrement().value());

    ASSERT_TRUE(reader->increment().value());
    generator.validate_record(*reader->record(), LSN {3});
    ASSERT_TRUE(reader->decrement().value());

    generator.validate_record(*reader->record(), LSN {2});
    ASSERT_TRUE(reader->decrement().value());
    generator.validate_record(*reader->record(), LSN {1});
    ASSERT_FALSE(reader->decrement().value());
}

TEST_F(WALReaderWriterTests, TraverseBackwardBetweenBlocks)
{
    WALRecordGenerator generator {PAGE_SIZE};
    ASSERT_TRUE(writer->append(generator.generate_large()));
    ASSERT_TRUE(writer->append(generator.generate_large()));
    ASSERT_TRUE(writer->append(generator.generate_large()));
    ASSERT_TRUE(writer->flush());

    ASSERT_TRUE(reader->reset());
    generator.validate_record(*reader->record(), LSN {1});
    ASSERT_TRUE(reader->increment().value());
    generator.validate_record(*reader->record(), LSN {2});
    ASSERT_TRUE(reader->increment().value());
    generator.validate_record(*reader->record(), LSN {3});
    ASSERT_FALSE(reader->increment().value());

    generator.validate_record(*reader->record(), LSN {3});
    ASSERT_TRUE(reader->decrement().value());
    generator.validate_record(*reader->record(), LSN {2});
    ASSERT_TRUE(reader->decrement().value());
    generator.validate_record(*reader->record(), LSN {1});
    ASSERT_FALSE(reader->decrement().value());
}

template<class Test> auto test_write_records_and_traverse(Test &test, Size num_records, double large_fraction, double flush_fraction) -> void
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
    ASSERT_TRUE(test.reader->reset());

    // Read forward.
    for (Index i {}; i < num_records; ++i) {
        ASSERT_NE(test.reader->record(), std::nullopt) << "record " << i << " does not exist";
        ASSERT_TRUE(test.reader->record()->is_consistent()) << "record " << i << " is corrupted";
        generator.validate_record(*test.reader->record(), LSN {i + ROOT_ID_VALUE});
        ASSERT_EQ(test.reader->increment().value(), i < num_records - 1);
    }

    // Read backward.
    for (Index i {}; i < num_records - 1; ++i) {
        ASSERT_TRUE(test.reader->decrement());
        ASSERT_NE(test.reader->record(), std::nullopt);
        ASSERT_TRUE(test.reader->record()->is_consistent());
        generator.validate_record(*test.reader->record(), LSN {num_records - i - 1});
    }
}

TEST_F(WALReaderWriterTests, WriteAndTraverseSmallRecordsInCompleteBlocks)
{
    test_write_records_and_traverse(*this, 250, 0.0, 0.0);
}

TEST_F(WALReaderWriterTests, WriteAndTraverseLargeRecordsInCompleteBlocks)
{
    test_write_records_and_traverse(*this, 250, 1.0, 0.0);
}

TEST_F(WALReaderWriterTests, WriteAndTraverseMixedRecordsInCompleteBlocks)
{
    test_write_records_and_traverse(*this, 250, 0.5, 0.0);
}

TEST_F(WALReaderWriterTests, WriteAndTraverseSmallRecordsInIncompleteBlocks)
{
    test_write_records_and_traverse(*this, 250, 0.0, 0.5);
}

TEST_F(WALReaderWriterTests, WriteAndTraverseLargeRecordsInIncompleteBlocks)
{
    test_write_records_and_traverse(*this, 250, 1.0, 0.5);
}

TEST_F(WALReaderWriterTests, WriteAndTraverseMixedRecordsInIncompleteBlocks)
{
    test_write_records_and_traverse(*this, 250, 0.5, 0.5);
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
    test_write_records_and_traverse(*this, 250, 0.0, 0.0);
}

TEST_F(RealWALReaderWriterTests, WriteAndTraverseLargeRecordsInCompleteBlocks)
{
    test_write_records_and_traverse(*this, 250, 1.0, 0.0);
}

TEST_F(RealWALReaderWriterTests, WriteAndTraverseMixedRecordsInCompleteBlocks)
{
    test_write_records_and_traverse(*this, 250, 0.5, 0.0);
}

TEST_F(RealWALReaderWriterTests, WriteAndTraverseSmallRecordsInIncompleteBlocks)
{
    test_write_records_and_traverse(*this, 250, 0.0, 0.5);
}

TEST_F(RealWALReaderWriterTests, WriteAndTraverseLargeRecordsInIncompleteBlocks)
{
    test_write_records_and_traverse(*this, 250, 1.0, 0.5);
}

TEST_F(RealWALReaderWriterTests, WriteAndTraverseMixedRecordsInIncompleteBlocks)
{
    test_write_records_and_traverse(*this, 250, 0.5, 0.5);
}


class WALTests: public testing::Test {
public:
    static constexpr Size PAGE_SIZE = 0x100;

    WALTests()
    {
        home = std::make_unique<FakeDirectory>("WALReaderWriterTests");
        pool = BufferPool::open({*home, create_sink(), LSN::null(), 0, PAGE_SIZE, 0666, true}).value();
        wal = WALManager::open({pool.get(), *home, create_sink(), PAGE_SIZE, LSN::null()}).value();
        wal_backing = home->get_shared("wal");
        wal_faults = home->get_faults("wal");
        data_backing = home->get_shared("data");
        data_faults = home->get_faults("data");
    }

    ~WALTests() override = default;

    SharedMemory wal_backing;
    SharedMemory data_backing;
    FaultControls wal_faults;
    FaultControls data_faults;
    std::unique_ptr<FakeDirectory> home;
    std::unique_ptr<IBufferPool> pool;
    std::unique_ptr<IWALManager> wal;
};

} // <anonymous>
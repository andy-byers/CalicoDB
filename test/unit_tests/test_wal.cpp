
#include <gtest/gtest.h>

#include "common.h"
#include "bytes.h"
#include "utils/utils.h"
#include "file/file.h"
#include "wal/wal_reader.h"
#include "wal/wal_record.h"
#include "wal/wal_writer.h"

#include "fakes.h"
#include "random.h"
#include "tools.h"

namespace {

using namespace cub;

struct TestWALOptions {
    std::string path;
    Size block_size {};
    Size page_size {};
};

class WALTests: public testing::Test {
public:
    static constexpr Size BLOCK_SIZE = 0x400;
    static constexpr Size PAGE_SIZE = 0x100;

    WALTests()
    {
        m_options.block_size = BLOCK_SIZE;
        m_options.page_size = PAGE_SIZE;
        WALHarness harness {PAGE_SIZE};
        m_wal_backing = std::move(harness.backing);
        reader = std::move(harness.reader);
        writer = std::move(harness.writer);
    }

    ~WALTests() override = default;

    TestWALOptions m_options {"WALTests"};
    SharedMemory m_wal_backing;
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

TEST_F(WALTests, PayloadEncoding)
{
    WALRecordGenerator generator {BLOCK_SIZE};
    const auto record = generator.generate(0x10, 10);
    const auto update = record.payload().decode();
    ASSERT_EQ(update.changes.size(), 10);
}

TEST_F(WALTests, SingleSplit)
{
    WALRecordGenerator generator {BLOCK_SIZE};
    auto left = generator.generate(0x10, 10);
    const auto payload_size = left.payload().data().size();
    const auto split_offset = payload_size / 2;

    auto right = left.split(split_offset);
    assert_records_are_siblings(left, right, split_offset, payload_size);
}

TEST_F(WALTests, MultipleSplits)
{
    WALRecordGenerator generator {BLOCK_SIZE};
    auto left = generator.generate(0x10, 10);
    const auto payload_size = left.payload().data().size();
    const auto split_offset = payload_size / 3;

    auto middle = left.split(split_offset);
    assert_records_are_siblings(left, middle, split_offset, payload_size);

    auto right = middle.split(split_offset);
    assert_records_are_siblings(middle, right, split_offset, payload_size - split_offset);
}

TEST_F(WALTests, SingleMerge)
{
    WALRecordGenerator generator {BLOCK_SIZE};
    auto left = generator.generate(0x10, 10);
    const auto lsn = left.lsn();
    const auto crc = left.crc();
    const auto payload = _s(left.payload().data());
    auto right = left.split(left.payload().data().size() / 2);

    left.merge(right);
    ASSERT_EQ(left.lsn(), lsn);
    ASSERT_EQ(left.crc(), crc);
    ASSERT_EQ(left.type(), WALRecord::Type::FULL);
    ASSERT_EQ(_s(left.payload().data()), payload);
}

TEST_F(WALTests, MultipleMerges)
{
    WALRecordGenerator generator {BLOCK_SIZE};
    auto left = generator.generate(0x10, 10);
    const auto lsn = left.lsn();
    const auto crc = left.crc();
    const auto payload = _s(left.payload().data());
    auto middle = left.split(payload.size() / 3);
    auto right = middle.split(payload.size() / 3);

    left.merge(middle);
    left.merge(right);
    ASSERT_EQ(left.lsn(), lsn);
    ASSERT_EQ(left.crc(), crc);
    ASSERT_EQ(left.type(), WALRecord::Type::FULL);
    ASSERT_EQ(_s(left.payload().data()), payload);
}

TEST_F(WALTests, EmptyFileBehavior)
{
    ASSERT_EQ(reader->record(), std::nullopt);
    ASSERT_FALSE(reader->decrement());
    ASSERT_FALSE(reader->increment());
}

TEST_F(WALTests, WritesRecordCorrectly)
{
    WALRecordGenerator generator {BLOCK_SIZE};
    writer->write(generator.generate_small());
    writer->flush();

    const auto &memory = m_wal_backing.memory();
    WALRecord record;
    record.read(_b(memory));
    generator.validate_record(record, LSN::base());
}

TEST_F(WALTests, FlushedLSNReflectsLastFullRecord)
{
    WALRecordGenerator generator {BLOCK_SIZE};
    writer->write(generator.generate_small());

    // Writing this record should cause a try_flush after the FIRST part is written. The last record we wrote should
    // then be on disk, and the LAST part of the current record should be in the tail buffer.
    ASSERT_EQ(writer->write(generator.generate(BLOCK_SIZE / 2 * 3, 1)), LSN::base());
    ASSERT_EQ(writer->flush(), LSN {ROOT_ID_VALUE + 1});
}

auto test_writes_then_reads(WALTests &test, const std::vector<Size> &sizes) -> void
{
    WALRecordGenerator generator {WALTests::BLOCK_SIZE};

    for (auto size: sizes)
        test.writer->write(generator.generate(size, 10));
    test.writer->flush();
    test.reader->reset();

    auto lsn = LSN::base();
    std::for_each(sizes.begin(), sizes.end(), [&generator, &lsn, &test](Size) {
        ASSERT_NE(test.reader->record(), std::nullopt);
        generator.validate_record(*test.reader->record(), LSN {lsn.value++});
        test.reader->increment();
    });
}

TEST_F(WALTests, SingleSmallRecord)
{
    test_writes_then_reads(*this, {1});
}

TEST_F(WALTests, MultipleSmallRecords)
{
    test_writes_then_reads(*this, {1, 2, 3, 4, 5});
}

TEST_F(WALTests, LargeRecord)
{
    test_writes_then_reads(*this, {0x400});
}

TEST_F(WALTests, MultipleLargeRecords)
{
    test_writes_then_reads(*this, {0x400, 0x800, 0x1000, 0x1400, 0x1800});
}

TEST_F(WALTests, CursorStopsAtLastRecord)
{
    WALRecordGenerator generator {BLOCK_SIZE};
    writer->write(generator.generate_small());
    writer->write(generator.generate_small());
    writer->write(generator.generate_small());
    writer->flush();

    reader->reset();
    generator.validate_record(*reader->record(), LSN {1});
    ASSERT_TRUE(reader->increment());
    generator.validate_record(*reader->record(), LSN {2});
    ASSERT_TRUE(reader->increment());
    generator.validate_record(*reader->record(), LSN {3});
    ASSERT_FALSE(reader->increment());
}

TEST_F(WALTests, TraversesIncompleteBlocks)
{
    WALRecordGenerator generator {BLOCK_SIZE};

    writer->write(generator.generate_small());
    writer->flush();

    writer->write(generator.generate_small());
    writer->write(generator.generate_small());
    writer->flush();

    writer->write(generator.generate_small());
    writer->write(generator.generate_small());
    writer->write(generator.generate_small());
    writer->flush();

    reader->reset();
    generator.validate_record(*reader->record(), LSN {1});
    ASSERT_TRUE(reader->increment());
    generator.validate_record(*reader->record(), LSN {2});
    ASSERT_TRUE(reader->increment());
    generator.validate_record(*reader->record(), LSN {3});
    ASSERT_TRUE(reader->increment());
    generator.validate_record(*reader->record(), LSN {4});
    ASSERT_TRUE(reader->increment());
    generator.validate_record(*reader->record(), LSN {5});
    ASSERT_TRUE(reader->increment());
    generator.validate_record(*reader->record(), LSN {6});
    ASSERT_FALSE(reader->increment());
}

TEST_F(WALTests, TraverseBackwardWithinBlock)
{
    WALRecordGenerator generator {BLOCK_SIZE};
    writer->write(generator.generate_small());
    writer->write(generator.generate_small());
    writer->write(generator.generate_small());
    writer->flush();

    reader->reset();
    generator.validate_record(*reader->record(), LSN {1});
    ASSERT_TRUE(reader->increment());
    generator.validate_record(*reader->record(), LSN {2});
    ASSERT_TRUE(reader->increment());
    generator.validate_record(*reader->record(), LSN {3});
    ASSERT_FALSE(reader->increment());

    generator.validate_record(*reader->record(), LSN {3});
    ASSERT_TRUE(reader->decrement());

    ASSERT_TRUE(reader->increment());
    generator.validate_record(*reader->record(), LSN {3});
    ASSERT_TRUE(reader->decrement());

    generator.validate_record(*reader->record(), LSN {2});
    ASSERT_TRUE(reader->decrement());
    generator.validate_record(*reader->record(), LSN {1});
    ASSERT_FALSE(reader->decrement());
}

TEST_F(WALTests, TraverseBackwardBetweenBlocks)
{
    WALRecordGenerator generator {BLOCK_SIZE};
    writer->write(generator.generate_large());
    writer->write(generator.generate_large());
    writer->write(generator.generate_large());
    writer->flush();

    reader->reset();
    generator.validate_record(*reader->record(), LSN {1});
    ASSERT_TRUE(reader->increment());
    generator.validate_record(*reader->record(), LSN {2});
    ASSERT_TRUE(reader->increment());
    generator.validate_record(*reader->record(), LSN {3});
    ASSERT_FALSE(reader->increment());

    generator.validate_record(*reader->record(), LSN {3});
    ASSERT_TRUE(reader->decrement());
    generator.validate_record(*reader->record(), LSN {2});
    ASSERT_TRUE(reader->decrement());
    generator.validate_record(*reader->record(), LSN {1});
    ASSERT_FALSE(reader->decrement());
}

template<class Test> auto test_write_records_and_traverse(Test &test, Size num_records, double large_fraction, double flush_fraction) -> void
{
    WALRecordGenerator generator {WALTests::BLOCK_SIZE};

    auto make_choice = [&generator](double fraction) -> bool {
        return generator.random.next_real(1.0) < fraction;
    };

    for (Index i {}; i < num_records; ++i) {
        test.writer->write(make_choice(large_fraction)
            ? generator.generate_large()
            : generator.generate_small());
        // Always flush on the last round.
        if (make_choice(flush_fraction) || i == num_records - 1)
            test.writer->flush();
    }
    test.reader->reset();

    // Read forward.
    for (Index i {}; i < num_records; ++i) {
        ASSERT_NE(test.reader->record(), std::nullopt);
        ASSERT_TRUE(test.reader->record()->is_consistent());
        generator.validate_record(*test.reader->record(), LSN {i + ROOT_ID_VALUE});
        test.reader->increment();
    }

    // Read backward.
    for (Index i {}; i < num_records - 1; ++i) {
        test.reader->decrement();
        ASSERT_NE(test.reader->record(), std::nullopt);
        ASSERT_TRUE(test.reader->record()->is_consistent());
        generator.validate_record(*test.reader->record(), LSN {num_records - i - 1});
    }
}

TEST_F(WALTests, WriteAndTraverseSmallRecordsInCompleteBlocks)
{
    test_write_records_and_traverse(*this, 250, 0.0, 0.0);
}

TEST_F(WALTests, WriteAndTraverseLargeRecordsInCompleteBlocks)
{
    test_write_records_and_traverse(*this, 250, 1.0, 0.0);
}

TEST_F(WALTests, WriteAndTraverseMixedRecordsInCompleteBlocks)
{
    test_write_records_and_traverse(*this, 250, 0.5, 0.0);
}

TEST_F(WALTests, WriteAndTraverseSmallRecordsInIncompleteBlocks)
{
    test_write_records_and_traverse(*this, 250, 0.0, 0.5);
}

TEST_F(WALTests, WriteAndTraverseLargeRecordsInIncompleteBlocks)
{
    test_write_records_and_traverse(*this, 250, 1.0, 0.5);
}

TEST_F(WALTests, WriteAndTraverseMixedRecordsInIncompleteBlocks)
{
    test_write_records_and_traverse(*this, 250, 0.5, 0.5);
}

class RealWALTests: public testing::Test {
public:
    static constexpr Size BLOCK_SIZE = 0x400;
    static constexpr auto DB_PATH = "/tmp/cub_test_wal";

    RealWALTests()
    {
        const auto path = get_wal_path(DB_PATH);
        const auto mode = Mode::DIRECT | Mode::SYNCHRONOUS;
        std::filesystem::remove(path);
        writer = std::make_unique<WALWriter>(std::make_unique<LogFile>(path, Mode::CREATE | mode, 0666), BLOCK_SIZE);
        reader = std::make_unique<WALReader>(std::make_unique<ReadOnlyFile>(path, mode, 0666), BLOCK_SIZE);
    }

    ~RealWALTests() override = default;

    std::unique_ptr<IWALReader> reader;
    std::unique_ptr<IWALWriter> writer;
};

TEST_F(RealWALTests, WriteAndTraverseSmallRecordsInCompleteBlocks)
{
    test_write_records_and_traverse(*this, 250, 0.0, 0.0);
}

TEST_F(RealWALTests, WriteAndTraverseLargeRecordsInCompleteBlocks)
{
    test_write_records_and_traverse(*this, 250, 1.0, 0.0);
}

TEST_F(RealWALTests, WriteAndTraverseMixedRecordsInCompleteBlocks)
{
    test_write_records_and_traverse(*this, 250, 0.5, 0.0);
}

TEST_F(RealWALTests, WriteAndTraverseSmallRecordsInIncompleteBlocks)
{
    test_write_records_and_traverse(*this, 250, 0.0, 0.5);
}

TEST_F(RealWALTests, WriteAndTraverseLargeRecordsInIncompleteBlocks)
{
    test_write_records_and_traverse(*this, 250, 1.0, 0.5);
}

TEST_F(RealWALTests, WriteAndTraverseMixedRecordsInIncompleteBlocks)
{
    test_write_records_and_traverse(*this, 250, 0.5, 0.5);
}

} // <anonymous>
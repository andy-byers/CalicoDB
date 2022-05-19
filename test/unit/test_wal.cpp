
#include <gtest/gtest.h>

#include "common.h"
#include "utils/slice.h"
#include "utils/utils.h"
#include "wal/wal_reader.h"
#include "wal/wal_record.h"
#include "wal/wal_writer.h"

#include "fakes.h"
#include "random.h"

namespace {

using namespace cub;

class WALRecordGenerator {
public:
    explicit WALRecordGenerator(Size block_size)
        : m_block_size {block_size}
    {
        EXPECT_GE(block_size, 0);
        EXPECT_TRUE(is_power_of_two(block_size));
    }

    auto generate_small() -> WALRecord
    {
        const auto small_size = m_block_size / 0x10;
        const auto total_update_size = random.next_int(small_size, small_size * 2);
        const auto update_count = random.next_int(1UL, 5UL);
        const auto mean_update_size = total_update_size / update_count;
        return generate(mean_update_size, update_count);
    }

    auto generate_large() -> WALRecord
    {
        const auto large_size = m_block_size / 3 * 2;
        const auto total_update_size = random.next_int(large_size, large_size * 2);
        const auto update_count = random.next_int(1UL, 5UL);
        const auto mean_update_size = total_update_size / update_count;
        return generate(mean_update_size, update_count);
    }

    auto generate(Size mean_update_size, Size update_count) -> WALRecord
    {
        CUB_EXPECT_GT(mean_update_size, 0);
        constexpr Size page_count = 0x1000;
        const auto lower_bound = mean_update_size - mean_update_size/3;
        const auto upper_bound = mean_update_size + mean_update_size/3;
        const auto page_size = upper_bound;
        EXPECT_LE(page_size, std::numeric_limits<uint16_t>::max());

        m_snapshots_before.emplace_back(random.next_string(page_size));
        m_snapshots_after.emplace_back(random.next_string(page_size));
        std::vector<ChangedRegion> update {};

        for (Index i {}; i < update_count; ++i) {
            const auto size = random.next_int(lower_bound, upper_bound);
            const auto offset = random.next_int(page_size - size);

            update.emplace_back();
            update.back().offset = offset;
            update.back().before = to_bytes(m_snapshots_before.back()).range(offset, size);
            update.back().after = to_bytes(m_snapshots_after.back()).range(offset, size);
        }
        WALRecord record {{
            std::move(update),
            PID {static_cast<uint32_t>(random.next_int(page_count))},
            LSN::null(),
            LSN {static_cast<uint32_t>(m_payloads.size() + ROOT_ID_VALUE)},
        }};
        m_payloads.push_back(to_string(record.payload().data()));
        return record;
    }

    auto validate_record(const WALRecord &record, LSN target_lsn) const -> void
    {
        ASSERT_EQ(record.lsn(), target_lsn)
            << "Record has incorrect LSN";
        const auto payload = retrieve_payload(target_lsn);
        ASSERT_EQ(record.type(), WALRecord::Type::FULL)
            << "Record is incomplete";
        ASSERT_TRUE(record.payload().data() == to_bytes(payload))
            << "Record payload was corrupted";
        ASSERT_TRUE(record.is_consistent())
            << "Record has an inconsistent CRC";
    }

    auto retrieve_payload(LSN lsn) const -> const std::string&
    {
        return m_payloads.at(lsn.as_index());
    }

    Random random {0};

private:
    std::vector<std::string> m_payloads;
    std::vector<std::string> m_snapshots_before;
    std::vector<std::string> m_snapshots_after;
    Size m_block_size;
};

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
        m_reader = std::move(harness.reader);
        m_writer = std::move(harness.writer);
    }

    ~WALTests() override = default;

    TestWALOptions m_options {"WALTests"};
    SharedMemory m_wal_backing;
    std::unique_ptr<IWALReader> m_reader;
    std::unique_ptr<IWALWriter> m_writer;
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
    const auto payload = to_string(left.payload().data());
    auto right = left.split(left.payload().data().size() / 2);

    left.merge(right);
    ASSERT_EQ(left.lsn(), lsn);
    ASSERT_EQ(left.crc(), crc);
    ASSERT_EQ(left.type(), WALRecord::Type::FULL);
    ASSERT_EQ(to_string(left.payload().data()), payload);
}

TEST_F(WALTests, MultipleMerges)
{
    WALRecordGenerator generator {BLOCK_SIZE};
    auto left = generator.generate(0x10, 10);
    const auto lsn = left.lsn();
    const auto crc = left.crc();
    const auto payload = to_string(left.payload().data());
    auto middle = left.split(payload.size() / 3);
    auto right = middle.split(payload.size() / 3);

    left.merge(std::move(middle));
    left.merge(std::move(right));
    ASSERT_EQ(left.lsn(), lsn);
    ASSERT_EQ(left.crc(), crc);
    ASSERT_EQ(left.type(), WALRecord::Type::FULL);
    ASSERT_EQ(to_string(left.payload().data()), payload);
}

TEST_F(WALTests, EmptyFileBehavior)
{
    ASSERT_EQ(m_reader->record(), std::nullopt);
    ASSERT_FALSE(m_reader->decrement());
    ASSERT_FALSE(m_reader->increment());
}

TEST_F(WALTests, WritesRecordCorrectly)
{
    WALRecordGenerator generator {BLOCK_SIZE};
    m_writer->write(generator.generate_small());
    m_writer->flush();

    const auto &memory = m_wal_backing.memory();
    WALRecord record;
    record.read(to_bytes(memory));
    generator.validate_record(record, LSN::base());
}

TEST_F(WALTests, FlushedLSNReflectsLastFullRecord)
{
    WALRecordGenerator generator {BLOCK_SIZE};
    m_writer->write(generator.generate_small());

    // Writing this record should cause a flush after the FIRST part is written. The last record we wrote should
    // then be on disk, and the LAST part of the current record should be in the tail buffer.
    ASSERT_EQ(m_writer->write(generator.generate(BLOCK_SIZE / 2 * 3, 1)), LSN::base());
    ASSERT_EQ(m_writer->flush(), LSN {ROOT_ID_VALUE + 1});
}

auto test_writes_then_reads(WALTests &test, const std::vector<Size> &sizes) -> void
{
    WALRecordGenerator generator {WALTests::BLOCK_SIZE};

    for (auto size: sizes)
        test.m_writer->write(generator.generate(size, 10));
    test.m_writer->flush();
    test.m_reader->reset();

    auto lsn = LSN::base();
    std::for_each(sizes.begin(), sizes.end(), [&generator, &lsn, &test](Size) {
        ASSERT_NE(test.m_reader->record(), std::nullopt);
        generator.validate_record(*test.m_reader->record(), LSN {lsn.value++});
        test.m_reader->increment();
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
    test_writes_then_reads(*this, {0x1000});
}

TEST_F(WALTests, MultipleLargeRecords)
{
    test_writes_then_reads(*this, {0x1000, 0x2000, 0x3000, 0x4000, 0x5000});
}

TEST_F(WALTests, CursorStopsAtLastRecord)
{
    WALRecordGenerator generator {BLOCK_SIZE};
    m_writer->write(generator.generate_small());
    m_writer->write(generator.generate_small());
    m_writer->write(generator.generate_small());
    m_writer->flush();

    m_reader->reset();
    generator.validate_record(*m_reader->record(), LSN {1});
    ASSERT_TRUE(m_reader->increment());
    generator.validate_record(*m_reader->record(), LSN {2});
    ASSERT_TRUE(m_reader->increment());
    generator.validate_record(*m_reader->record(), LSN {3});
    ASSERT_FALSE(m_reader->increment());
}

TEST_F(WALTests, TraversesIncompleteBlocks)
{
    WALRecordGenerator generator {BLOCK_SIZE};

    m_writer->write(generator.generate_small());
    m_writer->flush();

    m_writer->write(generator.generate_small());
    m_writer->write(generator.generate_small());
    m_writer->flush();

    m_writer->write(generator.generate_small());
    m_writer->write(generator.generate_small());
    m_writer->write(generator.generate_small());
    m_writer->flush();

    m_reader->reset();
    generator.validate_record(*m_reader->record(), LSN {1});
    ASSERT_TRUE(m_reader->increment());
    generator.validate_record(*m_reader->record(), LSN {2});
    ASSERT_TRUE(m_reader->increment());
    generator.validate_record(*m_reader->record(), LSN {3});
    ASSERT_TRUE(m_reader->increment());
    generator.validate_record(*m_reader->record(), LSN {4});
    ASSERT_TRUE(m_reader->increment());
    generator.validate_record(*m_reader->record(), LSN {5});
    ASSERT_TRUE(m_reader->increment());
    generator.validate_record(*m_reader->record(), LSN {6});
    ASSERT_FALSE(m_reader->increment());
}

TEST_F(WALTests, TraverseBackwardWithinBlock)
{
    WALRecordGenerator generator {BLOCK_SIZE};
    m_writer->write(generator.generate_small());
    m_writer->write(generator.generate_small());
    m_writer->write(generator.generate_small());
    m_writer->flush();

    m_reader->reset();
    generator.validate_record(*m_reader->record(), LSN {1});
    ASSERT_TRUE(m_reader->increment());
    generator.validate_record(*m_reader->record(), LSN {2});
    ASSERT_TRUE(m_reader->increment());
    generator.validate_record(*m_reader->record(), LSN {3});
    ASSERT_FALSE(m_reader->increment());

    generator.validate_record(*m_reader->record(), LSN {3});
    ASSERT_TRUE(m_reader->decrement());

    ASSERT_TRUE(m_reader->increment());
    generator.validate_record(*m_reader->record(), LSN {3});
    ASSERT_TRUE(m_reader->decrement());

    generator.validate_record(*m_reader->record(), LSN {2});
    ASSERT_TRUE(m_reader->decrement());
    generator.validate_record(*m_reader->record(), LSN {1});
    ASSERT_FALSE(m_reader->decrement());
}

TEST_F(WALTests, TraverseBackwardBetweenBlocks)
{
    WALRecordGenerator generator {BLOCK_SIZE};
    m_writer->write(generator.generate_large());
    m_writer->write(generator.generate_large());
    m_writer->write(generator.generate_large());
    m_writer->flush();

    m_reader->reset();
    generator.validate_record(*m_reader->record(), LSN {1});
    ASSERT_TRUE(m_reader->increment());
    generator.validate_record(*m_reader->record(), LSN {2});
    ASSERT_TRUE(m_reader->increment());
    generator.validate_record(*m_reader->record(), LSN {3});
    ASSERT_FALSE(m_reader->increment());

    generator.validate_record(*m_reader->record(), LSN {3});
    ASSERT_TRUE(m_reader->decrement());
    generator.validate_record(*m_reader->record(), LSN {2});
    ASSERT_TRUE(m_reader->decrement());
    generator.validate_record(*m_reader->record(), LSN {1});
    ASSERT_FALSE(m_reader->decrement());
}

auto test_write_records_and_traverse(WALTests &test, Size num_records, float large_fraction, float flush_fraction) -> void
{
    WALRecordGenerator generator {WALTests::BLOCK_SIZE};

    auto make_choice = [&generator](float fraction) -> bool {
        return generator.random.next_real(1.0F) < fraction;
    };

    for (Index i {}; i < num_records; ++i) {
        test.m_writer->write(make_choice(large_fraction)
            ? generator.generate_large()
            : generator.generate_small());
        // Always flush on the last round.
        if (make_choice(flush_fraction) || i == num_records - 1)
            test.m_writer->flush();
    }
    test.m_reader->reset();

    // Read forward.
    for (Index i {}; i < num_records; ++i) {
        ASSERT_NE(test.m_reader->record(), std::nullopt);
        generator.validate_record(*test.m_reader->record(), LSN {static_cast<uint32_t>(i + ROOT_ID_VALUE)});
        test.m_reader->increment();
    }

    // Read backward.
    for (Index i {}; i < num_records - 1; ++i) {
        test.m_reader->decrement();
        ASSERT_NE(test.m_reader->record(), std::nullopt);
        generator.validate_record(*test.m_reader->record(), LSN {static_cast<uint32_t>(num_records - i - 1)});
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

} // <anonymous>
#include <array>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>

#include "calico/bytes.h"
#include "calico/options.h"
#include "calico/storage.h"
#include "fakes.h"
#include "pager/basic_pager.h"
#include "pager/framer.h"
#include "tools.h"
#include "unit_tests.h"
#include "utils/layout.h"
#include "utils/logging.h"
#include "utils/utils.h"
#include "wal/basic_wal.h"
#include "wal/helpers.h"
#include "wal/reader.h"
#include "wal/writer.h"

namespace calico {

namespace internal {
    extern std::uint32_t random_seed;
} // namespace internal

namespace fs = std::filesystem;

template<class Base>
class TestWithWalSegments: public Base {
public:
    [[nodiscard]]
    static auto get_segment_name(SegmentId id) -> std::string
    {
        return Base::ROOT + id.to_name();
    }

    [[nodiscard]]
    static auto get_segment_name(Size index) -> std::string
    {
        return Base::ROOT + SegmentId::from_index(index).to_name();
    }

    template<class Id>
    [[nodiscard]]
    auto get_segment_size(const Id &id) const -> Size
    {
        Size size {};
        EXPECT_TRUE(expose_message(Base::store->file_size(get_segment_name(id), size)));
        return size;
    }

    template<class Id>
    [[nodiscard]]
    auto get_segment_data(const Id &id) const -> std::string
    {
        RandomReader *reader {};
        EXPECT_TRUE(expose_message(Base::store->open_random_reader(get_segment_name(id), &reader)));

        std::string data(get_segment_size(id), '\x00');
        auto bytes = stob(data);
        EXPECT_TRUE(expose_message(reader->read(bytes, 0)));
        EXPECT_EQ(bytes.size(), data.size());
        delete reader;
        return data;
    }
};

using TestWithWalSegmentsOnHeap = TestWithWalSegments<TestOnHeap>;
using TestWithWalSegmentsOnDisk = TestWithWalSegments<TestOnDisk>;

template<class Store>
[[nodiscard]]
auto get_file_size(const Store &store, const std::string &path) -> Size
{
    Size size {};
    EXPECT_TRUE(expose_message(store.file_size(path, size)));
    return size;
}

[[nodiscard]]
auto open_and_write_file(Storage &store, const std::string &name, const std::string &in)
{
    RandomEditor *temp {};
    EXPECT_TRUE(expose_message(store.open_random_editor(name, &temp)));
    std::unique_ptr<RandomEditor> file {temp};
    EXPECT_TRUE(expose_message(file->write(stob(in), 0)));
}

class WalPayloadSizeLimitTests: public testing::TestWithParam<Size> {
public:
    WalPayloadSizeLimitTests()
        : scratch(max_size, '\x00'),
          image {random.get<std::string>('\x00', '\xFF', GetParam())}
    {
        static_assert(WAL_SCRATCH_SCALE >= 1);
    }

    ~WalPayloadSizeLimitTests() override = default;

    Size max_size {GetParam() * WAL_SCRATCH_SCALE};
    Size min_size {max_size - GetParam()};
    Random random {internal::random_seed};
    std::string scratch;
    std::string image;
};

TEST_P(WalPayloadSizeLimitTests, LargestPossibleRecord)
{
    std::vector<PageDelta> deltas;

    for (Size i {}; i < GetParam(); i += 2)
        deltas.emplace_back(PageDelta {i, 1});

    auto size = encode_deltas_payload(PageId {2}, stob(image), deltas, stob(scratch));
    ASSERT_GE(size, min_size) << "Excessive scratch memory allocated";
    ASSERT_LE(size, max_size) << "Scratch memory cannot fit maximally sized WAL record payload";
}

INSTANTIATE_TEST_SUITE_P(
    LargestPossibleRecord,
    WalPayloadSizeLimitTests,
    ::testing::Values(
        0x100,
        0x100 << 1,
        0x100 << 2,
        0x100 << 3,
        0x100 << 4,
        0x100 << 5,
        0x100 << 6,
        0x100 << 7
        )
);

class WalRecordMergeTests: public testing::Test {
public:
    auto setup(const std::array<WalRecordHeader::Type, 3> &types) -> void
    {
        lhs.type = types[0];
        rhs.type = types[1];
        lhs.size = 1;
        rhs.size = 2;
    }

    auto check(const WalRecordHeader &header, WalRecordHeader::Type type) -> bool
    {
        return header.type == type && header.size == 3;
    }

    std::vector<std::array<WalRecordHeader::Type, 3>> valid_left_merges {
        std::array<WalRecordHeader::Type, 3> {WalRecordHeader::Type {}, WalRecordHeader::Type::FIRST, WalRecordHeader::Type::FIRST},
        std::array<WalRecordHeader::Type, 3> {WalRecordHeader::Type {}, WalRecordHeader::Type::FULL, WalRecordHeader::Type::FULL},
        std::array<WalRecordHeader::Type, 3> {WalRecordHeader::Type::FIRST, WalRecordHeader::Type::MIDDLE, WalRecordHeader::Type::FIRST},
        std::array<WalRecordHeader::Type, 3> {WalRecordHeader::Type::FIRST, WalRecordHeader::Type::LAST, WalRecordHeader::Type::FULL},
    };
    std::vector<std::array<WalRecordHeader::Type, 3>> valid_right_merges {
        std::array<WalRecordHeader::Type, 3> {WalRecordHeader::Type::LAST, WalRecordHeader::Type {}, WalRecordHeader::Type::LAST},
        std::array<WalRecordHeader::Type, 3> {WalRecordHeader::Type::FULL, WalRecordHeader::Type {}, WalRecordHeader::Type::FULL},
        std::array<WalRecordHeader::Type, 3> {WalRecordHeader::Type::MIDDLE, WalRecordHeader::Type::LAST, WalRecordHeader::Type::LAST},
        std::array<WalRecordHeader::Type, 3> {WalRecordHeader::Type::FIRST, WalRecordHeader::Type::LAST, WalRecordHeader::Type::FULL},
    };
    WalRecordHeader lhs {};
    WalRecordHeader rhs {};
};

TEST_F(WalRecordMergeTests, MergeEmptyRecordsDeathTest)
{
    ASSERT_DEATH(const auto ignore = merge_records_left(lhs, rhs), EXPECTATION_MATCHER);
    ASSERT_DEATH(const auto ignore = merge_records_right(lhs, rhs), EXPECTATION_MATCHER);
}

TEST_F(WalRecordMergeTests, ValidLeftMerges)
{
    ASSERT_TRUE(std::all_of(cbegin(valid_left_merges), cend(valid_left_merges), [this](const auto &triplet) {
        setup(triplet);
        const auto s = merge_records_left(lhs, rhs);
        return s.is_ok() && check(lhs, triplet[2]);
    }));
}

TEST_F(WalRecordMergeTests, ValidRightMerges)
{
    ASSERT_TRUE(std::all_of(cbegin(valid_right_merges), cend(valid_right_merges), [this](const auto &triplet) {
        setup(triplet);
        const auto s = merge_records_right(lhs, rhs);
        return s.is_ok() && check(rhs, triplet[2]);
    }));
}

TEST_F(WalRecordMergeTests, MergeInvalidTypesDeathTest)
{
    setup({WalRecordHeader::Type::FIRST, WalRecordHeader::Type::FIRST});
    ASSERT_DEATH(const auto ignore = merge_records_left(lhs, rhs), EXPECTATION_MATCHER);
    ASSERT_DEATH(const auto ignore = merge_records_right(lhs, rhs), EXPECTATION_MATCHER);

    setup({WalRecordHeader::Type {}, WalRecordHeader::Type::MIDDLE});
    ASSERT_DEATH(const auto ignore = merge_records_left(lhs, rhs), EXPECTATION_MATCHER);
    ASSERT_DEATH(const auto ignore = merge_records_right(lhs, rhs), EXPECTATION_MATCHER);

    setup({WalRecordHeader::Type::MIDDLE, WalRecordHeader::Type::FIRST});
    ASSERT_DEATH(const auto ignore = merge_records_left(lhs, rhs), EXPECTATION_MATCHER);

    setup({WalRecordHeader::Type::FIRST, WalRecordHeader::Type::MIDDLE});
    ASSERT_DEATH(const auto ignore = merge_records_right(lhs, rhs), EXPECTATION_MATCHER);
}

class WalPayloadTests: public testing::Test {
public:
    static constexpr Size PAGE_SIZE {0x80};

    WalPayloadTests()
        : image {random.get<std::string>('\x00', '\xFF', PAGE_SIZE)},
          scratch(PAGE_SIZE * WAL_SCRATCH_SCALE, '\x00')
    {}

    Random random {internal::random_seed};
    std::string image;
    std::string scratch;
};

TEST_F(WalPayloadTests, EncodeAndDecodeFullImage)
{
    const auto size = encode_full_image_payload(PageId::root(), stob(image), stob(scratch));
    const auto descriptor = decode_full_image_payload(stob(scratch).truncate(size));
    ASSERT_EQ(descriptor.page_id, 1);
    ASSERT_EQ(descriptor.image.to_string(), image);
}

TEST_F(WalPayloadTests, EncodeAndDecodeDeltas)
{
    WalRecordGenerator generator;
    auto deltas = generator.setup_deltas(stob(image));
    const auto size = encode_deltas_payload(PageId::root(), stob(image), deltas, stob(scratch));

    WalRecordHeader header {};
    header.size = static_cast<std::uint16_t>(size);
    header.lsn = 123;
    header.crc = crc_32(stob(scratch).truncate(size));
    const auto descriptor = decode_deltas_payload(header, stob(scratch).truncate(size));
    ASSERT_EQ(descriptor.page_lsn, 123);
    ASSERT_EQ(descriptor.page_id, 1);
    ASSERT_EQ(descriptor.deltas.size(), deltas.size());
    ASSERT_FALSE(descriptor.is_commit);
    ASSERT_TRUE(std::all_of(cbegin(descriptor.deltas), cend(descriptor.deltas), [this](const DeltaContent &delta) {
        return delta.data == stob(image).range(delta.offset, delta.data.size());
    }));
}

class WalBufferTests: public testing::Test {
public:
    static constexpr Size BLOCK_SIZE {4};

    WalBuffer buffer {BLOCK_SIZE};
};

TEST_F(WalBufferTests, BufferIsSetUpCorrectly)
{
    ASSERT_EQ(buffer.block_number(), 0);
    ASSERT_EQ(buffer.block_offset(), 0);
    ASSERT_EQ(buffer.remaining().size(), BLOCK_SIZE);
    ASSERT_EQ(buffer.block().size(), BLOCK_SIZE);
}

TEST_F(WalBufferTests, OutOfBoundsCursorDeathTest)
{
    ASSERT_DEATH(buffer.advance_cursor(BLOCK_SIZE + 1), EXPECTATION_MATCHER);

    buffer.advance_cursor(1);
    ASSERT_DEATH(buffer.advance_cursor(BLOCK_SIZE), EXPECTATION_MATCHER);
}

TEST_F(WalBufferTests, KeepsTrackOfPosition)
{
    Random random {internal::random_seed};
    Size block_number {};
    Size block_offset {};

    const auto check = [&] {
        const auto block_numbers_match = buffer.block_number() == block_number;
        const auto block_offsets_match = buffer.block_offset() == block_offset;
        EXPECT_TRUE(block_numbers_match) << buffer.block_number() << " should equal " << block_number;
        EXPECT_TRUE(block_offsets_match) << buffer.block_offset() << " should equal " << block_offset;
        return block_numbers_match && block_offsets_match;
    };

    for (Size i {}; i < 100; ++i) {
        const auto size = random.get(buffer.remaining().size());
        block_offset += size;
        buffer.advance_cursor(size);
        ASSERT_TRUE(check());

        if (buffer.remaining().is_empty()) {
            block_offset = 0;
            block_number++;
            ASSERT_TRUE(expose_message(buffer.advance_block([] { return Status::ok(); })));
            ASSERT_TRUE(check());
        }
    }
}

TEST_F(WalBufferTests, MemoryIsReused)
{
    buffer.remaining()[0] = 'a';
    buffer.advance_cursor(1);
    buffer.remaining()[0] = 'b';
    buffer.advance_cursor(1);
    buffer.remaining()[0] = 'c';
    buffer.advance_cursor(1);
    buffer.remaining()[0] = 'd';
    buffer.advance_cursor(1);

    auto s = buffer.advance_block([] {return Status::ok();});
    ASSERT_TRUE(s.is_ok());

    ASSERT_EQ(buffer.block()[0], 'a');
    ASSERT_EQ(buffer.block()[1], 'b');
    ASSERT_EQ(buffer.block()[2], 'c');
    ASSERT_EQ(buffer.block()[3], 'd');
}

class WalRecordWriterTests: public TestWithWalSegmentsOnHeap {
public:
    static constexpr Size BLOCK_SIZE {0x200};

    WalRecordWriterTests() = default;

    ~WalRecordWriterTests() override = default;

    auto attach_writer(SegmentId id)
    {
        AppendWriter *file {};
        ASSERT_TRUE(expose_message(store->open_append_writer(ROOT + id.to_name(), &file)));
        writer.attach(file);
    }

    auto detach_writer()
    {
        ASSERT_TRUE(expose_message(writer.detach([](auto) {})));
    }

    WalRecordWriter writer {BLOCK_SIZE};
};

TEST_F(WalRecordWriterTests, NewWriterStateIsCorrect)
{
    ASSERT_FALSE(writer.has_written());
    ASSERT_FALSE(writer.is_attached());
    ASSERT_EQ(writer.block_count(), 0);
}

auto dummy_cb(SequenceId) -> Status
{
    return Status::ok();
}

TEST_F(WalRecordWriterTests, AdvancesToNewBlocksDuringWrite)
{
    attach_writer(SegmentId {1});
    auto lsn = SequenceId::base();
    Random random {internal::random_seed};

    while (writer.block_count() < 10) {
        const auto payload = random.get<std::string>('\x00', '\xFF', 10);
        writer.write(lsn++, stob(payload), dummy_cb);
    }
    ASSERT_TRUE(writer.has_written());
    detach_writer();

    ASSERT_EQ(get_file_size(*store, ROOT + SegmentId {1}.to_name()) / BLOCK_SIZE, 11);
}

TEST_F(WalRecordWriterTests, NonEmptyLastBlockIsWrittenAfterClose)
{
    const auto path = ROOT + SegmentId {1}.to_name();

    attach_writer(SegmentId {1});
    detach_writer();
    ASSERT_EQ(get_file_size(*store, path), 0);

    attach_writer(SegmentId {1});
    writer.write(SequenceId::base(), stob("payload!"), dummy_cb);
    detach_writer();
    ASSERT_EQ(get_file_size(*store, path), BLOCK_SIZE);
}

TEST_F(WalRecordWriterTests, ClearsRestOfBlock)
{
    const auto path = ROOT + SegmentId {1}.to_name();
    std::string payload {"payload!"};
    const SegmentId id {1};

    attach_writer(id);
    writer.write(SequenceId::base(), stob(payload), dummy_cb);
    detach_writer();

    auto result = get_segment_data(id).substr(sizeof(WalRecordHeader));
    payload.resize(result.size());
    ASSERT_EQ(payload, result);
}

[[nodiscard]]
auto get_ids(const WalCollection &c)
{
    std::vector<SegmentId> ids;
    std::transform(cbegin(c.segments()), cend(c.segments()), back_inserter(ids), [](const auto &itr) {
        return itr.id;
    });
    return ids;
}

class WalCollectionTests: public testing::Test {
public:
    static auto test_has_commit(SegmentId id) -> bool
    {
        return id.as_index() & 1;
    }

    auto add_segments(Size n)
    {
        for (Size i {}; i < n; ++i) {
            auto id = SegmentId::from_index(i);
            collection.add_segment({id, test_has_commit(id)});
        }
        ASSERT_EQ(collection.most_recent_id(), SegmentId::from_index(n - 1));
    }

    WalCollection collection;
};

TEST_F(WalCollectionTests, NewCollectionState)
{
    ASSERT_TRUE(collection.most_recent_id().is_null());
}

TEST_F(WalCollectionTests, AddSegment)
{
    collection.add_segment({SegmentId {1}});
    ASSERT_EQ(collection.most_recent_id().value, 1);
}

TEST_F(WalCollectionTests, RecordsMostRecentSegmentId)
{
    add_segments(20);
    ASSERT_EQ(collection.most_recent_id(), SegmentId::from_index(19));
}

template<class Itr>
[[nodiscard]]
auto contains_n_consecutive_segments(const Itr &begin, const Itr &end, SegmentId id, Size n)
{
    return std::distance(begin, end) == std::ptrdiff_t(n) && std::all_of(begin, end, [&id](auto current) {
        return current.value == id.value++;
    });
}

TEST_F(WalCollectionTests, RecordsSegmentInfoCorrectly)
{
    add_segments(20);

    const auto ids = get_ids(collection);
    ASSERT_EQ(ids.size(), 20);

    const auto result = get_ids(collection);
    ASSERT_TRUE(contains_n_consecutive_segments(cbegin(result), cend(result), SegmentId {1}, 20));
}

TEST_F(WalCollectionTests, RemovesAllSegmentsFromLeft)
{
    add_segments(20);
    ASSERT_TRUE(expose_message(collection.remove_from_left(SegmentId::from_index(20), [](auto) {return Status::ok();})));

    const auto ids = get_ids(collection);
    ASSERT_TRUE(ids.empty());
}

TEST_F(WalCollectionTests, RemovesAllSegmentsFromRight)
{
    add_segments(20);
    ASSERT_TRUE(expose_message(collection.remove_from_right(SegmentId::from_index(0), [](auto) {return Status::ok();})));

    const auto ids = get_ids(collection);
    ASSERT_TRUE(ids.empty());
}

TEST_F(WalCollectionTests, RemovesSomeSegmentsFromLeft)
{
    add_segments(20);
    ASSERT_TRUE(expose_message(collection.remove_from_left(SegmentId::from_index(10), [](auto) {return Status::ok();})));

    const auto ids = get_ids(collection);
    ASSERT_TRUE(contains_n_consecutive_segments(cbegin(ids), cend(ids), SegmentId::from_index(10), 10));
}

TEST_F(WalCollectionTests, RemovesSomeSegmentsFromRight)
{
    add_segments(20);
    ASSERT_TRUE(expose_message(collection.remove_from_right(SegmentId::from_index(10), [](auto) {return Status::ok();})));

    const auto ids = get_ids(collection);
    ASSERT_TRUE(contains_n_consecutive_segments(cbegin(ids), cend(ids), SegmentId::from_index(0), 10));
}

class BackgroundWriterTests: public TestOnDisk {
public:
    static constexpr Size PAGE_SIZE {0x100};
    static constexpr Size BLOCK_SIZE {PAGE_SIZE * WAL_BLOCK_SCALE};

    BackgroundWriterTests()
        : scratch {std::make_unique<LogScratchManager>(PAGE_SIZE * WAL_SCRATCH_SCALE)}
    {}

    auto SetUp() -> void override
    {
        writer = std::make_unique<BackgroundWriter>(BackgroundWriter::Parameters {
            store.get(),
            scratch.get(),
            &collection,
            &flushed_lsn,
            create_logger(create_sink(), "wal"),
            ROOT,
            BLOCK_SIZE,
        });
    }

    auto TearDown() -> void override
    {
        ASSERT_TRUE(expose_message(std::move(*writer).destroy()));
    }

    [[nodiscard]]
    auto get_commit_event(SequenceId lsn)
    {
        BackgroundWriter::Event event {};
        event.lsn = lsn;
        event.type = BackgroundWriter::EventType::LOG_COMMIT;
        return event;
    }

    [[nodiscard]]
    auto get_update_event(SequenceId lsn)
    {
        auto event = get_commit_event(lsn);
        event.type = random.get(3) == 0
            ? BackgroundWriter::EventType::LOG_FULL_IMAGE
            : BackgroundWriter::EventType::LOG_DELTAS;
        auto buffer = scratch->get();
        event.size = random.get(10ULL, buffer->size());
        const auto data = random.get<std::string>('\x00', '\xFF', event.size);
        mem_copy(*buffer, stob(data));
        event.buffer = buffer;
        return event;
    }

    WalCollection collection;
    std::atomic<SequenceId> flushed_lsn;
    std::atomic<SequenceId> pager_lsn;
    std::unique_ptr<LogScratchManager> scratch;
    std::unique_ptr<BackgroundWriter> writer;
    Random random {internal::random_seed};
};

TEST_F(BackgroundWriterTests, NewWriterState)
{
    ASSERT_TRUE(expose_message(writer->status()));
}

//TEST_F(BackgroundWriterTests, StartAndStopRepeatedly)
//{
//    // Should be run with TSan every once in a while!
//    for (Size i {}; i < 100; ++i) {
//        writer->startup();
//        writer->destroy();
//        ASSERT_TRUE(expose_message(writer->status()));
//    }
//}

//TEST_F(BackgroundWriterTests, WriterCleansUp)
//{
//    writer->dispatch(get_update_event(SequenceId::from_index(0)));
//    ASSERT_TRUE(expose_message(writer->status()));
//
//    writer->dispatch(BackgroundWriter::Event {
//        BackgroundWriter::EventType::STOP_WRITER,
//        SequenceId::from_index(0),
//        std::nullopt,
//        0,
//    }, true);
//
//    const auto ids = get_ids(collection);
//    ASSERT_EQ(ids.size(), 1);
//    ASSERT_EQ(ids[0].value, 1);
//}

TEST_F(BackgroundWriterTests, WriteUpdates)
{
    for (Size i {}; i < 100; ++i) {
        writer->dispatch(get_update_event(SequenceId::from_index(i)));
        ASSERT_TRUE(expose_message(writer->status()));
    }
    writer->dispatch(get_commit_event(SequenceId::from_index(100)), true);


    const auto ids = get_ids(collection);
    ASSERT_FALSE(ids.empty());
}

// TODO: Considering using a WAL iterator construct instead of the WAL reader class. Lay out all the intended functionality in tests here.
//TEST_F(WalIteratorTests, CannotBeOpenedOnNonexistentSegment)
//{
//    auto itr = create_iterator();
//    ASSERT_TRUE(itr.open(SegmentId {1}).is_system_error());
//}
//
//TEST_F(WalIteratorTests, CannotBeOpenedOnEmptySegment)
//{
//    auto itr = create_iterator();
//    create_random_segment(SegmentId {1}, 0);
//    ASSERT_TRUE(itr.open(SegmentId {1}).is_not_found());
//}
//
//TEST_F(WalIteratorTests, CannotBeIncrementedPastLastRecord)
//{
//    auto itr = create_iterator();
//    create_random_segment(SegmentId {1}, 1);
//
//    // We should be open and positioned on the first (and only) record.
//    ASSERT_TRUE(expose_message(itr.open(SegmentId {1})));
//    ASSERT_FALSE(itr.payload().is_empty());
//    ASSERT_FALSE(itr.increment());
//}
//
//auto increment_and_collect(BasicWalIterator &itr)
//{
//    do {
//
//    } while (itr.increment());
//}
//
//TEST_F(WalIteratorTests, IncrementInsideBlock)
//{
//    auto itr = create_iterator();
//    create_random_segment(SegmentId {1}, 1);
//
//    // We should be open and positioned on the first (and only) record.
//    ASSERT_TRUE(expose_message(itr.open(SegmentId {1})));
//    ASSERT_FALSE(itr.payload().is_empty());
//    ASSERT_FALSE(itr.increment());
//}
//
//auto decrement_and_collect(WalIteratorTests &test)
//{
//
//}





template<class Reader>
class LogReaderTests: public TestWithWalSegmentsOnHeap {
public:
    static constexpr Size BLOCK_SIZE {4};

    LogReaderTests()
    {
        open_and_write_file(*store, get_segment_name(1), "01234567");
        open_and_write_file(*store, get_segment_name(2), "89012345");
        open_and_write_file(*store, get_segment_name(3), "67890123");
        result = "012345678901234567890123";
    }

    ~LogReaderTests() override
    {
        if (reader.is_attached())
            delete reader.detach();
    }

    auto open_file_and_attach_reader(SegmentId id) -> void
    {
        const auto path = get_segment_name(id.value);
        EXPECT_TRUE(expose_message(store->open_random_reader(path, &file)));
        EXPECT_TRUE(expose_message(reader.attach(file)));
    }

    Reader reader {BLOCK_SIZE};
    std::string result;
    RandomReader *file {};
};

class SequentialLogReaderTests: public LogReaderTests<SequentialLogReader> {
public:
    SequentialLogReaderTests()
    {
        open_file_and_attach_reader(SegmentId {1});
    }
};

TEST_F(SequentialLogReaderTests, NewWriterStartsAtBeginning)
{
    ASSERT_EQ(reader.position().offset.value, 0);
    ASSERT_EQ(reader.position().number.value, 0);
}

TEST_F(SequentialLogReaderTests, OutOfBoundsCursorDeathTest)
{
    ASSERT_DEATH(reader.advance_cursor(5), EXPECTATION_MATCHER);
}

auto randomly_read_from_segment(Random &random, SequentialLogReader &reader) -> std::string
{
    std::string out;
    for (; ; ) {
        if (reader.remaining().is_empty()) {
            const auto s = reader.advance_block();
            if (s.is_logic_error())
                break;
            EXPECT_TRUE(s.is_ok()) << "Error: " << s.what();
        } else {
            const auto n = random.get(1ULL, reader.remaining().size());
            std::string chunk(n, '\x00');
            mem_copy(stob(chunk), reader.remaining().truncate(n));
            out += chunk;
            reader.advance_cursor(n);
        }
    }
    return out;
}

TEST_F(SequentialLogReaderTests, ReadsAndAdvancesWithinSegment)
{
    Random random {internal::random_seed};
    ASSERT_EQ(randomly_read_from_segment(random, reader), result.substr(0, 8));
}

TEST_F(SequentialLogReaderTests, ReadsAndAdvancesBetweenSegments)
{
    Random random {internal::random_seed};
    auto answer = randomly_read_from_segment(random, reader);
    open_file_and_attach_reader(SegmentId {2});
    answer += randomly_read_from_segment(random, reader);
    open_file_and_attach_reader(SegmentId {3});
    answer += randomly_read_from_segment(random, reader);
    ASSERT_EQ(answer, result);
}

class RandomLogReaderTests: public LogReaderTests<RandomLogReader> {
public:
    RandomLogReaderTests()
    {
        open_file_and_attach_reader(SegmentId {1});
    }
};

auto append_bytes_at(RandomLogReader &reader, LogPosition position, Size num_bytes, std::string &out)
{
    Bytes temp;
    ASSERT_TRUE(expose_message(reader.present(position, temp)));
    out.resize(out.size() + num_bytes);
    mem_copy(stob(out).advance(out.size() - num_bytes), temp.truncate(num_bytes));
}

TEST_F(RandomLogReaderTests, ReadsRecordsWithinBlock)
{
    std::string answer;
    append_bytes_at(reader, LogPosition {BlockNumber {0}, BlockOffset {0}}, 3, answer);
    append_bytes_at(reader, LogPosition {BlockNumber {0}, BlockOffset {3}}, 1, answer);
    ASSERT_EQ(answer, result.substr(0, answer.size()));
}

TEST_F(RandomLogReaderTests, ReadsRecordsBetweenBlocks)
{
    std::string answer;
    append_bytes_at(reader, LogPosition {BlockNumber {0}, BlockOffset {0}}, 2, answer);
    append_bytes_at(reader, LogPosition {BlockNumber {0}, BlockOffset {2}}, 2, answer);
    append_bytes_at(reader, LogPosition {BlockNumber {1}, BlockOffset {0}}, 1, answer);
    append_bytes_at(reader, LogPosition {BlockNumber {1}, BlockOffset {1}}, 3, answer);
    ASSERT_EQ(answer, result.substr(0, answer.size()));
}

class SegmentGuardTests: public TestWithWalSegmentsOnHeap {
public:
    static constexpr Size PAGE_SIZE {0x100};

    SegmentGuardTests()
        : writer {PAGE_SIZE * WAL_BLOCK_SCALE}
    {}

    [[nodiscard]]
    auto create_guard() -> SegmentGuard
    {
        return SegmentGuard {*store, writer, collection, flushed_lsn, ROOT};
    }

    auto assert_components_are_started() const -> void
    {
        ASSERT_TRUE(writer.is_attached());
    }

    auto assert_components_are_stopped() const -> void
    {
        ASSERT_FALSE(writer.is_attached());
    }

    WalCollection collection;
    WalRecordWriter writer;
    std::atomic<SequenceId> flushed_lsn;
};

TEST_F(SegmentGuardTests, NewGuardIsNotStarted)
{
    auto guard = create_guard();
    ASSERT_FALSE(guard.is_started());
    assert_components_are_stopped();
}

TEST_F(SegmentGuardTests, StartAndFinish)
{
    auto guard = create_guard();
    ASSERT_TRUE(expose_message(guard.start()));
    ASSERT_TRUE(guard.is_started());
    assert_components_are_started();

    ASSERT_TRUE(expose_message(guard.finish(false)));
    ASSERT_FALSE(guard.is_started());
    assert_components_are_stopped();

    ASSERT_EQ(collection.segments().size(), 1);
    const auto segment = cbegin(collection.segments());
    ASSERT_EQ(segment->id.value, 1);
    ASSERT_FALSE(segment->has_commit);
}

TEST_F(SegmentGuardTests, StartAndFinishWithCommit)
{
    auto guard = create_guard();
    ASSERT_TRUE(expose_message(guard.start()));
    ASSERT_TRUE(expose_message(guard.finish(true)));

    ASSERT_EQ(collection.segments().size(), 1);
    const auto segment = cbegin(collection.segments());
    ASSERT_EQ(segment->id.value, 1);
    ASSERT_TRUE(segment->has_commit);
}

TEST_F(SegmentGuardTests, BehavesLikeScopeGuard)
{
    {
        auto guard = create_guard();
        ASSERT_TRUE(expose_message(guard.start()));
    }

    assert_components_are_stopped();
    ASSERT_TRUE(collection.segments().empty());
}

TEST_F(SegmentGuardTests, DoubleStartDeathTest)
{
    auto guard = create_guard();
    ASSERT_TRUE(expose_message(guard.start()));
    ASSERT_DEATH(const auto unused = guard.start(), EXPECTATION_MATCHER);
}

TEST_F(SegmentGuardTests, DoubleFinishDeathTest)
{
    auto guard = create_guard();
    ASSERT_TRUE(expose_message(guard.start()));
    ASSERT_TRUE(expose_message(guard.finish(true)));
    ASSERT_DEATH(const auto unused = guard.finish(true), EXPECTATION_MATCHER);
}

TEST_F(SegmentGuardTests, NotStartedDeathTest)
{
    auto guard = create_guard();
    ASSERT_DEATH(const auto unused = guard.abort(), EXPECTATION_MATCHER);
    ASSERT_DEATH(const auto unused = guard.finish(true), EXPECTATION_MATCHER);
}

class BasicWalReaderWriterTests: public TestWithWalSegmentsOnHeap {
public:
    static constexpr Size PAGE_SIZE {0x100};
    static constexpr Size BLOCK_SIZE {PAGE_SIZE * WAL_BLOCK_SCALE};

    BasicWalReaderWriterTests()
        : scratch {std::make_unique<LogScratchManager>(PAGE_SIZE * WAL_SCRATCH_SCALE)}
    {}

    auto SetUp() -> void override
    {
        reader = std::make_unique<BasicWalReader>(
            *store,
            ROOT,
            PAGE_SIZE
        );

        writer = std::make_unique<BasicWalWriter>(BasicWalWriter::Parameters {
            store.get(),
            &collection,
            &flushed_lsn,
            &pager_lsn,
            create_logger(create_sink(), "wal"),
            ROOT,
            PAGE_SIZE,
            128,
        });
    }

    WalCollection collection;
    std::atomic<SequenceId> flushed_lsn {};
    std::atomic<SequenceId> pager_lsn {};
    std::unique_ptr<LogScratchManager> scratch;
    std::unique_ptr<BasicWalReader> reader;
    std::unique_ptr<BasicWalWriter> writer;
    Random random {internal::random_seed};
};

TEST_F(BasicWalReaderWriterTests, NewWriterIsOk)
{
    ASSERT_TRUE(writer->status().is_ok());
    writer->stop();
}

TEST_F(BasicWalReaderWriterTests, WritesAndReadsDeltasNormally)
{
    // NOTE: This test doesn't handle segmentation. If the writer segments, the test will fail!
    static constexpr Size NUM_RECORDS {100};
    WalRecordGenerator generator;
    std::vector<std::vector<PageDelta>> deltas;
    std::vector<std::string> images;

    for (Size i {}; i < NUM_RECORDS; ++i) {
        images.emplace_back(random.get<std::string>('\x00', '\xFF', PAGE_SIZE));
        deltas.emplace_back(generator.setup_deltas(stob(images.back())));
        writer->log_deltas(PageId::root(), stob(images.back()), deltas[i]);
    }
    // close() should cause the writer to flush the current block.
    writer->stop();

    std::vector<RecordPosition> positions;
    ASSERT_TRUE(expose_message(reader->open(SegmentId {1})));

    Size i {};
    ASSERT_TRUE(expose_message(reader->redo(positions, [deltas, &i, images](const RedoDescriptor &descriptor) {
        auto lhs = cbegin(descriptor.deltas);
        auto rhs = cbegin(deltas[i]);
        for (; rhs != cend(deltas[i]); ++lhs, ++rhs) {
            EXPECT_NE(lhs, cend(descriptor.deltas));
            EXPECT_TRUE(lhs->data == stob(images[i]).range(rhs->offset, rhs->size));
            EXPECT_EQ(lhs->offset, rhs->offset);
        }
        i++;
        return Status::ok();
    })));

    ASSERT_EQ(get_segment_size(0UL) % BLOCK_SIZE, 0);
}

TEST_F(BasicWalReaderWriterTests, WritesAndReadsFullImagesNormally)
{
    // NOTE: This test doesn't handle segmentation. If the writer segments, the test will fail!
    static constexpr Size NUM_RECORDS {100};
    std::vector<std::string> images;

    for (Size i {}; i < NUM_RECORDS; ++i) {
        images.emplace_back(random.get<std::string>('\x00', '\xFF', PAGE_SIZE));
        writer->log_full_image(PageId::from_index(i), stob(images.back()));
    }
    writer->stop();

    std::vector<RecordPosition> positions;
    ASSERT_TRUE(expose_message(reader->open(SegmentId {1})));

    ASSERT_TRUE(expose_message(reader->redo(positions, [](const auto&) {
        ADD_FAILURE() << "This should not be called";
        return Status::logic_error("Logic error!");
    })));

    Size i {};
    ASSERT_TRUE(expose_message(reader->undo(crbegin(positions), crend(positions), [&i, images](const UndoDescriptor &descriptor) {
        const auto n = NUM_RECORDS - i - 1;
        EXPECT_EQ(descriptor.page_id, n + 1);
        EXPECT_TRUE(descriptor.image == stob(images[n]));
        i++;
        return Status::ok();
    })));

    ASSERT_EQ(get_segment_size(0UL) % BLOCK_SIZE, 0);
}

auto test_undo_redo(BasicWalReaderWriterTests &test, Size num_images, Size num_deltas)
{
    const auto deltas_per_image = num_deltas / num_images;

    std::vector<std::string> before_images;
    std::vector<std::string> after_images;
    WalRecordGenerator generator;

    auto &reader = test.reader;
    auto &writer = test.writer;
    auto &random = test.random;
    auto &collection = test.collection;

    for (Size i {}; i < num_images; ++i) {
        auto pid = PageId::from_index(i);

        before_images.emplace_back(random.get<std::string>('\x00', '\xFF', BasicWalReaderWriterTests::PAGE_SIZE));
        writer->log_full_image(pid, stob(before_images.back()));

        after_images.emplace_back(before_images.back());
        for (Size j {}; j < deltas_per_image; ++j) {
            const auto deltas = generator.setup_deltas(stob(after_images.back()));
            writer->log_deltas(pid, stob(after_images.back()), deltas);
        }
    }
    writer->stop();

    // Roll forward some copies of the "before images" to match the "after images".
    std::vector<std::vector<RecordPosition>> all_positions;
    auto images = before_images;
    for (const auto &[id, meta]: collection.segments()) {
        all_positions.emplace_back();
        ASSERT_TRUE(expose_message(reader->open(id)));
        ASSERT_TRUE(expose_message(reader->redo(all_positions.back(), [&images](const RedoDescriptor &info) {
            auto image = stob(images.at(info.page_id - 1));
            for (const auto &[offset, content]: info.deltas)
                mem_copy(image.range(offset, content.size()), content);
            return Status::ok();
        })));
        ASSERT_TRUE(expose_message(reader->close()));
    }

    // Image copies should match the "after images".
    for (Size i {}; i < images.size(); ++i) {
        ASSERT_EQ(images.at(i), after_images.at(i));
    }

    // Now roll them back to match the before images again.
    for (auto itr = crbegin(all_positions); itr != crend(all_positions); ++itr) {
        // Segment ID should be the same for each record position within each group.
        ASSERT_TRUE(expose_message(reader->open(itr->begin()->id)));
        ASSERT_TRUE(expose_message(reader->undo(crbegin(*itr), crend(*itr), [&images](const auto &info) {
            const auto index = info.page_id - 1;
            mem_copy(stob(images[index]), info.image);
            return Status::ok();
        })));
        ASSERT_TRUE(expose_message(reader->close()));
    }

    for (Size i {}; i < images.size(); ++i) {
        ASSERT_EQ(images.at(i), before_images.at(i));
    }
}

TEST_F(BasicWalReaderWriterTests, SingleImage)
{
    // This situation should not happen in practice, but we technically should be able to handle it.
    test_undo_redo(*this, 1, 0);
}

TEST_F(BasicWalReaderWriterTests, SingleImageSingleDelta)
{
    test_undo_redo(*this, 1, 1);
}

TEST_F(BasicWalReaderWriterTests, SingleImageManyDeltas)
{
    test_undo_redo(*this, 1, 100);
}

TEST_F(BasicWalReaderWriterTests, ManyImagesManyDeltas)
{
    test_undo_redo(*this, 100, 1'000);
}

//TEST_F(BasicWalReaderWriterTests, ManyManyImagesManyManyDeltas)
//{
//    test_undo_redo(*this, 10'000, 1'000'000);
//}

class BasicWalTests: public TestWithWalSegmentsOnHeap {
public:
    ~BasicWalTests() override = default;

    auto SetUp() -> void override
    {
        WriteAheadLog *temp {};

        ASSERT_TRUE(expose_message(BasicWriteAheadLog::open({
            ROOT,
            store.get(),
            create_sink(),
            0x100,
        }, &temp)));

        wal.reset(temp);

        ASSERT_TRUE(expose_message(wal->setup_and_recover([](const auto &) { return Status::logic_error(""); },
                                                             [](const auto &) { return Status::logic_error(""); })));
    }

    std::unique_ptr<WriteAheadLog> wal;
};

TEST_F(BasicWalTests, StartsAndStops)
{
    ASSERT_TRUE(expose_message(wal->start_workers()));
    ASSERT_TRUE(expose_message(wal->stop_workers()));
}

TEST_F(BasicWalTests, NewWalState)
{
    ASSERT_TRUE(expose_message(wal->start_workers()));
    ASSERT_EQ(wal->flushed_lsn(), 0);
    ASSERT_EQ(wal->current_lsn(), 1);
    ASSERT_TRUE(expose_message(wal->stop_workers()));
}

TEST_F(BasicWalTests, WriterDoesNotLeaveEmptySegments)
{
    std::vector<std::string> children;

    for (Size i {}; i < 10; ++i) {
        ASSERT_TRUE(expose_message(wal->start_workers()));

        // File should be deleted before this method returns, if no records were written to it.
        ASSERT_TRUE(expose_message(wal->stop_workers()));
        ASSERT_TRUE(expose_message(store->get_children(ROOT, children)));
        ASSERT_TRUE(children.empty());
    }
}

//TEST_F(BasicWalTests, FailureDuringOpen)
//{
//    interceptors::set_open(FailOnce<0> {"test/wal-"});
//    ASSERT_TRUE(expose_message(wal->start_workers()));
//    ASSERT_TRUE(expose_message(wal->stop_workers()));
//}

} // <anonymous>
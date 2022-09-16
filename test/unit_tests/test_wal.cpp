#include <array>
#include <gtest/gtest.h>
#include "calico/bytes.h"
#include "calico/options.h"
#include "calico/storage.h"
#include "fakes.h"
#include "tools.h"
#include "unit_tests.h"
#include "utils/layout.h"
#include "utils/logging.h"
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
class TestWithWalSegments : public Base {
public:
    [[nodiscard]] static auto get_segment_name(SegmentId id) -> std::string
    {
        return Base::PREFIX + id.to_name();
    }

    [[nodiscard]] static auto get_segment_name(Size index) -> std::string
    {
        return Base::PREFIX + SegmentId::from_index(index).to_name();
    }

    template<class Id>
    [[nodiscard]] auto get_segment_size(const Id &id) const -> Size
    {
        Size size {};
        EXPECT_TRUE(expose_message(Base::store->file_size(get_segment_name(id), size)));
        return size;
    }

    template<class Id>
    [[nodiscard]] auto get_segment_data(const Id &id) const -> std::string
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
[[nodiscard]] static auto get_file_size(const Store &store, const std::string &path) -> Size
{
    Size size {};
    EXPECT_TRUE(expose_message(store.file_size(path, size)));
    return size;
}

class WalPayloadSizeLimitTests : public testing::TestWithParam<Size> {
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

    auto size = encode_deltas_payload(SequenceId {1}, PageId {2}, stob(image), deltas, stob(scratch));
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
        0x100 << 7));

class WalRecordMergeTests : public testing::Test {
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

class WalPayloadTests : public testing::Test {
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
    const auto size = encode_full_image_payload(SequenceId {1}, PageId::root(), stob(image), stob(scratch));
    const auto descriptor = decode_full_image_payload(stob(scratch).truncate(size));
    ASSERT_EQ(descriptor.pid, 1);
    ASSERT_EQ(descriptor.image.to_string(), image);
}

TEST_F(WalPayloadTests, EncodeAndDecodeDeltas)
{
    WalRecordGenerator generator;
    auto deltas = generator.setup_deltas(stob(image));
    const auto size = encode_deltas_payload(SequenceId {42}, PageId::root(), stob(image), deltas, stob(scratch));
    const auto descriptor = decode_deltas_payload(stob(scratch).truncate(size));
    ASSERT_EQ(descriptor.lsn, 42);
    ASSERT_EQ(descriptor.pid, 1);
    ASSERT_EQ(descriptor.deltas.size(), deltas.size());
    ASSERT_TRUE(std::all_of(cbegin(descriptor.deltas), cend(descriptor.deltas), [this](const DeltaContent &delta) {
        return delta.data == stob(image).range(delta.offset, delta.data.size());
    }));
}

[[nodiscard]] auto get_ids(const WalCollection &c)
{
    std::vector<SegmentId> ids;
    std::transform(cbegin(c.segments()), cend(c.segments()), back_inserter(ids), [](const auto &id) {
        return id;
    });
    return ids;
}

class WalCollectionTests : public testing::Test {
public:
    auto add_segments(Size n)
    {
        for (Size i {}; i < n; ++i) {
            auto id = SegmentId::from_index(i);
            collection.add_segment(id);
        }
        ASSERT_EQ(collection.last(), SegmentId::from_index(n - 1));
    }

    WalCollection collection;
};

TEST_F(WalCollectionTests, NewCollectionState)
{
    ASSERT_TRUE(collection.last().is_null());
}

TEST_F(WalCollectionTests, AddSegment)
{
    collection.add_segment(SegmentId {1});
    ASSERT_EQ(collection.last().value, 1);
}

TEST_F(WalCollectionTests, RecordsMostRecentSegmentId)
{
    add_segments(20);
    ASSERT_EQ(collection.last(), SegmentId::from_index(19));
}

template<class Itr>
[[nodiscard]] auto contains_n_consecutive_segments(const Itr &begin, const Itr &end, SegmentId id, Size n)
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
    // SegmentId::from_index(20) is one past the end.
    collection.remove_before(SegmentId::from_index(20));

    const auto ids = get_ids(collection);
    ASSERT_TRUE(ids.empty());
}

TEST_F(WalCollectionTests, RemovesAllSegmentsFromRight)
{
    add_segments(20);
    // SegmentId::null() is one before the beginning.
    collection.remove_after(SegmentId::null());

    const auto ids = get_ids(collection);
    ASSERT_TRUE(ids.empty());
}

TEST_F(WalCollectionTests, RemovesSomeSegmentsFromLeft)
{
    add_segments(20);
    collection.remove_before(SegmentId::from_index(10));

    const auto ids = get_ids(collection);
    ASSERT_TRUE(contains_n_consecutive_segments(cbegin(ids), cend(ids), SegmentId::from_index(10), 10));
}

TEST_F(WalCollectionTests, RemovesSomeSegmentsFromRight)
{
    add_segments(20);
    collection.remove_after(SegmentId::from_index(9));

    const auto ids = get_ids(collection);
    ASSERT_TRUE(contains_n_consecutive_segments(cbegin(ids), cend(ids), SegmentId::from_index(0), 10));
}

class LogReaderWriterTests : public TestWithWalSegmentsOnHeap {
public:
    static constexpr Size PAGE_SIZE {0x100};

    LogReaderWriterTests()
        : reader_payload(wal_scratch_size(PAGE_SIZE), '\x00'),
          reader_tail(wal_block_size(PAGE_SIZE), '\x00'),
          writer_tail(wal_block_size(PAGE_SIZE), '\x00')
    {}

    auto get_reader(SegmentId id) -> LogReader
    {
        const auto path = get_segment_name(id);
        EXPECT_TRUE(expose_message(store->open_random_reader(path, &reader_file)));
        return LogReader {*reader_file};
    }

    auto get_writer(SegmentId id) -> LogWriter
    {
        const auto path = get_segment_name(id);
        EXPECT_TRUE(expose_message(store->open_append_writer(path, &writer_file)));
        return LogWriter {*writer_file, stob(writer_tail), flushed_lsn};
    }

    auto write_string(LogWriter &writer, const std::string &payload) -> void
    {
        ASSERT_TRUE(expose_message(writer.write(++last_lsn, BytesView {payload})));
    }

    auto read_string(LogReader &reader) -> std::string
    {
        Bytes out {reader_payload};
        EXPECT_TRUE(expose_message(reader.read(out, Bytes {reader_tail})));
        return out.to_string();
    }

    auto run_basic_test(const std::vector<std::string> &payloads) -> void
    {
        auto writer = get_writer(SegmentId {1});
        auto reader = get_reader(SegmentId {1});
        for (const auto &payload: payloads) {
            ASSERT_LE(payload.size(), wal_scratch_size(PAGE_SIZE));
            write_string(writer, payload);
        }
        ASSERT_TRUE(expose_message(writer.flush()));

        for (const auto &payload: payloads) {
            ASSERT_EQ(read_string(reader), payload);
        }
    }

    [[nodiscard]] auto get_small_payload() -> std::string
    {
        return random.get<std::string>('a', 'z', wal_scratch_size(PAGE_SIZE) / random.get(10UL, 20UL));
    }

    [[nodiscard]] auto get_large_payload() -> std::string
    {
        return random.get<std::string>('a', 'z', 2 * wal_scratch_size(PAGE_SIZE) / random.get(2UL, 4UL));
    }

    std::atomic<SequenceId> flushed_lsn {};
    std::string reader_payload;
    std::string reader_tail;
    std::string writer_tail;
    RandomReader *reader_file {};
    AppendWriter *writer_file {};
    SequenceId last_lsn;
    Random random {internal::random_seed};
};

TEST_F(LogReaderWriterTests, DoesNotFlushEmptyBlock)
{
    auto writer = get_writer(SegmentId {1});
    ASSERT_TRUE(writer.flush().is_logic_error());

    Size file_size {};
    ASSERT_TRUE(expose_message(store->file_size("test/wal-000001", file_size)));
    ASSERT_EQ(file_size, 0);
}

TEST_F(LogReaderWriterTests, WritesMultipleBlocks)
{
    auto writer = get_writer(SegmentId {1});
    write_string(writer, get_large_payload());
    ASSERT_TRUE(expose_message(writer.flush()));

    Size file_size {};
    ASSERT_TRUE(expose_message(store->file_size("test/wal-000001", file_size)));
    ASSERT_EQ(file_size % writer_tail.size(), 0);
    ASSERT_GT(file_size / writer_tail.size(), 0);
}

TEST_F(LogReaderWriterTests, SingleSmallPayload)
{
    run_basic_test({get_small_payload()});
}

TEST_F(LogReaderWriterTests, MultipleSmallPayloads)
{
    run_basic_test({
        get_small_payload(),
        get_small_payload(),
        get_small_payload(),
        get_small_payload(),
        get_small_payload(),
    });
}

TEST_F(LogReaderWriterTests, SingleLargePayload)
{
    run_basic_test({get_large_payload()});
}

TEST_F(LogReaderWriterTests, MultipleLargePayloads)
{
    run_basic_test({
        get_large_payload(),
        get_large_payload(),
        get_large_payload(),
        get_large_payload(),
        get_large_payload(),
    });
}

TEST_F(LogReaderWriterTests, MultipleMixedPayloads)
{
    run_basic_test({
        get_small_payload(),
        get_large_payload(),
        get_small_payload(),
        get_large_payload(),
        get_small_payload(),
    });
}

TEST_F(LogReaderWriterTests, SanityCheck)
{
    std::vector<std::string> payloads(1'000);
    std::generate(begin(payloads), end(payloads), [this] {
        return random.get(4) ? get_small_payload() : get_large_payload();
    });
    run_basic_test(payloads);
}

TEST_F(LogReaderWriterTests, HandlesEarlyFlushes)
{
    std::vector<std::string> payloads(1'000);
    std::generate(begin(payloads), end(payloads), [this] {
        return random.get(4) ? get_small_payload() : get_large_payload();
    });

    auto writer = get_writer(SegmentId {1});
    auto reader = get_reader(SegmentId {1});
    for (const auto &payload: payloads) {
        ASSERT_LE(payload.size(), wal_scratch_size(PAGE_SIZE));
        write_string(writer, payload);
        if (random.get(10) == 0) {
            auto s = writer.flush();
            ASSERT_TRUE(s.is_ok() or s.is_logic_error());
        }
    }
    ASSERT_TRUE(expose_message(writer.flush()));

    for (const auto &payload: payloads) {
        ASSERT_EQ(read_string(reader), payload);
    }
}

class WalWriterTests : public TestWithWalSegmentsOnHeap {
public:
    static constexpr Size PAGE_SIZE {0x100};
    static constexpr Size WAL_LIMIT {8};

    WalWriterTests()
        : scratch {wal_scratch_size(PAGE_SIZE)},
          tail(wal_block_size(PAGE_SIZE), '\x00')
    {
        writer.emplace(
            *store,
            collection,
            scratch,
            Bytes {tail},
            flushed_lsn,
            PREFIX,
            WAL_LIMIT);
    }

    ~WalWriterTests() override = default;

    WalCollection collection;
    LogScratchManager scratch;
    std::atomic<SequenceId> flushed_lsn {};
    std::optional<WalWriter> writer;
    std::string tail;
    Random random {internal::random_seed};
};

TEST_F(WalWriterTests, OpenAndDestroy)
{
    ASSERT_TRUE(expose_message(writer->open()));
    ASSERT_TRUE(expose_message(writer->status()));
    ASSERT_TRUE(expose_message(std::move(*writer).destroy()));
}

TEST_F(WalWriterTests, DoesNotLeaveEmptySegmentsAfterNormalClose)
{
    ASSERT_TRUE(expose_message(writer->open()));

    // After the writer closes a segment file, it will either add it to the set of segment files, or it
    // will delete it. Empty segments get deleted, while nonempty segments get added.
    writer->advance();
    writer->advance();
    writer->advance();

    // Blocks until the last segment is deleted.
    ASSERT_TRUE(expose_message(std::move(*writer).destroy()));
    ASSERT_TRUE(collection.segments().empty());

    std::vector<std::string> children;
    ASSERT_TRUE(expose_message(store->get_children(ROOT, children)));
    ASSERT_TRUE(children.empty());
}

template<class Test>
static auto test_write_until_failure(Test &test) -> void
{
    auto s = test.writer->open();
    if (!s.is_ok()) {
        assert_error_42(s);
        return;
    }

    while (test.writer->status().is_ok()) {
        auto payload = test.scratch.get();
        const auto size = test.random.get(1UL, payload->size());
        payload->truncate(size);
        test.writer->write(SequenceId {1}, payload);
    }

    // Blocks until the last segment is deleted.
    assert_error_42(std::move(*test.writer).destroy());
}

template<class Test>
static auto count_segments(Test &test) -> Size
{
    const auto expected = test.collection.segments().size();

    std::vector<std::string> children;
    EXPECT_TRUE(expose_message(test.store->get_children(Test::ROOT, children)));
    EXPECT_EQ(children.size(), expected);
    return expected;
}

TEST_F(WalWriterTests, DoesNotLeaveEmptySegmentsAfterOpenFailure)
{
    interceptors::set_open(FailAfter<0> {"test/wal-"});
    test_write_until_failure(*this);
    ASSERT_EQ(count_segments(*this), 0);
}

TEST_F(WalWriterTests, DoesNotLeaveEmptySegmentsAfterWriteFailure)
{
    interceptors::set_write(FailAfter<0> {"test/wal-"});
    test_write_until_failure(*this);
    ASSERT_EQ(count_segments(*this), 0);
}

TEST_F(WalWriterTests, LeavesSingleNonEmptySegmentAfterOpenFailure)
{
    interceptors::set_open(FailAfter<1> {"test/wal-"});
    test_write_until_failure(*this);
    ASSERT_EQ(count_segments(*this), 1);
}

TEST_F(WalWriterTests, LeavesSingleNonEmptySegmentAfterWriteFailure)
{
    interceptors::set_write(FailAfter<WAL_LIMIT / 2> {"test/wal-"});
    test_write_until_failure(*this);
    ASSERT_EQ(count_segments(*this), 1);
}

TEST_F(WalWriterTests, LeavesMultipleNonEmptySegmentsAfterOpenFailure)
{
    interceptors::set_open(FailAfter<10> {"test/wal-"});
    test_write_until_failure(*this);
    ASSERT_EQ(count_segments(*this), 10);
}

TEST_F(WalWriterTests, LeavesMultipleNonEmptySegmentsAfterWriteFailure)
{
    interceptors::set_write(FailAfter<WAL_LIMIT * 10> {"test/wal-"});
    test_write_until_failure(*this);
    ASSERT_GT(count_segments(*this), 2);
}

class WalReaderWriterTests : public TestWithWalSegmentsOnHeap {
public:
    static constexpr Size PAGE_COUNT {32};
    static constexpr Size PAGE_SIZE {0x100};
    static constexpr Size WAL_LIMIT {8};

    WalReaderWriterTests()
        : images(PAGE_COUNT),
          scratch {wal_scratch_size(PAGE_SIZE)},
          reader_data(wal_scratch_size(PAGE_SIZE), '\x00'),
          reader_tail(wal_block_size(PAGE_SIZE), '\x00'),
          writer_tail(wal_block_size(PAGE_SIZE), '\x00')
    {
        std::generate(begin(images), end(images), [this] {
            return random.get<std::string>('a', 'z', PAGE_SIZE);
        });
        committed = images;
        has_full_image.resize(images.size());
    }

    ~WalReaderWriterTests() override = default;

    [[nodiscard]]
    auto get_reader() -> WalReader
    {
        return WalReader {
            *store,
            collection,
            PREFIX,
            Bytes {reader_tail},
            Bytes {reader_data}};
    }

    [[nodiscard]]
    auto get_writer() -> WalWriter
    {
        return WalWriter {
            *store,
            collection,
            scratch,
            Bytes {writer_tail},
            flushed_lsn,
            PREFIX,
            WAL_LIMIT};
    }

    [[nodiscard]]
    auto get_image(SequenceId lsn, PageId id) -> NamedScratch
    {
        EXPECT_LT(id.as_index(), PAGE_COUNT);
        auto payload = scratch.get();
        const auto size = encode_full_image_payload(lsn, id, Bytes {images[id.as_index()]}, *payload);
        payload->truncate(size);
        return payload;
    }

    [[nodiscard]]
    auto get_deltas(SequenceId lsn, PageId id) -> NamedScratch
    {
        EXPECT_LT(id.as_index(), PAGE_COUNT);
        auto deltas = generator.setup_deltas(Bytes {images[id.as_index()]});
        auto payload = scratch.get();
        const auto size = encode_deltas_payload(lsn, id, images[id.as_index()], deltas, *payload);
        payload->truncate(size);
        return payload;
    }

    [[nodiscard]]
    auto get_commit(SequenceId lsn) -> NamedScratch
    {
        auto payload = scratch.get();
        const auto size = encode_commit_payload(lsn, *payload);
        payload->truncate(size);
        return payload;
    }

    [[nodiscard]]
    auto emit_segments(Size num_writes, Size commit_interval = 0)
    {
        auto writer = get_writer();
        auto s = writer.open();
        if (!s.is_ok()) return s;

        SequenceId lsn;
        for (Size i {}; i < num_writes && writer.status().is_ok(); ++i) {
            const auto n = random.get(PAGE_COUNT - 1UL);
            const auto id = PageId::from_index(n);
            if (has_full_image[n]) {
                ++lsn;
                writer.write(lsn, get_deltas(lsn, id));
                if(i>3000)fmt::print(stderr,"OUT dl LSN: {}\n", lsn.value);
            } else {
                ++lsn;
                writer.write(lsn, get_image(lsn, id));
                if(i>3000)fmt::print(stderr,"OUT im LSN: {}\n", lsn.value);
                has_full_image[n] = true;
            }
            // Simulate a commit. We've been modifying the images when generating delta records, so we'll just save our
            // state at this point.
            if (commit_interval && !lsn.is_null() && lsn.as_index() % commit_interval == 0) {
                committed = images;
                commit_lsn = ++lsn;
                std::fill(begin(has_full_image), end(has_full_image), false);
                writer.write(lsn, get_commit(lsn));
                writer.advance();
                if(i>3000)fmt::print(stderr,"OUT cm SID: {}, LSN: {}\n", collection.last().value, lsn.value);

            }
        }
        return std::move(writer).destroy();
    }

    auto assert_images_match(const std::vector<std::string> &lhs, const std::vector<std::string> &rhs) const -> void
    {
        auto itr = cbegin(lhs);
        for (const auto &image: rhs) {
            CALICO_EXPECT_NE(itr, cend(lhs));
            ASSERT_EQ(image, *itr++);
        }
    }

    [[nodiscard]]
    auto contains_sequence(WalReader &reader, SequenceId last_lsn) -> Status
    {
        auto s = Status::ok();
        SequenceId lsn;
        // Roll forward to the end of the WAL.
        while (s.is_ok()) {
            s = reader.roll([&](const PayloadDescriptor &info) {
                SequenceId next_lsn;
                if (std::holds_alternative<DeltasDescriptor>(info)) {
                    const auto deltas = std::get<DeltasDescriptor>(info);
                    next_lsn = deltas.lsn;
                } else if (std::holds_alternative<FullImageDescriptor>(info)) {
                    const auto image = std::get<FullImageDescriptor>(info);
                    next_lsn = image.lsn;
                } else if (std::holds_alternative<CommitDescriptor>(info)) {
                    const auto image = std::get<CommitDescriptor>(info);
                    next_lsn = image.lsn;
                }
                EXPECT_EQ(++lsn, next_lsn);
                return Status::ok();
            });
            if (!s.is_ok()) break;
            s = reader.seek_next();
            if (s.is_not_found()) {
                EXPECT_EQ(lsn, last_lsn);
                return Status::ok();
            } else if (!s.is_ok()) {
                break;
            }
        }
        return s;
    }

    [[nodiscard]]
    auto roll_segments_forward(WalReader &reader, std::vector<std::string> &snapshots) -> Status
    {
        auto s = Status::ok();
        // Roll forward to the end of the WAL.
        while (s.is_ok()) {
            s = reader.roll([&](const PayloadDescriptor &info) {
                if (std::holds_alternative<DeltasDescriptor>(info)) {
                    const auto deltas = std::get<DeltasDescriptor>(info);
                    for (const auto &delta: deltas.deltas)
                        mem_copy(Bytes {snapshots[deltas.pid.as_index()]}.range(delta.offset), delta.data);
                } else if (std::holds_alternative<FullImageDescriptor>(info)) {
                    const auto image = std::get<FullImageDescriptor>(info);
                    // We shouldn't have encountered this page yet.
                    EXPECT_EQ(image.image.to_string(), snapshots[image.pid.as_index()]);
                }
                return Status::ok();
            });
            if (!s.is_ok()) break;
            s = reader.seek_next();
            if (s.is_not_found()) {
                return Status::ok();
            } else if (!s.is_ok()) {
                break;
            }
        }
        return s;
    }

    [[nodiscard]]
    auto roll_segments_backward(WalReader &reader, std::vector<std::string> &snapshots) -> Status
    {
        auto s = Status::ok();
        // Roll back to the most-recent commit.
        for (Size i {}; s.is_ok(); ++i) {

            SequenceId first_lsn;
            s = reader.read_first_lsn(first_lsn);
            if (!s.is_ok()) return s;

            if (first_lsn < commit_lsn)
                break;

            s = reader.roll([&](const PayloadDescriptor &info) {
                if (std::holds_alternative<CommitDescriptor>(info)) {
                    CALICO_EXPECT_TRUE(false);
                    return Status::not_found("should not have hit a commit record");
                } else if (std::holds_alternative<FullImageDescriptor>(info)) {
                    const auto image = std::get<FullImageDescriptor>(info);
                    mem_copy(snapshots[image.pid.as_index()], image.image);
                    fmt::print(stderr,"im SID:{}, LSN: {}\n", reader.segment_id().value, image.lsn.value);
                } else if (std::holds_alternative<DeltasDescriptor>(info)) {
                    const auto deltas = std::get<DeltasDescriptor>(info);
                    fmt::print(stderr,"dl SID:{}, LSN: {}\n", reader.segment_id().value, deltas.lsn.value);
                }
                return Status::ok();
            });
            if (!s.is_ok()) {
                if (!s.is_corruption() || i)
                    break;
            }
            s = reader.seek_previous();
            if (s.is_not_found()) {
                return Status::ok();
            } else if (!s.is_ok()) {
                break;
            }
        }
        return s;
    }

    SequenceId commit_lsn;
    std::vector<std::string> committed;
    std::vector<std::string> images;
    std::vector<int> has_full_image;
    WalRecordGenerator generator;
    WalCollection collection;
    LogScratchManager scratch;
    std::atomic<SequenceId> flushed_lsn {};
    std::string reader_data;
    std::string reader_tail;
    std::string writer_tail;
    Random random {internal::random_seed};
};

static auto does_not_lose_records_test(WalReaderWriterTests &test, Size num_writes)
{
    ASSERT_TRUE(expose_message(test.emit_segments(num_writes)));

    auto reader = test.get_reader();
    ASSERT_TRUE(expose_message(reader.open()));
    ASSERT_TRUE(expose_message(test.contains_sequence(reader, SequenceId {num_writes})));
}

TEST_F(WalReaderWriterTests, DoesNotLoseRecordWithinSegment)
{
    does_not_lose_records_test(*this, 3);
}

TEST_F(WalReaderWriterTests, DoesNotLoseRecordsAcrossSegments)
{
    does_not_lose_records_test(*this, 5'000);
}

static auto roll_forward_test(WalReaderWriterTests &test, Size num_writes)
{
    auto snapshots = test.images;
    ASSERT_TRUE(expose_message(test.emit_segments(num_writes)));

    auto reader = test.get_reader();
    ASSERT_TRUE(expose_message(reader.open()));
    ASSERT_TRUE(expose_message(test.roll_segments_forward(reader, snapshots)));
    test.assert_images_match(snapshots, test.images);
}

TEST_F(WalReaderWriterTests, RollForwardWithinSegment)
{
    roll_forward_test(*this, 3);
}

TEST_F(WalReaderWriterTests, RollForwardAcrossSegments)
{
    roll_forward_test(*this, 5'000);
}

static auto roll_backward_test(WalReaderWriterTests &test, Size num_writes)
{
    auto snapshots = test.images;
    ASSERT_TRUE(expose_message(test.emit_segments(num_writes)));

    auto reader = test.get_reader();
    ASSERT_TRUE(expose_message(reader.open()));
    ASSERT_TRUE(expose_message(test.roll_segments_forward(reader, snapshots)));
    test.assert_images_match(snapshots, test.images);
    ASSERT_TRUE(expose_message(test.roll_segments_backward(reader, snapshots)));
    test.assert_images_match(snapshots, test.committed);
}

TEST_F(WalReaderWriterTests, RollBackwardWithinSegment)
{
    roll_backward_test(*this, 3);
}

TEST_F(WalReaderWriterTests, RollBackwardAcrossSegments)
{
    roll_backward_test(*this, 5'000);
}

TEST_F(WalReaderWriterTests, RunsTransactionsNormally)
{
    auto snapshots = images;
    ASSERT_TRUE(expose_message(emit_segments(5000, 100)));

    auto reader = get_reader();
    ASSERT_TRUE(expose_message(reader.open()));
    ASSERT_TRUE(expose_message(roll_segments_forward(reader, snapshots)));
    assert_images_match(snapshots, images);
    ASSERT_TRUE(expose_message(roll_segments_backward(reader, snapshots)));
    assert_images_match(snapshots, committed);
}

TEST_F(WalReaderWriterTests, CommitIsCheckpoint)
{
    auto snapshots = images;

    // Should commit after the last write.
    ASSERT_TRUE(expose_message(emit_segments(200, 99)));

    auto reader = get_reader();
    ASSERT_TRUE(expose_message(reader.open()));
    ASSERT_TRUE(expose_message(roll_segments_forward(reader, snapshots)));
    assert_images_match(snapshots, images);
    ASSERT_TRUE(expose_message(roll_segments_backward(reader, snapshots)));
    assert_images_match(snapshots, images);
    assert_images_match(images, committed);
}

TEST_F(WalReaderWriterTests, RollWalAfterWriteError)
{
    interceptors::set_write(FailOnce<10> {"test/wal-"});

    auto snapshots = images;
    assert_error_42(emit_segments(5'000));

    auto reader = get_reader();
    ASSERT_TRUE(expose_message(reader.open()));
    auto s = roll_segments_forward(reader, snapshots);

    // The writer may have failed in the middle of writing a record (FIRST is written but LAST is in the tail buffer)
    // still. In this case, we'll get a corruption error during the forward pass.
    ASSERT_TRUE(s.is_corruption() or s.is_ok());

    // We should be able to roll back any changes we have made to the snapshots.
    ASSERT_TRUE(expose_message(roll_segments_backward(reader, snapshots)));
}

TEST_F(WalReaderWriterTests, RollWalAfterOpenError)
{
    interceptors::set_open(FailOnce<3> {"test/wal-"});

    auto snapshots = images;
    assert_error_42(emit_segments(5'000));

    auto reader = get_reader();
    ASSERT_TRUE(expose_message(reader.open()));
    auto s = roll_segments_forward(reader, snapshots);

    // The writer may have failed in the middle of writing a record (FIRST is written but LAST is in the tail buffer)
    // still. In this case, we'll get a corruption error during the forward pass.
    ASSERT_TRUE(s.is_corruption() or s.is_ok());

    // We should be able to roll back any changes we have made to the snapshots.
    ASSERT_TRUE(expose_message(roll_segments_backward(reader, snapshots)));
}

//
//class BasicWalReaderWriterTests: public TestWithWalSegmentsOnHeap {
//public:
//    static constexpr Size PAGE_SIZE {0x100};
//    static constexpr Size BLOCK_SIZE {PAGE_SIZE * WAL_BLOCK_SCALE};
//
//    BasicWalReaderWriterTests()
//        : scratch {std::make_unique<LogScratchManager>(PAGE_SIZE * WAL_SCRATCH_SCALE)}
//    {}
//
//    auto SetUp() -> void override
//    {
//        reader = std::make_unique<BasicWalReader>(
//            *store,
//            ROOT,
//            PAGE_SIZE
//        );
//
//        writer = std::make_unique<BasicWalWriter>(BasicWalWriter::Parameters {
//            store.get(),
//            &collection,
//            &flushed_lsn,
//            create_logger(create_sink(), "wal"),
//            ROOT,
//            PAGE_SIZE,
//            128,
//        });
//    }
//
//    WalCollection collection;
//    std::atomic<SequenceId> flushed_lsn {};
//    std::unique_ptr<LogScratchManager> scratch;
//    std::unique_ptr<BasicWalReader> reader;
//    std::unique_ptr<BasicWalWriter> writer;
//    Random random {internal::random_seed};
//};
//
//TEST_F(BasicWalReaderWriterTests, NewWriterIsOk)
//{
//    ASSERT_TRUE(writer->status().is_ok());
//    writer->stop();
//}

template<class Test>
auto generate_images(Test &test, Size page_size, Size n)
{
    std::vector<std::string> images;
    std::generate_n(back_inserter(images), n, [&test, page_size] {
        return test.random.template get<std::string>('\x00', '\xFF', page_size);
    });
    return images;
}
//
//auto generate_deltas(std::vector<std::string> &images)
//{
//    WalRecordGenerator generator;
//    std::vector<std::vector<PageDelta>> deltas;
//    for (auto &image: images)
//        deltas.emplace_back(generator.setup_deltas(stob(image)));
//    return deltas;
//}
//
//TEST_F(BasicWalReaderWriterTests, WritesAndReadsDeltasNormally)
//{
//    // NOTE: This test doesn't handle segmentation. If the writer segments, the test will fail!
//    static constexpr Size NUM_RECORDS {100};
//    auto images = generate_images(*this, PAGE_SIZE, NUM_RECORDS);
//    auto deltas = generate_deltas(images);
//    for (Size i {}; i < NUM_RECORDS; ++i)
//        writer->log_deltas(PageId::root(), stob(images[i]), deltas[i]);
//
//    // close() should cause the writer to flush the current block.
//    writer->stop();
//
//    ASSERT_TRUE(expose_message(reader->open(SegmentId {1})));
//
//    Size i {};
//    ASSERT_TRUE(expose_message(reader->redo([deltas, &i, images](const RedoDescriptor &descriptor) {
//        auto lhs = cbegin(descriptor.deltas);
//        auto rhs = cbegin(deltas[i]);
//        for (; rhs != cend(deltas[i]); ++lhs, ++rhs) {
//            EXPECT_NE(lhs, cend(descriptor.deltas));
//            EXPECT_TRUE(lhs->data == stob(images[i]).range(rhs->offset, rhs->size));
//            EXPECT_EQ(lhs->offset, rhs->offset);
//        }
//        i++;
//        return Status::ok();
//    })));
//    ASSERT_EQ(i, images.size());
//    ASSERT_EQ(get_segment_size(0UL) % BLOCK_SIZE, 0);
//}
//
//TEST_F(BasicWalReaderWriterTests, WritesAndReadsFullImagesNormally)
//{
//    // NOTE: This test doesn't handle segmentation. If the writer segments, the test will fail!
//    static constexpr Size NUM_RECORDS {100};
//    auto images = generate_images(*this, PAGE_SIZE, NUM_RECORDS);
//
//    for (Size i {}; i < NUM_RECORDS; ++i)
//        writer->log_full_image(PageId::from_index(i), stob(images[i]));
//    writer->stop();
//
//    ASSERT_TRUE(expose_message(reader->open(SegmentId {1})));
//    ASSERT_TRUE(expose_message(reader->redo([](const auto&) {
//        ADD_FAILURE() << "This should not be called";
//        return Status::logic_error("Logic error!");
//    })));
//
//    Size i {};
//    ASSERT_TRUE(expose_message(reader->undo([&i, images](const UndoDescriptor &descriptor) {
//        EXPECT_EQ(descriptor.page_id, i + 1);
//        EXPECT_TRUE(descriptor.image == stob(images[i]));
//        i++;
//        return Status::ok();
//    })));
//    ASSERT_EQ(i, images.size());
//    ASSERT_EQ(get_segment_size(0UL) % BLOCK_SIZE, 0);
//}
//
//auto test_undo_redo(BasicWalReaderWriterTests &test, Size num_images, Size num_deltas)
//{
//    const auto deltas_per_image = num_deltas / num_images;
//
//    std::vector<std::string> before_images;
//    std::vector<std::string> after_images;
//    WalRecordGenerator generator;
//
//    auto &reader = test.reader;
//    auto &writer = test.writer;
//    auto &random = test.random;
//    auto &collection = test.collection;
//
//    for (Size i {}; i < num_images; ++i) {
//        auto pid = PageId::from_index(i);
//
//        before_images.emplace_back(random.get<std::string>('\x00', '\xFF', BasicWalReaderWriterTests::PAGE_SIZE));
//        writer->log_full_image(pid, stob(before_images.back()));
//
//        after_images.emplace_back(before_images.back());
//        for (Size j {}; j < deltas_per_image; ++j) {
//            const auto deltas = generator.setup_deltas(stob(after_images.back()));
//            writer->log_deltas(pid, stob(after_images.back()), deltas);
//        }
//    }
//    writer->stop();
//
//    // Roll forward some copies of the "before images" to match the "after images".
//    auto images = before_images;
//    for (const auto &[id, meta]: collection.segments()) {
//        ASSERT_TRUE(expose_message(reader->open(id)));
//        ASSERT_TRUE(expose_message(reader->redo([&images](const RedoDescriptor &info) {
//            auto image = stob(images.at(info.page_id - 1));
//            for (const auto &[offset, content]: info.deltas)
//                mem_copy(image.range(offset, content.size()), content);
//            return Status::ok();
//        })));
//        ASSERT_TRUE(expose_message(reader->close()));
//    }
//
//    // Image copies should match the "after images".
//    for (Size i {}; i < images.size(); ++i) {
//        ASSERT_EQ(images.at(i), after_images.at(i));
//    }
//
//    // Now roll them back to match the before images again.
//    for (auto itr = crbegin(collection.segments()); itr != crend(collection.segments()); ++itr) {
//        // Segment ID should be the same for each record position within each group.
//        ASSERT_TRUE(expose_message(reader->open(itr->id)));
//        ASSERT_TRUE(expose_message(reader->undo([&images](const auto &info) {
//            const auto index = info.page_id - 1;
//            mem_copy(stob(images[index]), info.image);
//            return Status::ok();
//        })));
//        ASSERT_TRUE(expose_message(reader->close()));
//    }
//
//    for (Size i {}; i < images.size(); ++i) {
//        ASSERT_EQ(images.at(i), before_images.at(i));
//    }
//}
//
//TEST_F(BasicWalReaderWriterTests, SingleImage)
//{
//    // This situation should not happen in practice, but we technically should be able to handle it.
//    test_undo_redo(*this, 1, 0);
//}
//
//TEST_F(BasicWalReaderWriterTests, SingleImageSingleDelta)
//{
//    test_undo_redo(*this, 1, 1);
//}
//
//TEST_F(BasicWalReaderWriterTests, SingleImageManyDeltas)
//{
//    test_undo_redo(*this, 1, 100);
//}
//
//TEST_F(BasicWalReaderWriterTests, ManyImagesManyDeltas)
//{
//    test_undo_redo(*this, 100, 1'000);
//}
//
////TEST_F(BasicWalReaderWriterTests, ManyManyImagesManyManyDeltas)
////{
////    test_undo_redo(*this, 10'000, 1'000'000);
////}

class BasicWalTests: public TestWithWalSegmentsOnHeap {
public:
    static constexpr Size PAGE_SIZE {0x100};

    ~BasicWalTests() override = default;

    auto SetUp() -> void override
    {
        WriteAheadLog *temp {};

        ASSERT_TRUE(expose_message(BasicWriteAheadLog::open({
            PREFIX,
            store.get(),
            create_sink(),
            PAGE_SIZE,
        }, &temp)));

        wal.reset(temp);

        ASSERT_TRUE(expose_message(wal->start_recovery([](const auto &) { return Status::logic_error(""); },
                                                       [](const auto &) { return Status::logic_error(""); })));
    }

    Random random {42};
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

TEST_F(BasicWalTests, FailureDuringFirstOpen)
{
    interceptors::set_open(FailOnce<0> {"test/wal-"});
    assert_error_42(wal->start_workers());
}

TEST_F(BasicWalTests, FailureDuringNthOpen)
{
    auto images = generate_images(*this, PAGE_SIZE, 1'000);
    interceptors::set_open(FailEvery<5> {"test/wal-"});
    ASSERT_TRUE(expose_message(wal->start_workers()));

    Size num_writes {};
    for (Size i {}; i < images.size(); ++i) {
        auto s = wal->log(i, stob(images[i]));
        if (!s.is_ok()) {
            assert_error_42(s);
            break;
        }
        num_writes++;
    }
    ASSERT_GT(num_writes, 5);
    assert_error_42(wal->stop_workers());
}

} // <anonymous>
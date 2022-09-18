#include "calico/bytes.h"
#include "calico/options.h"
#include "calico/storage.h"
#include "core/transaction_log.h"
#include "fakes.h"
#include "tools.h"
#include "unit_tests.h"
#include "utils/info_log.h"
#include "utils/layout.h"
#include "wal/basic_wal.h"
#include "wal/helpers.h"
#include "wal/reader.h"
#include "wal/writer.h"
#include <array>
#include <gtest/gtest.h>

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

    auto size = encode_deltas_payload(PageId {2}, stob(image), deltas, stob(scratch));
    ASSERT_GE(size + WalPayloadHeader::SIZE, min_size) << "Excessive scratch memory allocated";
    ASSERT_LE(size + WalPayloadHeader::SIZE, max_size) << "Scratch memory cannot fit maximally sized WAL record payload";
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
    const auto size = encode_full_image_payload(PageId::root(), stob(image), stob(scratch));
    const auto payload = decode_payload(stob(scratch).truncate(size));
    ASSERT_TRUE(std::holds_alternative<FullImageDescriptor>(payload.value()));
    const auto descriptor = std::get<FullImageDescriptor>(*payload);
    ASSERT_EQ(descriptor.pid, 1);
    ASSERT_EQ(descriptor.image.to_string(), image);
}

TEST_F(WalPayloadTests, EncodeAndDecodeDeltas)
{
    WalRecordGenerator generator;
    auto deltas = generator.setup_deltas(stob(image));
    const auto size = encode_deltas_payload(PageId::root(), stob(image), deltas, stob(scratch));
    const auto payload = decode_payload(stob(scratch).truncate(size));
    ASSERT_TRUE(std::holds_alternative<DeltasDescriptor>(payload.value()));
    const auto descriptor = std::get<DeltasDescriptor>(*payload);
    ASSERT_EQ(descriptor.pid, 1);
    ASSERT_EQ(descriptor.deltas.size(), deltas.size());
    ASSERT_TRUE(std::all_of(cbegin(descriptor.deltas), cend(descriptor.deltas), [this](const auto &delta) {
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
          writer_tail(wal_block_size(PAGE_SIZE), '\x00'),
          scratch {wal_scratch_size(PAGE_SIZE)}
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
        auto buffer = scratch.get();
        ASSERT_GE(buffer->size(), payload.size() + sizeof(SequenceId));
        WalPayloadIn in {++last_lsn, buffer};
        mem_copy(in.data(), payload);
        in.shrink_to_fit(payload.size());
        ASSERT_OK(writer.write(in));
    }

    auto read_string(LogReader &reader) -> std::string
    {
        WalPayloadOut payload;
        EXPECT_TRUE(expose_message(reader.read(payload, Bytes {reader_payload}, Bytes {reader_tail})));
        return payload.data().to_string();
    }

    auto run_basic_test(const std::vector<std::string> &payloads) -> void
    {
        auto writer = get_writer(SegmentId {1});
        auto reader = get_reader(SegmentId {1});
        for (const auto &payload: payloads) {
            ASSERT_LE(payload.size(), wal_scratch_size(PAGE_SIZE) - sizeof(SequenceId));
            write_string(writer, payload);
        }
        ASSERT_OK(writer.flush());

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
        return random.get<std::string>('a', 'z', 2 * wal_scratch_size(PAGE_SIZE) / random.get(3UL, 4UL));
    }

    std::atomic<SequenceId> flushed_lsn {};
    std::string reader_payload;
    std::string reader_tail;
    std::string writer_tail;
    LogScratchManager scratch;
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
    ASSERT_OK(store->file_size("test/wal-000001", file_size));
    ASSERT_EQ(file_size, 0);
}

TEST_F(LogReaderWriterTests, WritesMultipleBlocks)
{
    auto writer = get_writer(SegmentId {1});
    write_string(writer, get_large_payload());
    ASSERT_OK(writer.flush());

    Size file_size {};
    ASSERT_OK(store->file_size("test/wal-000001", file_size));
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
    ASSERT_OK(writer.flush());

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
    ASSERT_OK(writer->open());
    ASSERT_OK(writer->status());
    ASSERT_OK(std::move(*writer).destroy());
}

TEST_F(WalWriterTests, DoesNotLeaveEmptySegmentsAfterNormalClose)
{
    ASSERT_OK(writer->open());

    // After the writer closes a segment file, it will either add it to the set of segment files, or it
    // will delete it. Empty segments get deleted, while nonempty segments get added.
    writer->advance();
    writer->advance();
    writer->advance();

    // Blocks until the last segment is deleted.
    ASSERT_OK(std::move(*writer).destroy());
    ASSERT_TRUE(collection.segments().empty());

    std::vector<std::string> children;
    ASSERT_OK(store->get_children(ROOT, children));
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

    SequenceId last_lsn;
    while (test.writer->status().is_ok()) {
        auto buffer = test.scratch.get();
        WalPayloadIn payload {++last_lsn, buffer};
        const auto size = test.random.get(1UL, payload.data().size());
        payload.shrink_to_fit(size);
        test.writer->write(payload);
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
    static constexpr Size PAGE_SIZE {0x100};
    static constexpr Size WAL_LIMIT {8};

    WalReaderWriterTests()
        : scratch {wal_scratch_size(PAGE_SIZE)},
          reader_data(wal_scratch_size(PAGE_SIZE), '\x00'),
          reader_tail(wal_block_size(PAGE_SIZE), '\x00'),
          writer_tail(wal_block_size(PAGE_SIZE), '\x00')
    {}

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
    auto get_payload() -> WalPayloadIn
    {
        WalPayloadIn payload {++last_lsn, scratch.get()};
        const auto size = random.get(payload.data().size());
        payload.shrink_to_fit(size);
        payloads.emplace_back(random.get<std::string>('a', 'z', size));
        mem_copy(payload.data(), payloads.back());
        return payload;
    }

    [[nodiscard]]
    auto emit_segments(Size num_writes, Size segment_interval = 0)
    {
        auto writer = get_writer();
        auto s = writer.open();
        if (!s.is_ok()) return s;

        for (Size i {}; i < num_writes && writer.status().is_ok(); ++i) {
            writer.write(get_payload());
            if (segment_interval && i && i % segment_interval == 0)
                writer.advance();
        }
        return std::move(writer).destroy();
    }

    [[nodiscard]]
    auto contains_sequence(WalReader &reader, SequenceId final_lsn) -> Status
    {
        auto s = Status::ok();
        SequenceId lsn;
        // Roll forward to the end of the WAL.
        while (s.is_ok()) {
            s = reader.roll([&](auto info) {
                EXPECT_EQ(++lsn, info.lsn());
                return Status::ok();
            });
            if (!s.is_ok()) break;
            s = reader.seek_next();
            if (s.is_not_found()) {
                EXPECT_EQ(lsn, final_lsn);
                return Status::ok();
            } else if (!s.is_ok()) {
                break;
            }
        }
        return s;
    }

    [[nodiscard]]
    auto roll_segments_forward(WalReader &reader) -> Status
    {
        auto s = Status::ok();
        // Roll forward to the end of the WAL.
        while (s.is_ok()) {
            s = reader.roll([&](auto info) {
                EXPECT_EQ(info.data().to_string(), payloads[info.lsn().as_index()]);
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
    auto roll_segments_backward(WalReader &reader) -> Status
    {
        auto s = Status::ok();
        for (Size i {}; s.is_ok(); ++i) {

            SequenceId first_lsn;
            s = reader.read_first_lsn(first_lsn);
            if (!s.is_ok()) return s;

            s = reader.roll([&](auto info) {
                EXPECT_EQ(info.data().to_string(), payloads[info.lsn().as_index()]);
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

    SequenceId last_lsn;
    std::vector<std::string> payloads;
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
    ASSERT_OK(test.emit_segments(num_writes));

    auto reader = test.get_reader();
    ASSERT_OK(reader.open());
    ASSERT_OK(test.contains_sequence(reader, SequenceId {num_writes}));
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
    ASSERT_OK(test.emit_segments(num_writes));

    auto reader = test.get_reader();
    ASSERT_OK(reader.open());
    ASSERT_OK(test.roll_segments_forward(reader));
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
    ASSERT_OK(test.emit_segments(num_writes));

    auto reader = test.get_reader();
    ASSERT_OK(reader.open());
    ASSERT_OK(test.roll_segments_forward(reader));
    ASSERT_OK(test.roll_segments_backward(reader));
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
    ASSERT_OK(emit_segments(5000, 100));

    auto reader = get_reader();
    ASSERT_OK(reader.open());
    ASSERT_OK(roll_segments_forward(reader));
    ASSERT_OK(roll_segments_backward(reader));
}

TEST_F(WalReaderWriterTests, CommitIsCheckpoint)
{
    // Should commit after the last write.
    ASSERT_OK(emit_segments(200, 99));

    auto reader = get_reader();
    ASSERT_OK(reader.open());
    ASSERT_OK(roll_segments_forward(reader));
    ASSERT_OK(roll_segments_backward(reader));
}

TEST_F(WalReaderWriterTests, RollWalAfterWriteError)
{
    interceptors::set_write(FailOnce<10> {"test/wal-"});

    assert_error_42(emit_segments(5'000));

    auto reader = get_reader();
    ASSERT_OK(reader.open());
    auto s = roll_segments_forward(reader);
    ASSERT_TRUE(s.is_corruption() or s.is_ok());
    ASSERT_OK(roll_segments_backward(reader));
}

TEST_F(WalReaderWriterTests, RollWalAfterOpenError)
{
    interceptors::set_open(FailOnce<3> {"test/wal-"});

    assert_error_42(emit_segments(5'000));

    auto reader = get_reader();
    ASSERT_OK(reader.open());
    auto s = roll_segments_forward(reader);
    ASSERT_TRUE(s.is_corruption() or s.is_ok());
    ASSERT_OK(roll_segments_backward(reader));
}

class WalCleanerTests : public WalReaderWriterTests {
public:
    WalCleanerTests()
    {
        cleaner.emplace(
            *store,
            PREFIX,
            collection);
    }

    ~WalCleanerTests() override = default;

    std::optional<WalCleaner> cleaner;
};

TEST_F(WalCleanerTests, RemoveBeforeNullIdDoesNothing)
{
    cleaner->remove_before(SequenceId::null(), true);
    ASSERT_OK(std::move(*cleaner).destroy());
}

TEST_F(WalCleanerTests, DoesNotRemoveOnlySegment)
{
    auto writer = get_writer();
    ASSERT_OK(writer.open());
    writer.write(get_payload());
    writer.write(get_payload());
    writer.write(get_payload());
    ASSERT_OK(std::move(writer).destroy());
    ASSERT_EQ(collection.segments().size(), 1);

    cleaner->remove_before(last_lsn, true);
    ASSERT_OK(std::move(*cleaner).destroy());
    ASSERT_EQ(collection.segments().size(), 1);
}

TEST_F(WalCleanerTests, KeepsAtLeastMostRecentSegment)
{
    auto writer = get_writer();
    ASSERT_OK(writer.open());

    static constexpr Size NUM_ROUNDS {1'000};
    for (Size i {}; i < NUM_ROUNDS; ++i) {
        writer.write(get_payload());
        cleaner->remove_before(last_lsn);
    }
    ASSERT_OK(std::move(writer).destroy());
    ASSERT_OK(std::move(*cleaner).destroy());
    ASSERT_GE(collection.segments().size(), 1);
}

class BasicWalTests: public TestWithWalSegmentsOnHeap {
public:
    static constexpr Size PAGE_SIZE {0x100};

    ~BasicWalTests() override = default;

    BasicWalTests()
        : scratch {wal_scratch_size(PAGE_SIZE)}
    {}

    auto SetUp() -> void override
    {
        WriteAheadLog *temp {};

        ASSERT_TRUE(expose_message(BasicWriteAheadLog::open({
            PREFIX,
            store.get(),
            &scratch,
            create_sink(),
            PAGE_SIZE,
        }, &temp)));

        wal.reset(temp);
    }

    [[nodiscard]]
    auto get_data_payload(const std::string &data) -> WalPayloadIn
    {
        WalPayloadIn payload {wal->current_lsn(), scratch.get()};
        payload.shrink_to_fit(1 + data.size());
        payloads.emplace_back("p" + data);
        mem_copy(payload.data(), payloads.back());
        payloads_since_commit++;
        return payload;
    }

    [[nodiscard]]
    auto get_random_data_payload() -> WalPayloadIn
    {
        const auto max_size = wal_scratch_size(PAGE_SIZE) - WalPayloadHeader::SIZE - 1;
        const auto size = random.get(1, max_size);
        return get_data_payload(random.get<std::string>('a', 'z', size));
    }

    [[nodiscard]]
    auto get_commit_payload() -> WalPayloadIn
    {
        commit_lsn = wal->current_lsn();
        payloads_since_commit = 0;
        WalPayloadIn payload {commit_lsn, scratch.get()};
        payloads.emplace_back("c");
        payload.data()[0] = 'c';
        payload.shrink_to_fit(1);
        return payload;
    }

    [[nodiscard]]
    auto emit_segments(Size num_writes, Size commit_interval = 0)
    {
        for (Size i {}; i < num_writes && wal->worker_status().is_ok(); ++i) {
            if (commit_interval && i && i % commit_interval == 0) {
                ASSERT_OK(wal->log(get_commit_payload()));
                ASSERT_OK(wal->advance());
            } else {
                const auto data = random.get<std::string>('a', 'z', 10);
                ASSERT_OK(wal->log(get_data_payload(data)));
            }
        }
    }

    auto log_string(const std::string &payload)
    {
        ASSERT_OK(wal->log(get_data_payload(payload)));
    }

    auto roll_forward(bool strict = true)
    {
        SequenceId lsn;
        ASSERT_TRUE(expose_message(wal->roll_forward(++lsn, [&lsn, this](auto payload) {
            EXPECT_EQ(lsn++, payload.lsn());
            EXPECT_EQ(payload.data().to_string(), payloads[payload.lsn().as_index()]);
            return Status::ok();
        })));
        // We should have hit all records.
        if (strict) {
            ASSERT_EQ(lsn, wal->current_lsn());
        }
    }

    auto roll_backward(bool strict = true)
    {
        std::vector<SequenceId> lsns;
        ASSERT_TRUE(expose_message(wal->roll_backward(commit_lsn, [&lsns, this](auto payload) {
            lsns.emplace_back(payload.lsn());
            EXPECT_GT(payload.lsn(), commit_lsn);
            EXPECT_EQ(payload.data().to_string(), payloads[payload.lsn().as_index()]);
            return Status::ok();
        })));
        if (strict) {
            ASSERT_EQ(lsns.size(), payloads_since_commit);
        }
        std::sort(begin(lsns), end(lsns));
        SequenceId lsn_counter {commit_lsn};
        for (const auto &lsn: lsns)
            ASSERT_EQ(++lsn_counter, lsn);
    }

    enum class WalOperation: int {
        FLUSH = 1,
        SEGMENT = 2,
        COMMIT = 3,
        LOG = 4,
    };

    auto run_operations(std::vector<WalOperation> operations)
    {
        auto s = wal->start_workers();
        if (!s.is_ok()) return s;

        for (auto operation: operations) {
            switch (operation) {
                case WalOperation::FLUSH:
                    (void)wal->flush();
                    break;
                case WalOperation::SEGMENT:
                    (void)wal->advance();
                    break;
                case WalOperation::COMMIT:
                    s = wal->log(get_commit_payload());
                    if (s.is_ok()) s = wal->advance();
                    break;
                case WalOperation::LOG:
                    s = wal->log(get_random_data_payload());
                    break;
            }
            if (!s.is_ok())
                break;
        }
        auto t = wal->stop_workers();
        return s.is_ok() ? t : s;
    }

    Random random {42};
    Size payloads_since_commit {};
    SequenceId commit_lsn;
    LogScratchManager scratch;
    std::vector<std::string> payloads;
    std::unique_ptr<WriteAheadLog> wal;
};

TEST_F(BasicWalTests, StartsAndStops)
{
    ASSERT_OK(wal->start_workers());
    ASSERT_OK(wal->stop_workers());
}

TEST_F(BasicWalTests, NewWalState)
{
    ASSERT_OK(wal->start_workers());
    ASSERT_EQ(wal->flushed_lsn(), 0);
    ASSERT_EQ(wal->current_lsn(), 1);
    ASSERT_OK(wal->stop_workers());
}

TEST_F(BasicWalTests, WriterDoesNotLeaveEmptySegments)
{
    std::vector<std::string> children;

    for (Size i {}; i < 10; ++i) {
        ASSERT_OK(wal->start_workers());

        // File should be deleted before this method returns, if no records were written to it.
        ASSERT_OK(wal->stop_workers());
        ASSERT_OK(store->get_children(ROOT, children));
        ASSERT_TRUE(children.empty());
    }
}

TEST_F(BasicWalTests, RollWhileEmpty)
{
    ASSERT_OK(wal->roll_forward(SequenceId::null(), [](auto) {return Status::ok();}));
}

TEST_F(BasicWalTests, FlushWithEmptyTailBuffer)
{
    ASSERT_OK(run_operations({WalOperation::FLUSH}));
}

TEST_F(BasicWalTests, SegmentWithEmptyTailBuffer)
{
    ASSERT_OK(run_operations({WalOperation::SEGMENT}));
}

TEST_F(BasicWalTests, RollSingleRecord)
{
    ASSERT_OK(run_operations({WalOperation::LOG}));

    roll_forward();
    roll_backward();
}

TEST_F(BasicWalTests, RollSingleRecordWithCommit)
{
    ASSERT_TRUE(expose_message(run_operations({
        WalOperation::LOG,
        WalOperation::COMMIT,
    })));

    roll_forward();
    roll_backward();
}

TEST_F(BasicWalTests, RollMultipleRecords)
{
    ASSERT_TRUE(expose_message(run_operations({
        WalOperation::LOG,
        WalOperation::LOG,
        WalOperation::LOG,
    })));

    roll_forward();
    roll_backward();
}

TEST_F(BasicWalTests, RollMultipleRecordsWithCommitAtEnd)
{
    ASSERT_TRUE(expose_message(run_operations({
        WalOperation::LOG,
        WalOperation::LOG,
        WalOperation::LOG,
        WalOperation::LOG,
        WalOperation::COMMIT,
    })));

    roll_forward();
    roll_backward();
}

TEST_F(BasicWalTests, RollMultipleRecordsWithCommitInMiddle)
{
    ASSERT_TRUE(expose_message(run_operations({
        WalOperation::LOG,
        WalOperation::LOG,
        WalOperation::COMMIT,
        WalOperation::LOG,
        WalOperation::LOG,
    })));

    roll_forward();
    roll_backward();
}

template<class Test>
static auto generate_transaction(Test &test, Size n, bool add_commit = false)
{
    std::vector<BasicWalTests::WalOperation> operations;
    operations.reserve(n);

    while (operations.size() < n) {
        const auto r = test.random.get(20);
        if (r >= 2 || operations.empty() || operations.back() != BasicWalTests::WalOperation::LOG) {
            operations.emplace_back(BasicWalTests::WalOperation::LOG);
        } else {
            if (r == 0) {
                operations.emplace_back(BasicWalTests::WalOperation::FLUSH);
            } else {
                operations.emplace_back(BasicWalTests::WalOperation::SEGMENT);
            }
        }
    }
    if (add_commit)
        operations.emplace_back(BasicWalTests::WalOperation::COMMIT);
    return operations;
}

TEST_F(BasicWalTests, SanityCheckSingleTransaction)
{
    ASSERT_OK(run_operations(generate_transaction(*this, 1'000)));

    roll_forward();
    roll_backward();
}

TEST_F(BasicWalTests, SanityCheckSingleTransactionWithCommit)
{
    ASSERT_OK(run_operations(generate_transaction(*this, 1'000, true)));

    roll_forward();
    roll_backward();
}

TEST_F(BasicWalTests, SanityCheckMultipleTransactions)
{
    for (Size i {}; i < 10; ++i)
        ASSERT_OK(run_operations(generate_transaction(*this, 1'000, i != 9)));

    roll_forward();
    roll_backward();
}

TEST_F(BasicWalTests, SanityCheckMultipleTransactionsWithCommit)
{
    for (Size i {}; i < 10; ++i)
        ASSERT_OK(run_operations(generate_transaction(*this, 1'000, true)));

    roll_forward();
    roll_backward();
}

class WalFaultTests: public BasicWalTests {
public:

};

TEST_F(WalFaultTests, FailOnFirstWrite)
{
    interceptors::set_write(FailOnce<0> {"test/wal-"});
    assert_error_42(run_operations({WalOperation::LOG}));

    // We never wrote anything, so the writer should have removed the segment.
    ASSERT_OK(wal->roll_forward(SequenceId::null(), [](auto) {
        return Status::corruption("");
    }));
    ASSERT_OK(wal->roll_backward(SequenceId::null(), [](auto) {
        return Status::corruption("");
    }));
}

TEST_F(WalFaultTests, FailOnFirstOpen)
{
    interceptors::set_open(FailOnce<0> {"test/wal-"});
    assert_error_42(run_operations({WalOperation::LOG}));

    ASSERT_OK(wal->roll_forward(SequenceId::null(), [](auto) {
        return Status::corruption("");
    }));
    ASSERT_OK(wal->roll_backward(SequenceId::null(), [](auto) {
        return Status::corruption("");
    }));
}

TEST_F(WalFaultTests, FailOnNthOpen)
{
    interceptors::set_open(FailOnce<10> {"test/wal-"});
    assert_error_42(run_operations(std::vector<WalOperation>(1'000, WalOperation::LOG)));

    // We should have full records in the WAL, so these tests will work.
    roll_forward(false);
    roll_backward(false);
}

TEST_F(WalFaultTests, FailOnNthWrite)
{
    interceptors::set_write(FailOnce<10> {"test/wal-"});
    assert_error_42(run_operations(std::vector<WalOperation>(1'000, WalOperation::LOG)));

    // We may have a partial record at the end. The WAL will stop short of it.
    roll_forward(false);
    roll_backward(false);
}

} // <anonymous>
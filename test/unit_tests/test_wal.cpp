#include "calico/slice.h"
#include "calico/storage.h"
#include "core/recovery.h"
#include "fakes.h"
#include "tools.h"
#include "unit_tests.h"
#include "utils/system.h"
#include "wal/helpers.h"
#include "wal/reader.h"
#include "wal/writer.h"
#include <array>
#include <gtest/gtest.h>

namespace Calico {

namespace UnitTests {
    extern std::uint32_t random_seed;
} // namespace internal

namespace fs = std::filesystem;

template<class Base>
class TestWithWalSegments : public Base {
public:
    [[nodiscard]] static auto get_segment_name(Id id) -> std::string
    {
        return Base::PREFIX + encode_segment_name(id);
    }

    [[nodiscard]] static auto get_segment_name(Size index) -> std::string
    {
        return Base::PREFIX + encode_segment_name(Id::from_index(index));
    }

    template<class Id>
    [[nodiscard]] auto get_segment_size(const Id &id) const -> Size
    {
        Size size {};
        EXPECT_TRUE(expose_message(Base::storage->file_size(get_segment_name(id), size)));
        return size;
    }

    template<class Id>
    [[nodiscard]] auto get_segment_data(const Id &id) const -> std::string
    {
        RandomReader *reader {};
        EXPECT_TRUE(expose_message(Base::storage->open_random_reader(get_segment_name(id), &reader)));

        std::string data(get_segment_size(id), '\x00');
        Span bytes {data};
        auto read_size = bytes.size();
        EXPECT_TRUE(expose_message(reader->read(bytes.data(), read_size, 0)));
        EXPECT_EQ(read_size, data.size());
        delete reader;
        return data;
    }
};

using TestWithWalSegmentsOnHeap = TestWithWalSegments<TestOnHeap>;
using TestWithWalSegmentsOnDisk = TestWithWalSegments<TestOnDisk>;

//template<class Store>
//[[nodiscard]] static auto get_file_size(const Store &storage, const std::string &path) -> Size
//{
//    Size size {};
//    EXPECT_TRUE(expose_message(storage.file_size(path, size)));
//    return size;
//}

// TODO: Needs to be rewritten, but I guess we should make sure Page is correctly limiting the size of the record it creates.
//class WalPayloadSizeLimitTests : public testing::TestWithParam<Size> {
//public:
//    WalPayloadSizeLimitTests()
//        : scratch(max_size, '\x00'),
//          image {random.get<std::string>('\x00', '\xFF', GetParam())}
//    {
//        static_assert(WAL_SCRATCH_SCALE >= 1);
//    }
//
//    ~WalPayloadSizeLimitTests() override = default;
//
//    Size max_size {GetParam() * WAL_SCRATCH_SCALE};
//    Size min_size {max_size - GetParam()};
//    Random random {UnitTests::random_seed};
//    std::string scratch;
//    std::string image;
//};
//
//TEST_P(WalPayloadSizeLimitTests, LargestPossibleRecord)
//{
//    std::vector<PageDelta> deltas;
//
//    for (Size i {}; i < GetParam(); i += 2)
//        deltas.emplace_back(PageDelta {i, 1});
//
//    auto size = encode_deltas_payload(Id {2}, image, deltas, scratch);
//    ASSERT_GE(size + WalPayloadHeader::SIZE, min_size) << "Excessive scratch memory allocated";
//    ASSERT_LE(size + WalPayloadHeader::SIZE, max_size) << "Scratch memory cannot fit maximally sized WAL record payload";
//}
//
//INSTANTIATE_TEST_SUITE_P(
//    LargestPossibleRecord,
//    WalPayloadSizeLimitTests,
//    ::testing::Values(
//        0x100,
//        0x100 << 1,
//        0x100 << 2,
//        0x100 << 3,
//        0x100 << 4,
//        0x100 << 5,
//        0x100 << 6,
//        0x100 << 7));

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
          scratch(wal_scratch_size(PAGE_SIZE), '\x00')
    {}

    Random random {UnitTests::random_seed};
    std::string image;
    std::string scratch;
};

TEST_F(WalPayloadTests, EncodeAndDecodeFullImage)
{
    const auto payload_in = encode_full_image_payload(Lsn {2}, Id::root(), image, Span {scratch});
    WalPayloadOut payload_out {Span {scratch}.truncate(payload_in.data().size() + 8)};
    ASSERT_EQ(payload_in.lsn(), payload_out.lsn());
    const auto payload = decode_payload(payload_out);
    ASSERT_TRUE(std::holds_alternative<FullImageDescriptor>(payload));
    const auto descriptor = std::get<FullImageDescriptor>(payload);
    ASSERT_EQ(descriptor.pid.value, 1);
    ASSERT_EQ(descriptor.lsn.value, 2);
    ASSERT_EQ(descriptor.image.to_string(), image);
}

TEST_F(WalPayloadTests, EncodeAndDecodeDeltas)
{
    WalRecordGenerator generator;
    auto deltas = generator.setup_deltas(image);
    const auto payload_in = encode_deltas_payload(Lsn{2}, Id::root(), image, deltas, Span {scratch});
    WalPayloadOut payload_out {Span {scratch}.truncate(payload_in.data().size() + sizeof(Lsn))};
    ASSERT_EQ(payload_in.lsn(), payload_out.lsn());
    const auto payload = decode_payload(payload_out);
    ASSERT_TRUE(std::holds_alternative<DeltaDescriptor>(payload));
    const auto descriptor = std::get<DeltaDescriptor>(payload);
    ASSERT_EQ(descriptor.pid.value, 1);
    ASSERT_EQ(descriptor.deltas.size(), deltas.size());
    ASSERT_TRUE(std::all_of(cbegin(descriptor.deltas), cend(descriptor.deltas), [this](const auto &delta) {
        return delta.data == Slice {image}.range(delta.offset, delta.data.size());
    }));
}

[[nodiscard]] auto get_ids(const WalSet &c)
{
    std::vector<Id> ids;
    std::transform(cbegin(c.segments()), cend(c.segments()), back_inserter(ids), [](const auto &entry) {
        return entry.first;
    });
    return ids;
}

class WalSetTests : public testing::Test {
public:
    auto add_segments(Size n)
    {
        for (Size i {}; i < n; ++i) {
            auto id = Id::from_index(i);
            set.add_segment(id);
        }
        ASSERT_EQ(set.last(), Id::from_index(n - 1));
    }

    WalSet set;
};

TEST_F(WalSetTests, NewCollectionState)
{
    ASSERT_TRUE(set.last().is_null());
}

TEST_F(WalSetTests, AddSegment)
{
    set.add_segment(Id {1});
    ASSERT_EQ(set.last().value, 1);
}

TEST_F(WalSetTests, RecordsMostRecentId)
{
    add_segments(20);
    ASSERT_EQ(set.last(), Id::from_index(19));
}

template<class Itr>
[[nodiscard]] auto contains_n_consecutive_segments(const Itr &begin, const Itr &end, Id id, Size n)
{
    return std::distance(begin, end) == std::ptrdiff_t(n) && std::all_of(begin, end, [&id](auto current) {
               return current.value == id.value++;
           });
}

TEST_F(WalSetTests, RecordsSegmentInfoCorrectly)
{
    add_segments(20);

    const auto ids = get_ids(set);
    ASSERT_EQ(ids.size(), 20);

    const auto result = get_ids(set);
    ASSERT_TRUE(contains_n_consecutive_segments(cbegin(result), cend(result), Id {1}, 20));
}

TEST_F(WalSetTests, RemovesAllSegmentsFromLeft)
{
    add_segments(20);
    // Id::from_index(20) is one past the end.
    set.remove_before(Id::from_index(20));

    const auto ids = get_ids(set);
    ASSERT_TRUE(ids.empty());
}

TEST_F(WalSetTests, RemovesAllSegmentsFromRight)
{
    add_segments(20);
    // Id::null() is one before the beginning.
    set.remove_after(Id::null());

    const auto ids = get_ids(set);
    ASSERT_TRUE(ids.empty());
}

TEST_F(WalSetTests, RemovesSomeSegmentsFromLeft)
{
    add_segments(20);
    set.remove_before(Id::from_index(10));

    const auto ids = get_ids(set);
    ASSERT_TRUE(contains_n_consecutive_segments(cbegin(ids), cend(ids), Id::from_index(10), 10));
}

TEST_F(WalSetTests, RemovesSomeSegmentsFromRight)
{
    add_segments(20);
    set.remove_after(Id::from_index(9));

    const auto ids = get_ids(set);
    ASSERT_TRUE(contains_n_consecutive_segments(cbegin(ids), cend(ids), Id::from_index(0), 10));
}

class LogReaderWriterTests : public TestWithWalSegmentsOnHeap {
public:
    static constexpr Size PAGE_SIZE {0x100};

    LogReaderWriterTests()
        : reader_payload(wal_scratch_size(PAGE_SIZE), '\x00'),
          reader_tail(wal_block_size(PAGE_SIZE), '\x00'),
          writer_tail(wal_block_size(PAGE_SIZE), '\x00'),
          scratch {wal_scratch_size(PAGE_SIZE), 32}
    {}

    // NOTE: This invalidates the most-recently-allocated log reader.
    auto get_reader(Id id) -> LogReader
    {
        const auto path = get_segment_name(id);
        RandomReader *temp {};
        EXPECT_TRUE(expose_message(storage->open_random_reader(path, &temp)));
        reader_file.reset(temp);
        return LogReader {*reader_file};
    }

    // NOTE: This invalidates the most-recently-allocated log writer.
    auto get_writer(Id id) -> LogWriter
    {
        const auto path = get_segment_name(id);
        AppendWriter *temp {};
        EXPECT_TRUE(expose_message(storage->open_append_writer(path, &temp)));
        writer_file.reset(temp);
        return LogWriter {*writer_file, writer_tail, flushed_lsn};
    }

    auto write_string(LogWriter &writer, const std::string &payload) -> void
    {
        auto buffer = scratch.get();
        ASSERT_GE(buffer->size(), payload.size() + sizeof(Id));
        mem_copy(buffer->range(sizeof(Lsn)), payload);
        WalPayloadIn in {{++last_lsn.value}, buffer->range(0, payload.size() + sizeof(Lsn))};
        ASSERT_OK(writer.write(in));
    }

    auto read_string(LogReader &reader) -> std::string
    {
        WalPayloadOut payload;
        EXPECT_TRUE(expose_message(reader.read(payload, Span {reader_payload}, Span {reader_tail})));
        return payload.data().to_string();
    }

    auto run_basic_test(const std::vector<std::string> &payloads) -> void
    {
        auto writer = get_writer(Id {1});
        auto reader = get_reader(Id {1});
        for (const auto &payload: payloads) {
            ASSERT_LE(payload.size(), wal_scratch_size(PAGE_SIZE) - sizeof(Id));
            write_string(writer, payload);
        }
        ASSERT_OK(writer.flush());

        for (const auto &payload: payloads) {
            const auto str = read_string(reader);
            ASSERT_EQ(str, payload);
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

    std::atomic<Id> flushed_lsn {};
    std::string reader_payload;
    std::string reader_tail;
    std::string writer_tail;
    LogScratchManager scratch;
    std::unique_ptr<RandomReader> reader_file;
    std::unique_ptr<AppendWriter> writer_file;
    Id last_lsn;
    Random random {UnitTests::random_seed};
};

TEST_F(LogReaderWriterTests, DoesNotFlushEmptyBlock)
{
    auto writer = get_writer(Id {1});
    (void)writer.flush();

    Size file_size {};
    ASSERT_OK(storage->file_size("test/wal-1", file_size));
    ASSERT_EQ(file_size, 0);
}

TEST_F(LogReaderWriterTests, WritesMultipleBlocks)
{
    auto writer = get_writer(Id {1});
    write_string(writer, get_large_payload());
    ASSERT_OK(writer.flush());

    Size file_size {};
    ASSERT_OK(storage->file_size("test/wal-1", file_size));
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

    auto writer = get_writer(Id {1});
    auto reader = get_reader(Id {1});
    for (const auto &payload: payloads) {
        ASSERT_LE(payload.size(), wal_scratch_size(PAGE_SIZE));
        write_string(writer, payload);
        if (random.get(10) == 0) {
            auto s = writer.flush();
            ASSERT_TRUE(s.is_ok() or s.is_logic_error());
        }
    }
    (void)writer.flush();

    for (const auto &payload: payloads) {
        ASSERT_EQ(read_string(reader), payload);
    }
}

using namespace std::chrono_literals;

class WalWriterTests : public TestWithWalSegmentsOnHeap {
public:
    static constexpr Size PAGE_SIZE {0x100};
    static constexpr Size WAL_LIMIT {8};

    WalWriterTests()
        : scratch {wal_scratch_size(PAGE_SIZE), 32},
          system {"test", {}},
          tail(wal_block_size(PAGE_SIZE), '\x00'),
          writer {WalWriter::Parameters{
              PREFIX,
              Span {tail},
              storage.get(),
              &system,
              &set,
              &flushed_lsn,
              WAL_LIMIT,
          }}
    {}

    ~WalWriterTests() override = default;

    WalSet set;
    LogScratchManager scratch;
    System system;
    std::atomic<Id> flushed_lsn {};
    std::string tail;
    Random random {UnitTests::random_seed};
    WalWriter writer;
};

TEST_F(WalWriterTests, Destroy)
{
    ASSERT_OK(std::move(writer).destroy());
    ASSERT_FALSE(storage->file_exists(PREFIX + encode_segment_name(Id::root())).is_ok());
}

TEST_F(WalWriterTests, DoesNotLeaveEmptySegmentsAfterNormalClose)
{
    // After the writer closes a segment file, it will either add it to the set of segment files, or it
    // will delete it. Empty segments get deleted, while nonempty segments get added.
    writer.advance();
    writer.advance();
    writer.advance();

    // Blocks until the last segment is deleted.
    ASSERT_OK(std::move(writer).destroy());
    ASSERT_TRUE(set.segments().empty());

    std::vector<std::string> children;
    ASSERT_OK(storage->get_children(ROOT, children));
    ASSERT_TRUE(children.empty());
}

template<class Test>
static auto test_write_until_failure(Test &test) -> void
{
    Id last_lsn;
    while (!test.system.has_error()) {
        auto buffer = test.scratch.get();
        const auto size = test.random.get(1UL, buffer->size());
        test.writer.write(WalPayloadIn {{++last_lsn.value}, buffer->truncate(size)});
    }

    (void)std::move(test.writer).destroy();
    assert_special_error(test.system.original_error().status);
}

template<class Test>
static auto count_segments(Test &test) -> Size
{
    const auto expected = test.set.segments().size();

    std::vector<std::string> children;
    EXPECT_TRUE(expose_message(test.storage->get_children(Test::ROOT, children)));
    EXPECT_EQ(children.size(), expected);
    return expected;
}

TEST_F(WalWriterTests, DoesNotLeaveEmptySegmentsAfterWriteFailure)
{
    interceptors::set_write(FailAfter<0> {"test/wal-"});
    test_write_until_failure(*this);
    ASSERT_EQ(count_segments(*this), 0);
}

TEST_F(WalWriterTests, LeavesSingleNonEmptySegmentAfterOpenFailure)
{
    interceptors::set_open(FailAfter<0> {"test/wal-"});
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
    ASSERT_EQ(count_segments(*this), 11);
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
        : scratch {wal_scratch_size(PAGE_SIZE), 32},
          reader_data(wal_scratch_size(PAGE_SIZE), '\x00'),
          reader_tail(wal_block_size(PAGE_SIZE), '\x00'),
          writer_tail(wal_block_size(PAGE_SIZE), '\x00'),
          writer {WalWriter::Parameters{
              PREFIX,
              Span {writer_tail},
              storage.get(),
              &system,
              &set,
              &flushed_lsn,
              WAL_LIMIT,
          }},
          tasks {[this](auto event) {
              if (std::holds_alternative<WalPayloadIn>(event)) {
                  writer.write(std::get<WalPayloadIn>(event));
              } else if (std::holds_alternative<FlushToken>(event)) {
                  writer.flush();
              } else if (std::holds_alternative<AdvanceToken>(event)) {
                  writer.advance();
              }
          }, 32}
    {}

    ~WalReaderWriterTests() override = default;

    [[nodiscard]]
    auto get_reader() -> WalReader
    {
        return WalReader {
            *storage,
            set,
            PREFIX,
            Span {reader_tail},
            Span {reader_data}};
    }

    [[nodiscard]]
    auto get_payload() -> WalPayloadIn
    {
        auto buffer = *scratch.get();
        const auto size = random.get(1, 32);
        payloads.emplace_back(random.get<std::string>('a', 'z', size));
        mem_copy(buffer.range(sizeof(Lsn)), payloads.back());
        return WalPayloadIn {{++last_lsn.value}, buffer.truncate(size + sizeof(Lsn))};
    }

    auto emit_segments(Size num_writes)
    {
        for (Size i {}; i < num_writes; ++i)
            tasks.dispatch(get_payload(), i == num_writes - 1);

        return std::move(writer).destroy();
    }

    [[nodiscard]]
    auto contains_sequence(WalReader &reader, Id final_lsn) -> Status
    {
        auto s = ok();
        Id lsn;
        // Roll forward to the end of the WAL.
        while (s.is_ok()) {
            s = reader.roll([&](auto payload) {
                EXPECT_EQ(Id {++lsn.value}, payload.lsn());
                const auto descriptor = decode_payload(payload);
                return ok();
            });
            if (!s.is_ok()) break;
            s = reader.seek_next();
            if (s.is_not_found()) {
                EXPECT_EQ(lsn, final_lsn);
                return ok();
            } else if (!s.is_ok()) {
                break;
            }
        }
        return s;
    }

    [[nodiscard]]
    auto roll_segments_forward(WalReader &reader) -> Status
    {
        auto s = ok();
        // Roll forward to the end of the WAL.
        while (s.is_ok()) {
            s = reader.roll([&](auto info) {
                EXPECT_EQ(info.data().to_string(), payloads[info.lsn().as_index()]);
                return ok();
            });
            if (!s.is_ok()) break;
            s = reader.seek_next();
            if (s.is_not_found()) {
                return ok();
            } else if (!s.is_ok()) {
                break;
            }
        }
        return s;
    }

    [[nodiscard]]
    auto roll_segments_backward(WalReader &reader) -> Status
    {
        auto s = ok();
        for (Size i {}; s.is_ok(); ++i) {

            Id first_lsn;
            s = reader.read_first_lsn(first_lsn);
            if (!s.is_ok()) return s;

            s = reader.roll([&](auto info) {
                EXPECT_EQ(info.data().to_string(), payloads[info.lsn().as_index()]);
                return ok();
            });
            if (!s.is_ok()) {
                if (!s.is_corruption() || i)
                    break;
            }
            s = reader.seek_previous();
            if (s.is_not_found()) {
                return ok();
            } else if (!s.is_ok()) {
                break;
            }
        }
        return s;
    }

    struct FlushToken {};
    struct AdvanceToken {};
    using Event = std::variant<WalPayloadIn, FlushToken, AdvanceToken>;

    Id last_lsn;
    std::vector<std::string> payloads;
    WalSet set;
    LogScratchManager scratch;
    std::atomic<Id> flushed_lsn {};
    std::string reader_data;
    std::string reader_tail;
    std::string writer_tail;
    Random random {UnitTests::random_seed};
    System system {PREFIX, {}};
    WalWriter writer;
    Worker<Event> tasks;
};

static auto does_not_lose_records_test(WalReaderWriterTests &test, Size num_writes)
{
    ASSERT_OK(test.emit_segments(num_writes));

    auto reader = test.get_reader();
    ASSERT_OK(reader.open());
    ASSERT_OK(test.contains_sequence(reader, Id {num_writes}));
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
    ASSERT_OK(emit_segments(5'000));

    auto reader = get_reader();
    ASSERT_OK(reader.open());
    ASSERT_OK(roll_segments_forward(reader));
    ASSERT_OK(roll_segments_backward(reader));
}

TEST_F(WalReaderWriterTests, RollWalAfterWriteError)
{
    interceptors::set_write(FailOnce<1> {"test/wal-"});

    emit_segments(5'000);
    ASSERT_TRUE(system.has_error());
    assert_special_error(system.original_error().status);
    system.pop_error();

    auto reader = get_reader();
    ASSERT_OK(reader.open());
    auto s = roll_segments_forward(reader);
    ASSERT_TRUE(s.is_corruption() or s.is_ok());
    ASSERT_OK(roll_segments_backward(reader));
}

TEST_F(WalReaderWriterTests, RollWalAfterOpenError)
{
    interceptors::set_open(FailOnce<3> {"test/wal-"});

    ASSERT_FALSE(emit_segments(5'000).is_ok());
    assert_special_error(system.pop_error().status);

    auto reader = get_reader();
    ASSERT_OK(reader.open());
    auto s = roll_segments_forward(reader);
    ASSERT_TRUE(s.is_corruption() or s.is_ok());
    ASSERT_OK(roll_segments_backward(reader));
}

////class WalCleanerTests : public WalReaderWriterTests {
////public:
////    WalCleanerTests()
////        : cleanup {WalCleanup::Parameters{
////              PREFIX,
////              nullptr, // TODO
////              storage.get(),
////              &system,
////              &set,
////          }}
////    {
////        // Append the cleanup task. We keep the cleanup and writer functions in the same task in
////        // the actual implementation.
////        tasks.add([this] {cleanup();});
////    }
////
////    ~WalCleanerTests() override = default;
////
////    WalCleanup cleanup;
////};
////
////TEST_F(WalCleanerTests, RemoveBeforeNullIdDoesNothing)
////{
////    cleanup.remove_before(Id::null());
////}
////
////TEST_F(WalCleanerTests, DoesNotRemoveOnlySegment)
////{
////    writer.write(get_payload());
////    writer.write(get_payload());
////    writer.write(get_payload());
////
////    // NOTE: force() should always be called before destroy.
////    tasks.force();
////    ASSERT_OK(std::move(writer).destroy());
////    ASSERT_EQ(set.segments().size(), 1);
////
////    cleanup.remove_before(last_lsn);
////    tasks.force();
////
////    ASSERT_EQ(set.segments().size(), 1);
////}
////
////TEST_F(WalCleanerTests, KeepsAtLeastMostRecentSegment)
////{
////    static constexpr Size NUM_ROUNDS {1'000};
////    for (Size i {}; i < NUM_ROUNDS; ++i) {
////        writer.write(get_payload());
////        cleanup.remove_before(last_lsn);
////    }
////    tasks.force();
////    ASSERT_OK(std::move(writer).destroy());
////    ASSERT_GE(set.segments().size(), 1);
////}
//
//class BasicWalTests: public TestWithWalSegmentsOnHeap {
//public:
//    static constexpr Size PAGE_SIZE {0x100};
//
//    ~BasicWalTests() override = default;
//
//    BasicWalTests()
//        : scratch {wal_scratch_size(PAGE_SIZE), 32}
//    {}
//
//    auto SetUp() -> void override
//    {
//        auto r = WriteAheadLog::open({
//            PREFIX,
//            storage.get(),
//            &state,
//            PAGE_SIZE,
//            32,
//            32,
//        });
//        ASSERT_TRUE(r.has_value()) << r.error().what().data();
//        wal = std::move(*r);
//
//        ASSERT_OK(wal->start_workers());
//    }
//
//    [[nodiscard]]
//    auto get_data_payload(const std::string &data) -> WalPayloadIn
//    {
//        WalPayloadIn payload {wal->current_lsn(), scratch.get()};
//        payload.shrink_to_fit(1 + data.size());
//        payloads.emplace_back("p" + data);
//        mem_copy(payload.data(), payloads.back());
//        payloads_since_commit++;
//        return payload;
//    }
//
//    [[nodiscard]]
//    auto get_random_data_payload() -> WalPayloadIn
//    {
//        const auto max_size = wal_scratch_size(PAGE_SIZE) - WalPayloadHeader::SIZE - 1;
//        const auto size = random.get(1, max_size);
//        return get_data_payload(random.get<std::string>('a', 'z', size));
//    }
//
//    [[nodiscard]]
//    auto get_commit_payload() -> WalPayloadIn
//    {
//        payloads_since_commit = 0;
//        WalPayloadIn payload {wal->current_lsn(), scratch.get()};
//        payloads.emplace_back("c");
//        payload.data()[0] = 'c';
//        payload.shrink_to_fit(1);
//        return payload;
//    }
//
//    auto roll_forward(bool strict = true)
//    {
//        auto lsn = Id::root();
//        ASSERT_OK(wal->roll_forward(lsn, [&](auto payload) {
//            const auto lhs = payload.data();
//            const auto rhs = payloads.at(payload.lsn().as_index());
//            EXPECT_EQ(lhs.size(), rhs.size());
//            EXPECT_EQ(lhs.to_string(), rhs);
//            EXPECT_EQ(Id {lsn.value++}, payload.lsn());
//            return ok();
//        }));
//        // We should have hit all records.
//        if (strict) {
//            ASSERT_EQ(lsn, wal->current_lsn());
//        }
//    }
//
//    auto roll_backward(bool strict = true)
//    {
//        std::vector<Id> lsns;
//        ASSERT_OK(wal->roll_backward(commit_lsn, [&lsns, this](auto payload) {
//            lsns.emplace_back(payload.lsn());
//            EXPECT_GT(payload.lsn(), commit_lsn);
//            EXPECT_EQ(payload.data().to_string(), payloads[payload.lsn().as_index()]);
//            return ok();
//        }));
//        if (strict) {
//            ASSERT_EQ(lsns.size(), payloads_since_commit);
//        }
//        std::sort(begin(lsns), end(lsns));
//        Id lsn_counter {commit_lsn};
//        for (const auto &lsn: lsns)
//            ASSERT_EQ(++lsn_counter.value, lsn.value);
//    }
//
//    enum class WalOperation: int {
//        FLUSH = 1,
//        SEGMENT = 2,
//        COMMIT = 3,
//        LOG = 4,
//    };
//
//    auto run_operations(std::vector<WalOperation> operations, bool keep_clean = false)
//    {
//        for (auto operation: operations) {
//            switch (operation) {
//                case WalOperation::FLUSH:
//                    (void)wal->flush();
//                    break;
//                case WalOperation::SEGMENT:
//                    (void)wal->advance();
//                    break;
//                case WalOperation::COMMIT: {
//                    const auto payload = get_commit_payload();
//                    const auto lsn = payload.lsn();
//                    wal->log(payload);
//                    wal->advance();
//                    commit_lsn = lsn;
//                    break;
//                }
//                case WalOperation::LOG:
//                    wal->log(get_random_data_payload());
//                    break;
//            }
//            if (keep_clean)
//                wal->cleanup(commit_lsn);
//            if (state.has_error())
//                break;
//        }
//    }
//
//    System state {"test", LogLevel::OFF, {}};
//    Random random {42};
//    Size payloads_since_commit {};
//    Id commit_lsn;
//    LogScratchManager scratch;
//    std::vector<std::string> payloads;
//    std::unique_ptr<WriteAheadLog> wal;
//};
//
//TEST_F(BasicWalTests, StartsAndStops)
//{
//
//}
//
//TEST_F(BasicWalTests, NewWalState)
//{
//    ASSERT_EQ(wal->flushed_lsn().value, 0);
//    ASSERT_EQ(wal->current_lsn().value, 1);
//}
//
//TEST_F(BasicWalTests, RollWhileEmpty)
//{
//    ASSERT_OK(wal->roll_forward(Id::null(), [](auto) {return ok();}));
//}
//
//TEST_F(BasicWalTests, FlushWithEmptyTailBuffer)
//{
//    run_operations({WalOperation::FLUSH});
//}
//
//TEST_F(BasicWalTests, SegmentWithEmptyTailBuffer)
//{
//    run_operations({WalOperation::SEGMENT});
//}
//
//TEST_F(BasicWalTests, RollSingleRecord)
//{
//    run_operations({
//        WalOperation::LOG,
//        WalOperation::COMMIT,
//    });
//
//    roll_forward();
//    roll_backward();
//}
//
//TEST_F(BasicWalTests, RollMultipleRecords)
//{
//    run_operations({
//        WalOperation::LOG,
//        WalOperation::LOG,
//        WalOperation::LOG,
//        WalOperation::COMMIT,
//    });
//
//    roll_forward();
//    roll_backward();
//}
//
//TEST_F(BasicWalTests, RollMultipleCommits)
//{
//    run_operations({
//        WalOperation::LOG,
//        WalOperation::LOG,
//        WalOperation::COMMIT,
//        WalOperation::LOG,
//        WalOperation::LOG,
//        WalOperation::COMMIT,
//    });
//
//    roll_forward();
//    roll_backward();
//}
//
//template<class Test>
//static auto generate_transaction(Test &test, Size n, bool add_commit = false)
//{
//    std::vector<BasicWalTests::WalOperation> operations;
//    operations.reserve(n);
//
//    while (operations.size() < n) {
//        const auto r = test.random.get(20);
//        if (r >= 2 || operations.empty() || operations.back() != BasicWalTests::WalOperation::LOG) {
//            operations.emplace_back(BasicWalTests::WalOperation::LOG);
//        } else {
//            if (r == 0) {
//                operations.emplace_back(BasicWalTests::WalOperation::FLUSH);
//            } else {
//                operations.emplace_back(BasicWalTests::WalOperation::SEGMENT);
//            }
//        }
//    }
//    if (add_commit)
//        operations.emplace_back(BasicWalTests::WalOperation::COMMIT);
//    return operations;
//}
//
//TEST_F(BasicWalTests, SanityCheckSingleTransaction)
//{
//    run_operations(generate_transaction(*this, 1'000));
//
//    roll_forward(false);
//    roll_backward(false);
//}
//
//TEST_F(BasicWalTests, SanityCheckSingleTransactionWithCommit)
//{
//    run_operations(generate_transaction(*this, 1'000, true));
//
//    roll_forward();
//    roll_backward();
//}
//
//TEST_F(BasicWalTests, SanityCheckMultipleTransactions)
//{
//    for (Size i {}; i < 10; ++i)
//        run_operations(generate_transaction(*this, 1'000, i == 3 || i == 6));
//
//    roll_forward(false);
//    roll_backward(false);
//}
//
//TEST_F(BasicWalTests, SanityCheckMultipleTransactionsWithCommit)
//{
//    for (Size i {}; i < 10; ++i)
//        run_operations(generate_transaction(*this, 1'000, true));
//
//    roll_forward();
//    roll_backward();
//}
////
////class WalFaultTests: public BasicWalTests {
////public:
////
////};
////
////TEST_F(WalFaultTests, FailOnFirstWrite)
////{
////    interceptors::set_write(FailOnce<0> {"test/wal-"});
////    assert_special_error(run_operations({WalOperation::LOG}));
////
////    // We never wrote anything, so the writer should have removed the segment.
////    ASSERT_OK(wal->roll_forward(Id::null(), [](auto) {
////        return corruption("");
////    }));
////    ASSERT_OK(wal->roll_backward(Id::null(), [](auto) {
////        return corruption("");
////    }));
////}
////
////TEST_F(WalFaultTests, FailOnFirstOpen)
////{
////    interceptors::set_open(FailOnce<0> {"test/wal-"});
////    assert_special_error(run_operations({WalOperation::LOG}));
////
////    ASSERT_OK(wal->roll_forward(Id::null(), [](auto) {
////        return corruption("");
////    }));
////    ASSERT_OK(wal->roll_backward(Id::null(), [](auto) {
////        return corruption("");
////    }));
////}
////
////TEST_F(WalFaultTests, FailOnNthOpen)
////{
////    interceptors::set_open(FailOnce<10> {"test/wal-"});
////    assert_special_error(run_operations(std::vector<WalOperation>(1'000, WalOperation::LOG)));
////
////    // We should have full records in the WAL, so these tests will work.
////    roll_forward(false);
////    roll_backward(false);
////}
////
////TEST_F(WalFaultTests, FailOnNthWrite)
////{
////    interceptors::set_write(FailOnce<10> {"test/wal-"});
////    assert_special_error(run_operations(std::vector<WalOperation>(1'000, WalOperation::LOG)));
////
////    // We may have a partial record at the end. The WAL will stop short of it.
////    roll_forward(false);
////    roll_backward(false);
////}
////
////TEST(DisabledWalTests, ExersizeStubs)
////{
////    LogScratchManager scratch {wal_scratch_size(0x100), 32};
////    DisabledWriteAheadLog wal;
////
////    ASSERT_OK(wal.worker_status());
////    ASSERT_FALSE(wal.is_enabled());
////    ASSERT_FALSE(wal.is_working());
////    ASSERT_TRUE(wal.flushed_lsn().is_null());
////    ASSERT_TRUE(wal.current_lsn().is_null());
////    ASSERT_OK(wal.log(WalPayloadIn {Id::null(), scratch.get()}));
////    ASSERT_OK(wal.flush());
////    ASSERT_OK(wal.advance());
////    ASSERT_OK(wal.start_workers());
////    ASSERT_OK(wal.stop_workers());
////    ASSERT_OK(wal.roll_forward(Id::null(), [](auto) {return Status::ok();}));
////    ASSERT_OK(wal.roll_backward(Id::null(), [](auto) {return Status::ok();}));
////}

} // <anonymous>
#include "calico/slice.h"
#include "calico/storage.h"
#include "tools.h"
#include "unit_tests.h"
#include "utils/logging.h"
#include "wal/helpers.h"
#include "wal/reader.h"
#include "wal/writer.h"
#include <array>
#include <gtest/gtest.h>

namespace Calico {

namespace fs = std::filesystem;

template<class Base>
class TestWithWalSegments : public Base {
public:
    [[nodiscard]] static auto get_segment_name(Id id) -> std::string
    {
        return encode_segment_name(Base::PREFIX + std::string {"wal-"}, id);
    }

    [[nodiscard]] static auto get_segment_name(Size index) -> std::string
    {
        return encode_segment_name(Base::PREFIX + std::string {"wal-"}, Id::from_index(index));
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
        Reader *reader {};
        EXPECT_TRUE(expose_message(Base::storage->new_reader(get_segment_name(id), &reader)));

        std::string data(get_segment_size(id), '\x00');
        Span bytes {data};
        auto read_size = bytes.size();
        EXPECT_TRUE(expose_message(reader->read(bytes.data(), read_size, 0)));
        EXPECT_EQ(read_size, data.size());
        delete reader;
        return data;
    }
};

using TestWithWalSegmentsOnHeap = TestWithWalSegments<InMemoryTest>;
using TestWithWalSegmentsOnDisk = TestWithWalSegments<OnDiskTest>;

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

TEST_F(WalRecordMergeTests, MergingEmptyRecordsIndicatesCorruption)
{
    ASSERT_TRUE(merge_records_left(lhs, rhs).is_corruption());
}

TEST_F(WalRecordMergeTests, ValidLeftMerges)
{
    ASSERT_TRUE(std::all_of(cbegin(valid_left_merges), cend(valid_left_merges), [this](const auto &triplet) {
        setup(triplet);
        const auto s = merge_records_left(lhs, rhs);
        return s.is_ok() && check(lhs, triplet[2]);
    }));
}

TEST_F(WalRecordMergeTests, MergingInvalidTypesIndicatesCorruption)
{
    setup({WalRecordHeader::Type::FIRST, WalRecordHeader::Type::FIRST});
    ASSERT_TRUE(merge_records_left(lhs, rhs).is_corruption());

    setup({WalRecordHeader::Type {}, WalRecordHeader::Type::MIDDLE});
    ASSERT_TRUE(merge_records_left(lhs, rhs).is_corruption());

    setup({WalRecordHeader::Type::MIDDLE, WalRecordHeader::Type::FIRST});
    ASSERT_TRUE(merge_records_left(lhs, rhs).is_corruption());
}


class WalRecordGenerator {
public:

    [[nodiscard]]
    auto setup_deltas(Span image) -> std::vector<PageDelta>
    {
        static constexpr Size MAX_WIDTH {30};
        static constexpr Size MAX_SPREAD {20};
        std::vector<PageDelta> deltas;

        for (auto offset = random.Next<Size>(image.size() / 10); offset < image.size(); ) {
            const auto rest = image.size() - offset;
            const auto size = random.Next<Size>(1, std::min(rest, MAX_WIDTH));
            deltas.emplace_back(PageDelta {offset, size});
            offset += size + random.Next<Size>(1, MAX_SPREAD);
        }
        for (const auto &[offset, size]: deltas) {
            const auto replacement = random.Generate(size);
            mem_copy(image.range(offset, size), replacement);
        }
        return deltas;
    }

private:
    Tools::RandomGenerator random;
};

class WalPayloadTests : public testing::Test {
public:
    static constexpr Size PAGE_SIZE {0x80};

    WalPayloadTests()
        : image {random.Generate(PAGE_SIZE).to_string()},
          scratch(wal_scratch_size(PAGE_SIZE), '\x00')
    {}

    Tools::RandomGenerator random;
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

[[nodiscard]]
 auto get_ids(const WalSet &c)
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
[[nodiscard]]
auto contains_n_consecutive_segments(const Itr &begin, const Itr &end, Id id, Size n)
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
//
//class LogReaderWriterTests : public TestWithWalSegmentsOnHeap {
//public:
//    static constexpr Size PAGE_SIZE {0x200};
//
//    LogReaderWriterTests()
//        : reader_payload(wal_scratch_size(PAGE_SIZE), '\x00'),
//          reader_tail(wal_block_size(PAGE_SIZE), '\x00'),
//          writer_tail(wal_block_size(PAGE_SIZE), '\x00'),
//          scratch(wal_scratch_size(PAGE_SIZE), '\x00')
//    {}
//
//    // NOTE: This invalidates the most-recently-allocated log reader.
//    auto get_reader(Id id) -> WalReader
//    {
//        const auto path = get_segment_name(id);
//        Reader *temp {};
//        EXPECT_TRUE(expose_message(storage->new_reader(path, &temp)));
//        reader_file.reset(temp);
//        return WalIterator {*reader_file, reader_tail};
//    }
//
//    // NOTE: This invalidates the most-recently-allocated log writer.
//    auto get_writer(Id id) -> LogWriter
//    {
//        const auto path = get_segment_name(id);
//        Logger *temp {};
//        EXPECT_TRUE(expose_message(storage->new_logger(path, &temp)));
//        writer_file.reset(temp);
//        return LogWriter {*writer_file, writer_tail, flushed_lsn};
//    }
//
//    auto write_string(LogWriter &writer, const std::string &payload) -> void
//    {
//        Span buffer {scratch};
//        ASSERT_GE(buffer.size(), payload.size() + sizeof(Id));
//        mem_copy(buffer.range(sizeof(Lsn)), payload);
//        WalPayloadIn in {{++last_lsn.value}, buffer.range(0, payload.size() + sizeof(Lsn))};
//        ASSERT_OK(writer.write(in));
//    }
//
//    auto read_string(WalIterator &reader) -> std::string
//    {
//        WalPayloadOut payload;
//        EXPECT_TRUE(expose_message(reader.read(payload)));
//        return payload.data().to_string();
//    }
//
//    auto run_basic_test(const std::vector<std::string> &payloads) -> void
//    {
//        auto writer = get_writer(Id {1});
//        auto reader = get_reader(Id {1});
//        for (const auto &payload: payloads) {
//            ASSERT_LE(payload.size(), wal_scratch_size(PAGE_SIZE) - sizeof(Id));
//            write_string(writer, payload);
//        }
//        ASSERT_OK(writer.flush());
//
//        for (const auto &payload: payloads) {
//            const auto str = read_string(reader);
//            ASSERT_EQ(str, payload);
//        }
//    }
//
//    [[nodiscard]] auto get_small_payload() -> std::string
//    {
//        return random.Generate(wal_scratch_size(PAGE_SIZE) / random.Next<Size>(10, 20)).to_string();
//    }
//
//    [[nodiscard]] auto get_large_payload() -> std::string
//    {
//        return random.Generate(wal_scratch_size(PAGE_SIZE) / random.Next<Size>(2, 4)).to_string();
//    }
//
//    std::atomic<Id> flushed_lsn {};
//    std::string reader_payload;
//    std::string reader_tail;
//    std::string writer_tail;
//    std::string scratch;
//    std::unique_ptr<Reader> reader_file;
//    std::unique_ptr<Logger> writer_file;
//    Id last_lsn;
//    Tools::RandomGenerator random;
//};
//
//TEST_F(LogReaderWriterTests, DoesNotFlushEmptyBlock)
//{
//    auto writer = get_writer(Id {1});
//    (void)writer.flush();
//
//    Size file_size {};
//    ASSERT_OK(storage->file_size("test/wal-1", file_size));
//    ASSERT_EQ(file_size, 0);
//}
//
//TEST_F(LogReaderWriterTests, WritesMultipleBlocks)
//{
//    auto writer = get_writer(Id {1});
//    write_string(writer, get_large_payload());
//    ASSERT_OK(writer.flush());
//
//    Size file_size {};
//    ASSERT_OK(storage->file_size("test/wal-1", file_size));
//    ASSERT_EQ(file_size % writer_tail.size(), 0);
//    ASSERT_GT(file_size / writer_tail.size(), 0);
//}
//
//TEST_F(LogReaderWriterTests, SingleSmallPayload)
//{
//    run_basic_test({get_small_payload()});
//}
//
//TEST_F(LogReaderWriterTests, MultipleSmallPayloads)
//{
//    run_basic_test({
//        get_small_payload(),
//        get_small_payload(),
//        get_small_payload(),
//        get_small_payload(),
//        get_small_payload(),
//    });
//}
//
//TEST_F(LogReaderWriterTests, SingleLargePayload)
//{
//    run_basic_test({get_large_payload()});
//}
//
//TEST_F(LogReaderWriterTests, MultipleLargePayloads)
//{
//    run_basic_test({
//        get_large_payload(),
//        get_large_payload(),
//        get_large_payload(),
//        get_large_payload(),
//        get_large_payload(),
//    });
//}
//
//TEST_F(LogReaderWriterTests, MultipleMixedPayloads)
//{
//    run_basic_test({
//        get_small_payload(),
//        get_large_payload(),
//        get_small_payload(),
//        get_large_payload(),
//        get_small_payload(),
//    });
//}
//
//TEST_F(LogReaderWriterTests, SanityCheck)
//{
//    std::vector<std::string> payloads(1'000);
//    std::generate(begin(payloads), end(payloads), [this] {
//        return random.Next<Size>(4) ? get_small_payload() : get_large_payload();
//    });
//    run_basic_test(payloads);
//}
//
//TEST_F(LogReaderWriterTests, HandlesEarlyFlushes)
//{
//    std::vector<std::string> payloads(1'000);
//    std::generate(begin(payloads), end(payloads), [this] {
//        return random.Next<Size>(4) ? get_small_payload() : get_large_payload();
//    });
//
//    auto writer = get_writer(Id {1});
//    auto reader = get_reader(Id {1});
//    for (const auto &payload: payloads) {
//        ASSERT_LE(payload.size(), wal_scratch_size(PAGE_SIZE));
//        write_string(writer, payload);
//        if (random.Next<Size>(10) == 0) {
//            auto s = writer.flush();
//            ASSERT_TRUE(s.is_ok() or s.is_logic_error());
//        }
//    }
//    (void)writer.flush();
//
//    for (const auto &payload: payloads) {
//        ASSERT_EQ(read_string(reader), payload);
//    }
//}

using namespace std::chrono_literals;

class WalWriterTests : public TestWithWalSegmentsOnHeap {
public:
    static constexpr Size PAGE_SIZE {0x100};
    static constexpr Size WAL_LIMIT {8};

    WalWriterTests()
        : scratch(wal_scratch_size(PAGE_SIZE), '\x00'),
          tail(wal_block_size(PAGE_SIZE), '\x00'),
          writer {WalWriter::Parameters{
              "test/wal-",
              Span {tail},
              storage.get(),
              &error_buffer,
              &set,
              &flushed_lsn,
              WAL_LIMIT,
          }}
    {}

    ~WalWriterTests() override = default;

    WalSet set;
    ErrorBuffer error_buffer;
    std::string scratch;
    std::atomic<Id> flushed_lsn {};
    std::string tail;
    Tools::RandomGenerator random;
    WalWriter writer;
};

TEST_F(WalWriterTests, Destroy)
{
    std::move(writer).destroy();
    ASSERT_FALSE(storage->file_exists(get_segment_name(Id::root())).is_ok());
}

TEST_F(WalWriterTests, DoesNotLeaveEmptySegmentsAfterNormalClose)
{
    // After the writer closes a segment file, it will either add it to the set of segment files, or it
    // will delete it. Empty segments get deleted, while nonempty segments get added.
    writer.advance();
    writer.advance();
    writer.advance();

    // Blocks until the last segment is deleted.
    std::move(writer).destroy();
    ASSERT_TRUE(set.segments().empty());

    std::vector<std::string> children;
    ASSERT_OK(storage->get_children(ROOT, children));
    ASSERT_TRUE(children.empty());
}

template<class Test>
static auto test_write_until_failure(Test &test) -> void
{
    Id last_lsn;
    while (test.error_buffer.is_ok()) {
        Span buffer {test.scratch};
        const auto size = test.random.template Next<Size>(1, buffer.size());
        test.writer.write(WalPayloadIn {{++last_lsn.value}, buffer.truncate(size)});
    }

    (void)std::move(test.writer).destroy();
    assert_special_error(test.error_buffer.get());
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

TEST_F(WalWriterTests, CleansUpAfterWriteFailure)
{
    Quick_Interceptor("test/wal", Tools::Interceptor::WRITE);
    // Segment will not be written to.
    test_write_until_failure(*this);
    ASSERT_EQ(count_segments(*this), 0);
}

TEST_F(WalWriterTests, LeavesSegmentAfterOpenFailure)
{
    Quick_Interceptor("test/wal", Tools::Interceptor::OPEN);
    // Fails when advancing to the next segment.
    test_write_until_failure(*this);
    ASSERT_EQ(count_segments(*this), 1);
}

TEST_F(WalWriterTests, LeavesSingleNonEmptySegmentAfterWriteFailure)
{
    int counter {WAL_LIMIT / 2};
    Counting_Interceptor("test/wal", Tools::Interceptor::WRITE, counter);
    test_write_until_failure(*this);
    ASSERT_EQ(count_segments(*this), 1);
}

TEST_F(WalWriterTests, LeavesMultipleNonEmptySegmentsAfterOpenFailure)
{
    int counter {10};
    Counting_Interceptor("test/wal", Tools::Interceptor::OPEN, counter);
    test_write_until_failure(*this);
    ASSERT_EQ(count_segments(*this), 11);
}

TEST_F(WalWriterTests, LeavesMultipleNonEmptySegmentsAfterWriteFailure)
{
    int counter {WAL_LIMIT * 10};
    Counting_Interceptor("test/wal", Tools::Interceptor::WRITE, counter);
    test_write_until_failure(*this);
    ASSERT_GT(count_segments(*this), 2);
}

class WalReaderWriterTests : public TestWithWalSegmentsOnHeap {
public:
    static constexpr Size PAGE_SIZE {0x100};
    static constexpr Size WAL_LIMIT {8};

    WalReaderWriterTests()
        : scratch(wal_scratch_size(PAGE_SIZE), '\x00'),
          error_buffer {std::make_unique<ErrorBuffer>()},
          reader_data(wal_scratch_size(PAGE_SIZE), '\x00'),
          reader_tail(wal_block_size(PAGE_SIZE), '\x00'),
          writer_tail(wal_block_size(PAGE_SIZE), '\x00'),
          writer {WalWriter::Parameters{
              "test/wal-",
              Span {writer_tail},
              storage.get(),
              error_buffer.get(),
              &set,
              &flushed_lsn,
              WAL_LIMIT,
          }}
    {}

    ~WalReaderWriterTests() override = default;

    [[nodiscard]]
    auto get_reader() -> std::unique_ptr<WalReader>
    {
        const auto param = WalReader::Parameters {
            "test/wal-",
            Span {reader_tail},
            Span {reader_data},
            storage.get(),
            &set,
        };
        WalReader *reader;
        EXPECT_OK(WalReader::open(param, &reader));
        return std::unique_ptr<WalReader> {reader};
    }

    [[nodiscard]]
    auto get_payload() -> WalPayloadIn
    {
        Span buffer {scratch};
        const auto size = random.Next<Size>(1, 32);
        payloads.emplace_back(random.Generate(size).to_string());
        mem_copy(buffer.range(sizeof(Lsn)), payloads.back());
        return WalPayloadIn {{++last_lsn.value}, buffer.truncate(size + sizeof(Lsn))};
    }

    auto emit_segments(Size num_writes)
    {
        for (Size i {}; i < num_writes; ++i) {
            writer.write(get_payload());
        }

        std::move(writer).destroy();
        return error_buffer->get();
    }

    [[nodiscard]]
    static auto contains_sequence(WalReader &reader, Id final_lsn) -> Status
    {
        auto s = Status::ok();
        // Roll forward to the end of the WAL.
        for (auto lsn = Lsn::root(); ; lsn.value++) {
            WalPayloadOut payload;
            s = reader.read(payload);
            if (s.is_not_found()) {
                if (lsn.value != final_lsn.value + 1) {
                    return Status::corruption("missing record");
                }
                return Status::ok();
            } else if (!s.is_ok()) {
                return s;
            }
            if (lsn != payload.lsn()) {
                return Status::corruption("missing record");
            }
        }
        return s;
    }

    [[nodiscard]]
    auto roll_segments_forward(WalReader &reader, Size write_count) -> Status
    {
        auto s = Status::ok();
        // Roll forward to the end of the WAL.
        for (Size found {}; s.is_ok(); ) {
            WalPayloadOut payload;
            s = reader.read(payload);
            if (s.is_not_found()) {
                if (found != write_count) {
                    return Status::corruption("missing records");
                }
                return Status::ok();
            } else if (!s.is_ok()) {
                return s;
            }
            EXPECT_EQ(payload.data().to_string(), payloads[payload.lsn().as_index()]);
            found++;
        }
        return s;
    }

    Id last_lsn;
    std::vector<std::string> payloads;
    WalSet set;
    std::unique_ptr<ErrorBuffer> error_buffer;
    std::string scratch;
    std::atomic<Id> flushed_lsn {};
    std::string reader_data;
    std::string reader_tail;
    std::string writer_tail;
    Tools::RandomGenerator random;
    WalRecordGenerator generator;
    WalWriter writer;
};

static auto does_not_lose_records_test(WalReaderWriterTests &test, Size num_writes)
{
    ASSERT_OK(test.emit_segments(num_writes));

    auto reader = test.get_reader();
    ASSERT_OK(test.contains_sequence(*reader, Id {num_writes}));
}

TEST_F(WalReaderWriterTests, IterateFromBeginning)
{
    ASSERT_OK(emit_segments(50));

    Reader *file;
    ASSERT_OK(storage->new_reader(encode_segment_name("test/wal-", Id::root()), &file));
    WalIterator itr {*file, reader_tail};

    for (auto lsn = Lsn::root(); ; lsn.value++) {
        Span payload {reader_data};
        auto s = itr.read(payload);
        if (s.is_not_found()) {
            break;
        }
        ASSERT_EQ(lsn, Id {get_u64(payload.data())});
        ASSERT_OK(s);
    }

    delete file;
}

TEST_F(WalReaderWriterTests, IterateFromMiddle)
{
    ASSERT_OK(emit_segments(5'000));

    Reader *file;
    ASSERT_OK(storage->new_reader(encode_segment_name("test/wal-", Id {2}), &file));
    WalIterator itr {*file, reader_tail};

    auto lsn = read_first_lsn(*storage, "test/wal-", Id {2}, set).value();
    for (; ; lsn.value++) {
        Span payload {reader_data};
        auto s = itr.read(payload);
        if (s.is_not_found()) {
            break;
        }
        ASSERT_EQ(lsn, Id {get_u64(payload.data())});
        ASSERT_OK(s);
    }
    delete file;
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
    ASSERT_OK(test.roll_segments_forward(*reader, num_writes));
}

TEST_F(WalReaderWriterTests, RollForwardWithinSegment)
{
    roll_forward_test(*this, 3);
}

TEST_F(WalReaderWriterTests, RollForwardAcrossSegments)
{
    roll_forward_test(*this, 5'000);
}

class WalCleanupTests : public WalReaderWriterTests {
public:
    WalCleanupTests()
        : cleanup {WalCleanup::Parameters{
              "test/wal-",
              &limit,
              storage.get(),
              &error_buffer,
              &set,
          }}
    {}

    ~WalCleanupTests() override = default;

    [[nodiscard]]
    auto collect_wal_segment_ids() -> std::vector<Id>
    {
        const auto in = set.segments();
        std::vector<Id> out(in.size());
        std::transform(begin(in), end(in), begin(out), [](auto x) {
            return x.first;
        });
        return out;
    }

    ErrorBuffer error_buffer;
    std::atomic<Lsn> limit;
    WalCleanup cleanup;
};

TEST_F(WalCleanupTests, DoesNothingWhenSetIsEmpty)
{
    ASSERT_TRUE(collect_wal_segment_ids().empty());
    cleanup.cleanup();
    ASSERT_TRUE(collect_wal_segment_ids().empty());
}

TEST_F(WalCleanupTests, RemovesObsoleteSegments)
{
    writer.write(get_payload());
    writer.write(get_payload());
    writer.write(get_payload());
    writer.advance();

    writer.write(get_payload());
    writer.write(get_payload());
    writer.write(get_payload());
    writer.advance();

    writer.write(get_payload());
    writer.write(get_payload());
    writer.write(get_payload());
    writer.advance();

    std::move(writer).destroy();
    ASSERT_EQ(set.segments().size(), 3);

    limit.store({3});
    cleanup.cleanup();
    ASSERT_EQ(set.segments().size(), 3);

    limit.store({4});
    cleanup.cleanup();
    ASSERT_EQ(set.segments().size(), 2);

    // Always keep the most-recent segment.
    limit.store({100});
    cleanup.cleanup();
    ASSERT_EQ(set.segments().size(), 1);
    ASSERT_EQ(set.first(), Id {3});
}

TEST_F(WalCleanupTests, ReportsErrorOnLsnRead)
{
    writer.write(get_payload());
    writer.advance();

    writer.write(get_payload());
    writer.advance();

    std::move(writer).destroy();
    limit.store({3});

    Quick_Interceptor("test/wal", Tools::Interceptor::READ);
    cleanup.cleanup();

    assert_special_error(error_buffer.get());
}

TEST_F(WalCleanupTests, ReportsErrorOnUnlink)
{
    writer.write(get_payload());
    writer.advance();

    writer.write(get_payload());
    writer.advance();

    std::move(writer).destroy();
    limit.store({3});

    Quick_Interceptor("test/wal", Tools::Interceptor::UNLINK);
    cleanup.cleanup();

    assert_special_error(error_buffer.get());
}

//class BasicWalTests: public TestWithWalSegmentsOnHeap {
//public:
//    static constexpr Size PAGE_SIZE {0x100};
//
//    ~BasicWalTests() override = default;
//
//    BasicWalTests()
//        : scratch(wal_scratch_size(PAGE_SIZE), '\x00')
//    {}
//
//    auto SetUp() -> void override
//    {
//        auto r = WriteAheadLog::open({
//            "test/wal-",
//            storage.get(),
//            PAGE_SIZE,
//            32,
//        });
//        ASSERT_TRUE(r.has_value()) << r.error().what().data();
//        wal = std::move(*r);
//    }
//
//    auto initialize() -> void
//    {
//        // Initialize the WAL with a few records. This is to simulate new database setup.
//        run_operations({
//            WalOperation::LOG,
//            WalOperation::LOG,
//            WalOperation::COMMIT,
//            WalOperation::ADVANCE,
//        });
//    }
//
//    [[nodiscard]]
//    auto get_data_payload(const std::string &data) -> WalPayloadIn
//    {
//        Span buffer {scratch};
//        buffer.truncate(sizeof(Lsn) + 1 + data.size());
//        payloads.emplace_back("p" + data);
//        mem_copy(buffer.range(sizeof(Lsn)), payloads.back());
//        payloads_since_commit++;
//        return WalPayloadIn {wal->current_lsn(), buffer};
//    }
//
//    [[nodiscard]]
//    auto get_random_data_payload() -> WalPayloadIn
//    {
//        const auto max_size = wal_scratch_size(PAGE_SIZE) - WalPayloadHeader::SIZE - 1;
//        const auto size = random.Next<Size>(1, max_size);
//        return get_data_payload(random.Generate(size).to_string());
//    }
//
//    [[nodiscard]]
//    auto get_commit_payload() -> WalPayloadIn
//    {
//        Span buffer {scratch};
//        buffer.truncate(sizeof(Lsn) + 1);
//        payloads_since_commit = 0;
//        payloads.emplace_back("c");
//        buffer.data()[sizeof(Lsn)] = 'c';
//        return WalPayloadIn {wal->current_lsn(), buffer};
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
//            return Status::ok();
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
//            return Status::ok();
//        }));
//        if (strict) {
//            ASSERT_EQ(lsns.size(), payloads_since_commit);
//        }
//        std::sort(begin(lsns), end(lsns));
//        Id lsn_counter {commit_lsn};
//        for (const auto &lsn: lsns) {
//            ASSERT_EQ(++lsn_counter.value, lsn.value);
//        }
//    }
//
//    enum class WalOperation: int {
//        FLUSH = 1,
//        ADVANCE = 2,
//        COMMIT = 3,
//        LOG = 4,
//    };
//
//    auto run_operations(const std::vector<WalOperation> &operations, bool keep_clean = false) -> Status
//    {
//        for (auto operation: operations) {
//            switch (operation) {
//                case WalOperation::FLUSH:
//                    (void)wal->flush();
//                    break;
//                case WalOperation::ADVANCE:
//                    (void)wal->advance();
//                    break;
//                case WalOperation::COMMIT: {
//                    const auto payload = get_commit_payload();
//                    const auto lsn = payload.lsn();
//                    wal->log(payload);
//                    (void)wal->advance();
//                    commit_lsn = lsn;
//                    break;
//                }
//                case WalOperation::LOG:
//                    wal->log(get_random_data_payload());
//            }
//            if (keep_clean) {
//                wal->cleanup(commit_lsn);
//            }
//            if (!wal->status().is_ok()) {
//                break;
//            }
//        }
//        return wal->status();
//    }
//
//    Tools::RandomGenerator random;
//    Size payloads_since_commit {};
//    Id commit_lsn;
//    std::string scratch;
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
//    ASSERT_TRUE(wal->roll_forward(Id::null(), [](auto) {return Status::ok();}).is_corruption());
//}
//
//TEST_F(BasicWalTests, FlushesWithEmptyTailBuffer)
//{
//    run_operations({WalOperation::FLUSH});
//}
//
//TEST_F(BasicWalTests, AdvancesWithEmptyTailBuffer)
//{
//    run_operations({WalOperation::ADVANCE});
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
//        const auto r = test.random.template Next<Size>(20);
//        if (r >= 2 || operations.empty() || operations.back() != BasicWalTests::WalOperation::LOG) {
//            operations.emplace_back(BasicWalTests::WalOperation::LOG);
//        } else {
//            if (r == 0) {
//                operations.emplace_back(BasicWalTests::WalOperation::FLUSH);
//            } else {
//                operations.emplace_back(BasicWalTests::WalOperation::ADVANCE);
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
//    initialize();
//    run_operations(generate_transaction(*this, 1'000));
//
//    roll_forward(false);
//    roll_backward(false);
//}
//
//TEST_F(BasicWalTests, SanityCheckSingleTransactionWithCommit)
//{
//    initialize();
//    run_operations(generate_transaction(*this, 1'000, true));
//
//    roll_forward();
//    roll_backward();
//}
//
//TEST_F(BasicWalTests, SanityCheckMultipleTransactions)
//{
//    initialize();
//    for (Size i {}; i < 10; ++i) {
//        run_operations(generate_transaction(*this, 1'000, i == 3 || i == 6));
//    }
//
//    roll_forward(false);
//    roll_backward(false);
//}
//
//TEST_F(BasicWalTests, SanityCheckMultipleTransactionsWithCommit)
//{
//    initialize();
//    for (Size i {}; i < 10; ++i) {
//        run_operations(generate_transaction(*this, 1'000, true));
//    }
//
//    roll_forward();
//    roll_backward();
//}

//class WalFaultTests: public BasicWalTests {
//
//};
//
//TEST_F(WalFaultTests, FailOnFirstWrite)
//{
//    Quick_Interceptor("test/wal", Tools::Interceptor::WRITE);
//    assert_special_error(run_operations({WalOperation::LOG, WalOperation::FLUSH}));
//
//    // We never wrote anything, so the writer should have removed the segment.
//    ASSERT_TRUE(wal->roll_forward(Id::null(), [](auto) {
//        return Status::ok();
//    }).is_corruption());
//    ASSERT_TRUE(wal->roll_backward(Id::null(), [](auto) {
//        return Status::ok();
//    }).is_corruption());
//}
//
//TEST_F(WalFaultTests, FailOnFirstOpen)
//{
//    Quick_Interceptor("test/wal", Tools::Interceptor::OPEN);
//    assert_special_error(run_operations({WalOperation::LOG, WalOperation::ADVANCE}));
//    Clear_Interceptors();
//    SetUp();
//
//    roll_forward(false);
//    // Hits the beginning of the WAL without finding a commit.
//    ASSERT_TRUE(wal->roll_backward(Id::null(), [](auto) {
//        return Status::ok();
//    }).is_corruption());
//}
//
//TEST_F(WalFaultTests, FailOnNthOpen)
//{
//    initialize();
//    std::vector<WalOperation> ops(5'000, WalOperation::LOG);
//    ops.emplace_back(WalOperation::COMMIT);
//    run_operations(ops);
//
//    int counter {10};
//    Counting_Interceptor("test/wal", Tools::Interceptor::OPEN, counter);
//    assert_special_error(run_operations(ops));
//    Clear_Interceptors();
//    SetUp();
//
//    // We should have full records in the WAL, so these tests will work.
//    roll_forward(false);
//    roll_backward(false);
//}
//
//TEST_F(WalFaultTests, FailOnNthWrite)
//{
//    std::vector<WalOperation> ops(5'000, WalOperation::LOG);
//    ops.emplace_back(WalOperation::COMMIT);
//    run_operations(ops);
//
//    int counter {100};
//    Counting_Interceptor("test/wal", Tools::Interceptor::WRITE, counter);
//    assert_special_error(run_operations(ops));
//    Clear_Interceptors();
//    SetUp();
//
//    // We may have a partial record at the end. The WAL will stop short of it.
//    roll_forward(false);
//    roll_backward(false);
//}
//
//TEST_F(WalFaultTests, FailsIfMissingSegment)
//{
//    std::vector<WalOperation> ops(5'000, WalOperation::LOG);
//    ops.emplace_back(WalOperation::COMMIT);
//    run_operations(ops);
//
//    ASSERT_OK(storage->remove_file("test/wal-1"));
//
//    ASSERT_FALSE(wal->roll_forward(Id::root(), [](auto) {
//        return Status::ok();
//    }).is_ok());
//    ASSERT_FALSE(wal->roll_backward(Id::root(), [](auto) {
//        return Status::ok();
//    }).is_ok());
//}

} // <anonymous>
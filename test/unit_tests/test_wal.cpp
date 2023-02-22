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
        EXPECT_TRUE(expose_message(Base::storage->new_reader_(get_segment_name(id), &reader)));

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
              WAL_LIMIT,
          }}
    {}

    ~WalWriterTests() override = default;

    WalSet set;
    ErrorBuffer error_buffer;
    std::string scratch;
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
          writer_data(wal_scratch_size(PAGE_SIZE), '\x00'),
          reader_data(wal_scratch_size(PAGE_SIZE), '\x00'),
          reader_tail(wal_block_size(PAGE_SIZE), '\x00'),
          writer_tail(wal_block_size(PAGE_SIZE), '\x00'),
          writer {WalWriter::Parameters{
              "test/wal-",
              Span {writer_tail},
              storage.get(),
              error_buffer.get(),
              &set,
              WAL_LIMIT,
          }}
    {}

    ~WalReaderWriterTests() override = default;

    [[nodiscard]]
    auto get_payload() -> WalPayloadIn
    {
        Span buffer {scratch};
        const auto size = random.Next<Size>(1, 32);
        payloads.emplace_back(random.Generate(size).to_string());
        mem_copy(buffer.range(sizeof(Lsn)), payloads.back());
        return WalPayloadIn {{++last_lsn.value}, buffer.truncate(size + sizeof(Lsn))};
    }

    auto random_writes(Size num_writes)
    {
        for (Size i {}; i < num_writes; ++i) {
            writer.write(get_payload());
        }

        std::move(writer).destroy();
        return error_buffer->get();
    }

    [[nodiscard]]
    auto get_reader(Id id) -> WalReader
    {
        Reader *reader;
        EXPECT_OK(storage->new_reader(encode_segment_name("test/wal-", id), &reader));
        reader_file.reset(reader);
        return WalReader {*reader_file, reader_tail};
    }

    [[nodiscard]]
    auto read_record(WalReader &reader) -> WalPayloadOut
    {
        // Only supports reading 1 record at a time.
        Span buffer {reader_data};
        EXPECT_OK(reader.read(buffer));
        return WalPayloadOut {buffer};
    }

    auto write_record(LogWriter &writer_, Lsn lsn, const std::string &payload) -> void
    {
        Span buffer {writer_data};
        mem_copy(buffer.range(sizeof(Lsn)), payload);
        buffer.truncate(sizeof(Lsn) + payload.size());
        ASSERT_OK(writer_.write(WalPayloadIn {lsn, buffer}));
    }

    [[nodiscard]]
    auto get_writer(Id id, Size file_size) -> LogWriter
    {
        Logger *logger;
        EXPECT_OK(storage->new_logger(encode_segment_name("test/wal-", id), &logger));
        writer_file.reset(logger);
        return LogWriter {*writer_file, writer_tail, file_size};
    }

    Id last_lsn;
    std::vector<std::string> payloads;
    WalSet set;
    std::unique_ptr<ErrorBuffer> error_buffer;
    std::unique_ptr<Reader> reader_file;
    std::unique_ptr<Logger> writer_file;
    std::string scratch;
    Lsn flushed_lsn;
    std::string reader_data;
    std::string writer_data;
    std::string reader_tail;
    std::string writer_tail;
    Tools::RandomGenerator random;
    WalRecordGenerator generator;
    WalWriter writer;
};

TEST_F(WalReaderWriterTests, ReadsRecordsInBlock)
{
    auto writer = get_writer(Id {1}, 0);
    write_record(writer, Lsn {1}, "1");
    write_record(writer, Lsn {2}, "22");
    write_record(writer, Lsn {3}, "333");
    ASSERT_OK(writer.flush());

    auto reader = get_reader(Id {1});
    auto payload = read_record(reader);
    ASSERT_EQ(payload.lsn(), Lsn {1});
    ASSERT_EQ(payload.data(), "1");
    payload = read_record(reader);
    ASSERT_EQ(payload.lsn(), Lsn {2});
    ASSERT_EQ(payload.data(), "22");
    payload = read_record(reader);
    ASSERT_EQ(payload.lsn(), Lsn {3});
    ASSERT_EQ(payload.data(), "333");
}

static auto setup_scenario(WalReaderWriterTests &test)
{
    std::vector<std::string> data;
    data.emplace_back(test.random.Generate(WalReaderWriterTests::PAGE_SIZE).to_string());
    data.emplace_back(test.random.Generate(WalReaderWriterTests::PAGE_SIZE).to_string());
    data.emplace_back(test.random.Generate(WalReaderWriterTests::PAGE_SIZE).to_string());
    data.emplace_back(test.random.Generate(10).to_string());
    data.emplace_back(test.random.Generate(10).to_string());

    auto writer = test.get_writer(Id {1}, 0);
    test.write_record(writer, Lsn {1}, data[0]);
    test.write_record(writer, Lsn {2}, data[1]);
    EXPECT_OK(writer.flush());
    test.write_record(writer, Lsn {3}, data[2]);
    test.write_record(writer, Lsn {4}, data[3]);
    EXPECT_OK(writer.flush());
    test.write_record(writer, Lsn {5}, data[4]);
    EXPECT_OK(writer.flush());

    return data;
}

TEST_F(WalReaderWriterTests, ReadsRecordsBetweenBlocks)
{
    const auto data = setup_scenario(*this);

    auto reader = get_reader(Id {1});
    for (Size i {}; i < data.size(); ++i) {
        auto payload = read_record(reader);
        ASSERT_EQ(payload.lsn(), Lsn {i + 1});
        ASSERT_EQ(payload.data(), data[i]);
    }
}

TEST_F(WalReaderWriterTests, HandlesFlushes)
{
    auto writer = get_writer(Id {1}, 0);
    write_record(writer, Lsn {1}, "hello");
    ASSERT_OK(writer.flush());
    write_record(writer, Lsn {2}, "world");
    ASSERT_OK(writer.flush());

    auto reader = get_reader(Id {1});
    auto payload_1 = read_record(reader);
    ASSERT_EQ(payload_1.lsn(), Lsn {1});
    ASSERT_EQ(payload_1.data(), "hello");
    auto payload_2 = read_record(reader);
    ASSERT_EQ(payload_2.lsn(), Lsn {2});
    ASSERT_EQ(payload_2.data(), "world");
}

TEST_F(WalReaderWriterTests, HandlesCorruptedRecord)
{
    auto writer = get_writer(Id {1}, 0);
    write_record(writer, Lsn {1}, "hello");
    ASSERT_OK(writer.flush());

    // Corrupt the payload.
    storage_handle().memory()["test/wal-1"].buffer[WalRecordHeader::SIZE]++;

    Span payload {reader_data};
    auto reader = get_reader(Id {1});
    ASSERT_TRUE(reader.read(payload).is_corruption());
}

TEST_F(WalReaderWriterTests, IterateFromBeginning)
{
    ASSERT_OK(random_writes(50));

    Reader *file;
    ASSERT_OK(storage->new_reader(encode_segment_name("test/wal-", Id::root()), &file));
    WalReader itr {*file, reader_tail};

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
    ASSERT_OK(random_writes(5'000));

    Reader *file;
    ASSERT_OK(storage->new_reader(encode_segment_name("test/wal-", Id {2}), &file));
    WalReader itr {*file, reader_tail};

    Lsn lsn;
    ASSERT_OK(read_first_lsn(*storage, "test/wal-", Id {2}, set, lsn));
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

class WalCleanupTests : public WalReaderWriterTests {
public:
    WalCleanupTests()
        : cleanup {WalCleanup::Parameters{
              "test/wal-",
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
    WalCleanup cleanup;
};

TEST_F(WalCleanupTests, DoesNothingWhenSetIsEmpty)
{
    ASSERT_TRUE(collect_wal_segment_ids().empty());
    cleanup.cleanup(Lsn {123});
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

    cleanup.cleanup(Lsn {3});
    ASSERT_EQ(set.segments().size(), 3);

    cleanup.cleanup(Lsn {4});
    ASSERT_EQ(set.segments().size(), 2);

    // Always keep the most-recent segment. TODO: No longer important
    cleanup.cleanup(Lsn {100});
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

    Quick_Interceptor("test/wal", Tools::Interceptor::READ);
    cleanup.cleanup(Lsn {3});

    assert_special_error(error_buffer.get());
}

TEST_F(WalCleanupTests, ReportsErrorOnUnlink)
{
    writer.write(get_payload());
    writer.advance();

    writer.write(get_payload());
    writer.advance();

    std::move(writer).destroy();

    Quick_Interceptor("test/wal", Tools::Interceptor::UNLINK);
    cleanup.cleanup(Lsn {3});

    assert_special_error(error_buffer.get());
}

} // <anonymous>
#include "calicodb/env.h"
#include "calicodb/slice.h"
#include "crc.h"
#include "tools.h"
#include "unit_tests.h"
#include "wal_reader.h"
#include "wal_writer.h"
#include <array>
#include <gtest/gtest.h>

namespace calicodb
{

TEST(SegmentNameParserTests, MatchesOnPrefix)
{
    ASSERT_EQ(decode_segment_name("./prefix-", "./prefix-1"), Id {1});
    ASSERT_TRUE(decode_segment_name("./prefix_", "./prefix-1").is_null());
}

TEST(SegmentNameParserTests, SegmentIdMustBeADecimalNumber)
{
    ASSERT_TRUE(decode_segment_name("./prefix-", "./prefix-a").is_null());
    ASSERT_TRUE(decode_segment_name("./prefix-", "./prefix-z").is_null());
    ASSERT_TRUE(decode_segment_name("./prefix-", "./prefix-").is_null());
}

namespace fs = std::filesystem;

class WalRecordMergeTests : public testing::Test
{
public:
    auto setup(const std::array<WalRecordType, 3> &types) -> void
    {
        lhs.type = types[0];
        rhs.type = types[1];
        lhs.size = 1;
        rhs.size = 2;
    }

    auto check(const WalRecordHeader &header, WalRecordType type) -> bool
    {
        return header.type == type && header.size == 3;
    }

    std::vector<std::array<WalRecordType, 3>> valid_left_merges {
        std::array<WalRecordType, 3> {WalRecordType {}, kFirstRecord, kFirstRecord},
        std::array<WalRecordType, 3> {WalRecordType {}, kFullRecord, kFullRecord},
        std::array<WalRecordType, 3> {kFirstRecord, kMiddleRecord, kFirstRecord},
        std::array<WalRecordType, 3> {kFirstRecord, kLastRecord, kFullRecord},
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
    setup({kFirstRecord, kFirstRecord});
    ASSERT_TRUE(merge_records_left(lhs, rhs).is_corruption());

    setup({WalRecordType {}, kMiddleRecord});
    ASSERT_TRUE(merge_records_left(lhs, rhs).is_corruption());

    setup({kMiddleRecord, kFirstRecord});
    ASSERT_TRUE(merge_records_left(lhs, rhs).is_corruption());
}

class WalRecordGenerator
{
public:
    [[nodiscard]] auto setup_deltas(Span image) -> std::vector<PageDelta>
    {
        static constexpr std::size_t MAX_WIDTH {30};
        static constexpr std::size_t MAX_SPREAD {20};
        std::vector<PageDelta> deltas;

        for (auto offset = random.Next<std::size_t>(image.size() / 10); offset < image.size();) {
            const auto rest = image.size() - offset;
            const auto size = random.Next<std::size_t>(1, std::min(rest, MAX_WIDTH));
            deltas.emplace_back(PageDelta {offset, size});
            offset += size + random.Next<std::size_t>(1, MAX_SPREAD);
        }
        for (const auto &[offset, size] : deltas) {
            const auto replacement = random.Generate(size);
            mem_copy(image.range(offset, size), replacement);
        }
        return deltas;
    }

private:
    tools::RandomGenerator random;
};

class WalPayloadTests : public testing::Test
{
public:
    static constexpr std::size_t kPageSize {0x80};

    WalPayloadTests()
        : image {random.Generate(kPageSize).to_string()},
          scratch(wal_scratch_size(kPageSize), '\x00')
    {
    }

    tools::RandomGenerator random;
    std::string image;
    std::string scratch;
};

TEST_F(WalPayloadTests, ImagePayloadEncoding)
{
    const auto payload_in = encode_image_payload(Lsn {123}, Id {456}, image, scratch.data());
    const auto payload_out = decode_payload(Span {scratch}.truncate(payload_in.size()));
    ASSERT_TRUE(std::holds_alternative<ImageDescriptor>(payload_out));
    const auto descriptor = std::get<ImageDescriptor>(payload_out);
    ASSERT_EQ(descriptor.lsn.value, 123);
    ASSERT_EQ(descriptor.page_id.value, 456);
    ASSERT_EQ(descriptor.image.to_string(), image);
}

TEST_F(WalPayloadTests, DeltaPayloadEncoding)
{
    WalRecordGenerator generator;
    auto deltas = generator.setup_deltas(image);
    const auto payload_in = encode_deltas_payload(Lsn {123}, Id {456}, image, deltas, scratch.data());
    const auto payload_out = decode_payload(Span {scratch}.truncate(payload_in.size()));
    ASSERT_TRUE(std::holds_alternative<DeltaDescriptor>(payload_out));
    const auto descriptor = std::get<DeltaDescriptor>(payload_out);
    ASSERT_EQ(descriptor.lsn.value, 123);
    ASSERT_EQ(descriptor.page_id.value, 456);
    ASSERT_EQ(descriptor.deltas.size(), deltas.size());
    ASSERT_TRUE(std::all_of(cbegin(descriptor.deltas), cend(descriptor.deltas), [this](const auto &delta) {
        return delta.data == Slice {image}.range(delta.offset, delta.data.size());
    }));
}

TEST_F(WalPayloadTests, VacuumPayloadEncoding)
{
    WalRecordGenerator generator;
    const auto payload_in = encode_vacuum_payload(Lsn {123}, true, scratch.data());
    const auto payload_out = decode_payload(Span {scratch}.truncate(payload_in.size()));
    ASSERT_TRUE(std::holds_alternative<VacuumDescriptor>(payload_out));
    const auto descriptor = std::get<VacuumDescriptor>(payload_out);
    ASSERT_EQ(descriptor.lsn.value, 123);
    ASSERT_TRUE(descriptor.is_start);
}

[[nodiscard]] auto get_ids(const WalSet &c)
{
    std::vector<Id> ids;
    for (Id id; ; ) {
        id = c.id_after(id);
        if (id.is_null()) {
            break;
        }
        ids.emplace_back(id);
    }
    return ids;
}

class WalSetTests : public testing::Test
{
public:
    auto add_segments(std::size_t n)
    {
        for (std::size_t i {}; i < n; ++i) {
            auto id = Id::from_index(i);
            set.add_segment(id);
        }
        ASSERT_EQ(set.last(), Id::from_index(n - 1));
    }

    WalSet set;
};

TEST_F(WalSetTests, NullMarksEnd)
{
    ASSERT_TRUE(set.id_before(Id::null()).is_null());
    ASSERT_TRUE(set.id_after(Id::null()).is_null());
}

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

template <class Itr>
[[nodiscard]] auto contains_n_consecutive_segments(const Itr &begin, const Itr &end, Id id, std::size_t n)
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

class WalComponentTests
    : public InMemoryTest,
      public testing::Test
{
public:
    static constexpr std::size_t kPageSize {0x200};
    const std::string kWalPrefix {"test-wal-"};

    WalComponentTests()
        : m_writer_tail(wal_block_size(kPageSize), '\0'),
          m_reader_tail(wal_block_size(kPageSize), '\0'),
          m_reader_data(wal_block_size(kPageSize), '\0')
    {
    }

    ~WalComponentTests() override
    {
        delete m_reader_file;
        delete m_writer_file;
    }

    static auto assert_reader_is_done(WalReader &reader) -> void
    {
        std::string _;
        ASSERT_TRUE(wal_read_with_status(reader, _).is_not_found());
        ASSERT_TRUE(wal_read_with_status(reader, _).is_not_found());
    }

    [[nodiscard]] auto make_reader(Id id) -> WalReader
    {
        EXPECT_OK(env->new_reader(encode_segment_name(kWalPrefix, id), &m_reader_file));
        return WalReader {*m_reader_file, m_reader_tail};
    }

    [[nodiscard]] auto make_writer(Id id) -> WalWriter
    {
        EXPECT_OK(env->new_logger(encode_segment_name(kWalPrefix, id), &m_writer_file));
        return WalWriter {*m_writer_file, m_writer_tail};
    }

    [[nodiscard]] static auto wal_write(WalWriter &writer, Lsn lsn, const Slice &data) -> Status
    {
        std::string buffer(sizeof(lsn), '\0');
        put_u64(buffer.data(), lsn.value);
        buffer.append(data.to_string());
        return writer.write(lsn, buffer);
    }

    [[nodiscard]] static auto wal_read_with_status(WalReader &reader, std::string &out, Lsn *lsn = nullptr) -> Status
    {
        out.resize(wal_scratch_size(kPageSize));
        Span buffer {out};

        CDB_TRY(reader.read(buffer));
        if (lsn != nullptr) {
            *lsn = extract_payload_lsn(buffer);
        }
        out = buffer.advance(sizeof(Lsn)).to_string();
        return Status::ok();
    }

    [[nodiscard]] static auto wal_read(WalReader &reader, Lsn *lsn = nullptr) -> std::string
    {
        std::string out;
        EXPECT_OK(wal_read_with_status(reader, out, lsn));
        return out;
    }

private:
    std::string m_writer_tail;
    std::string m_reader_tail;
    std::string m_reader_data;
    Reader *m_reader_file {};
    Logger *m_writer_file {};
};

TEST_F(WalComponentTests, ManualFlush)
{
    auto writer = make_writer(Id::root());
    ASSERT_EQ(writer.flushed_lsn(), Lsn::null());
    ASSERT_OK(wal_write(writer, Lsn {1}, "hello"));
    ASSERT_OK(wal_write(writer, Lsn {2}, "world"));
    ASSERT_EQ(writer.flushed_lsn(), Lsn::null());
    ASSERT_OK(writer.flush());
    ASSERT_EQ(writer.flushed_lsn(), Lsn {2});
}

TEST_F(WalComponentTests, AutomaticFlush)
{
    auto writer = make_writer(Id::root());

    auto lsn = Lsn::root();
    for (; lsn.value < kPageSize * 5; ++lsn.value) {
        ASSERT_OK(wal_write(writer, lsn, "=^.^="));
    }
    ASSERT_GT(writer.flushed_lsn(), Lsn::null());
    ASSERT_LE(writer.flushed_lsn(), lsn);
}

TEST_F(WalComponentTests, HandlesRecordsWithinBlock)
{
    auto writer = make_writer(Id::root());
    ASSERT_OK(wal_write(writer, Lsn {1}, "hello"));
    ASSERT_OK(wal_write(writer, Lsn {2}, "world"));
    ASSERT_OK(writer.flush());

    auto reader = make_reader(Id::root());
    ASSERT_EQ(wal_read(reader), "hello");
    ASSERT_EQ(wal_read(reader), "world");
    assert_reader_is_done(reader);
}

TEST_F(WalComponentTests, HandlesRecordsAcrossPackedBlocks)
{
    auto writer = make_writer(Id::root());
    for (std::size_t i {1}; i < kPageSize * 2; ++i) {
        ASSERT_OK(wal_write(writer, Lsn {i}, tools::integral_key(i)));
    }
    ASSERT_OK(writer.flush());
    auto reader = make_reader(Id::root());
    for (std::size_t i {1}; i < kPageSize * 2; ++i) {
        ASSERT_EQ(wal_read(reader), tools::integral_key(i));
    }
    assert_reader_is_done(reader);
}

TEST_F(WalComponentTests, ReaderReportsMismatchedCrc)
{
    auto writer = make_writer(Id::root());
    ASSERT_OK(wal_write(writer, Lsn {1}, "./test"));
    ASSERT_OK(writer.flush());

    Editor *editor;
    ASSERT_OK(env->new_editor(encode_segment_name(kWalPrefix, Id::root()), &editor));
    ASSERT_OK(editor->write("TEST", WalRecordHeader::kSize + sizeof(Lsn)));
    delete editor;

    std::string buffer;
    auto reader = make_reader(Id::root());
    ASSERT_TRUE(wal_read_with_status(reader, buffer).is_corruption());
}

TEST_F(WalComponentTests, ReaderReportsEmptyFile)
{
    Editor *editor;
    ASSERT_OK(env->new_editor(encode_segment_name(kWalPrefix, Id::root()), &editor));
    delete editor;

    std::string buffer;
    auto reader = make_reader(Id::root());
    ASSERT_TRUE(wal_read_with_status(reader, buffer).is_not_found());
}

TEST_F(WalComponentTests, ReaderReportsIncompleteBlock)
{
    Editor *editor;
    ASSERT_OK(env->new_editor(encode_segment_name(kWalPrefix, Id::root()), &editor));
    ASSERT_OK(editor->write("\x01\x02\x03", 0));
    delete editor;

    std::string buffer(wal_scratch_size(kPageSize), '\0');
    auto reader = make_reader(Id::root());
    ASSERT_TRUE(wal_read_with_status(reader, buffer).is_corruption());
}

TEST_F(WalComponentTests, ReaderReportsInvalidSize)
{
    auto writer = make_writer(Id::root());
    ASSERT_OK(wal_write(writer, Lsn {1}, "./test"));
    ASSERT_OK(writer.flush());

    WalRecordHeader header;
    header.type = kFullRecord;
    header.size = -1;
    std::string buffer(WalRecordHeader::kSize, '\0');
    write_wal_record_header(buffer, header);

    Editor *editor;
    ASSERT_OK(env->new_editor(encode_segment_name(kWalPrefix, Id::root()), &editor));
    ASSERT_OK(editor->write(buffer, 0));
    delete editor;

    auto reader = make_reader(Id::root());
    ASSERT_TRUE(wal_read_with_status(reader, buffer).is_corruption());
}

TEST_F(WalComponentTests, ReadsFirstLsn)
{
    auto writer = make_writer(Id::root());
    ASSERT_OK(wal_write(writer, Lsn {42}, "./test"));
    ASSERT_OK(writer.flush());

    WalSet set;
    set.add_segment(Id::root());

    Lsn first_lsn;
    ASSERT_OK(read_first_lsn(*env, kWalPrefix, Id::root(), set, first_lsn));
    ASSERT_EQ(first_lsn, Lsn {42});
    ASSERT_EQ(set.first_lsn(Id::root()), Lsn {42});
}

TEST_F(WalComponentTests, FailureToReadFirstLsn)
{
    WalSet set;
    set.add_segment(Id::root());

    // File does not exist in env, so the reader can't be opened.
    Lsn first_lsn;
    ASSERT_TRUE(read_first_lsn(*env, kWalPrefix, Id::root(), set, first_lsn).is_not_found());

    // File exists, but is empty.
    Logger *logger;
    ASSERT_OK(env->new_logger(encode_segment_name(kWalPrefix, Id::root()), &logger));
    ASSERT_TRUE(read_first_lsn(*env, kWalPrefix, Id::root(), set, first_lsn).is_corruption());

    // File is too small to read the LSN.
    std::string buffer(WalRecordHeader::kSize + 3, '\0');
    ASSERT_OK(logger->write(buffer));
    ASSERT_TRUE(read_first_lsn(*env, kWalPrefix, Id::root(), set, first_lsn).is_corruption());

    // LSN is NULL.
    buffer.resize(wal_block_size(kPageSize) - buffer.size());
    ASSERT_OK(logger->write(buffer));
    ASSERT_TRUE(read_first_lsn(*env, kWalPrefix, Id::root(), set, first_lsn).is_corruption());

    delete logger;
}

TEST_F(WalComponentTests, PrefersToGetLsnFromCache)
{
    WalSet set;
    set.add_segment(Id::root());
    set.set_first_lsn(Id::root(), Lsn {42});

    // File doesn't exist, but the LSN is cached.
    Lsn first_lsn;
    ASSERT_OK(read_first_lsn(*env, kWalPrefix, Id::root(), set, first_lsn));
    ASSERT_EQ(first_lsn, Lsn {42});
}

TEST_F(WalComponentTests, HandlesRecordsAcrossSparseBlocks)
{
    auto writer = make_writer(Id::root());
    for (std::size_t i {1}; i < kPageSize * 2; ++i) {
        ASSERT_OK(wal_write(writer, Lsn {i}, tools::integral_key(i)));
        if (rand() % 8 == 0) {
            ASSERT_OK(writer.flush());
        }
    }
    ASSERT_OK(writer.flush());
    auto reader = make_reader(Id::root());
    for (std::size_t i {1}; i < kPageSize * 2; ++i) {
        ASSERT_EQ(wal_read(reader), tools::integral_key(i));
    }
    assert_reader_is_done(reader);
}

TEST_F(WalComponentTests, Corruption)
{
    // Don't flush the writer, so it leaves a partial record in the WAL.
    auto writer = make_writer(Id::root());
    for (std::size_t i {1}; i < kPageSize * 2; ++i) {
        ASSERT_OK(wal_write(writer, Lsn {i}, tools::integral_key(i)));
    }
    ASSERT_LT(writer.flushed_lsn(), Lsn {kPageSize * 2 - 1});

    auto reader = make_reader(Id::root());
    for (std::size_t i {1}; i < kPageSize * 2; ++i) {
        std::string data;
        auto s = wal_read_with_status(reader, data);
        if (s.is_corruption()) {
            break;
        }
        ASSERT_OK(s);
        ASSERT_EQ(data, tools::integral_key(i));
    }
    assert_reader_is_done(reader);
}

class WalTests
    : public InMemoryTest,
      public testing::Test
{
public:
    static constexpr auto kWalPrefix = "./wal-";
    static constexpr auto kPageSize = kMinPageSize;

    auto SetUp() -> void override
    {
        WriteAheadLog *temp;
        WriteAheadLog::Parameters param {
            kWalPrefix,
            env.get(),
            kPageSize};
        ASSERT_OK(WriteAheadLog::open(param, &temp));
        wal.reset(temp);

        tail_buffer.resize(wal_block_size(kPageSize));
        payload_buffer.resize(wal_scratch_size(kPageSize));
    }

    auto read_segment(Id segment_id, std::vector<std::string> *out) -> Status
    {
        Reader *temp;
        EXPECT_OK(env->new_reader(encode_segment_name(kWalPrefix, segment_id), &temp));

        std::unique_ptr<Reader> file {temp};
        WalReader reader {*file, tail_buffer};

        for (; ; ) {
            Span payload {payload_buffer};
            auto s = reader.read(payload);

            if (s.is_ok()) {
                out->emplace_back(payload.to_string());
            } else if (s.is_not_found()) {
                break;
            } else {
                return s;
            }
        }
        return Status::ok();
    }

    std::string payload_buffer;
    std::string tail_buffer;
    std::unique_ptr<WriteAheadLog> wal;
    tools::RandomGenerator random;
};

TEST_F(WalTests, SequenceNumbersAreMonotonicallyIncreasing)
{
    ASSERT_OK(wal->start_writing());
    Lsn lsn;
    ASSERT_OK(wal->log_image(Id::root(), "a", &lsn));
    ASSERT_EQ(lsn, Lsn {1});
    ASSERT_OK(wal->log_image(Id::root(), "b", &lsn));
    ASSERT_EQ(lsn, Lsn {2});
    ASSERT_OK(wal->log_image(Id::root(), "c", &lsn));
    ASSERT_EQ(lsn, Lsn {3});
}

TEST_F(WalTests, UnderstandsImageRecords)
{
    ASSERT_OK(wal->start_writing());
    ASSERT_EQ(wal->bytes_written(), 0);
    const auto image = random.Generate(kPageSize);
    ASSERT_OK(wal->log_image(Id {10}, "", nullptr));
    ASSERT_OK(wal->log_image(Id {20}, image, nullptr));
    ASSERT_OK(wal->flush());

    std::vector<std::string> payloads;
    ASSERT_OK(read_segment(Id {1}, &payloads));
    ASSERT_EQ(payloads.size(), 2);

    auto payload = decode_payload(payloads[0]);
    ASSERT_TRUE(std::holds_alternative<ImageDescriptor>(payload));
    auto descriptor = std::get<ImageDescriptor>(payload);
    ASSERT_EQ(descriptor.lsn, Lsn {1});
    ASSERT_EQ(descriptor.page_id, Id {10});
    ASSERT_EQ(descriptor.image, "");

    payload = decode_payload(payloads[1]);
    ASSERT_TRUE(std::holds_alternative<ImageDescriptor>(payload));
    descriptor = std::get<ImageDescriptor>(payload);
    ASSERT_EQ(descriptor.lsn, Lsn {2});
    ASSERT_EQ(descriptor.page_id, Id {20});
    ASSERT_EQ(descriptor.image, image);
}

TEST_F(WalTests, UnderstandsDeltaRecords)
{
    ASSERT_OK(wal->start_writing());
    ASSERT_EQ(wal->bytes_written(), 0);
    const auto image = random.Generate(kPageSize);
    ChangeBuffer delta {
        {100, 10},
        {200, 20},
        {300, 30},
    };
    ASSERT_OK(wal->log_delta(Id {12}, image, delta, nullptr));
    ASSERT_OK(wal->flush());

    std::vector<std::string> payloads;
    ASSERT_OK(read_segment(Id {1}, &payloads));
    ASSERT_EQ(payloads.size(), 1);

    const auto payload = decode_payload(payloads[0]);
    ASSERT_TRUE(std::holds_alternative<DeltaDescriptor>(payload));
    const auto descriptor = std::get<DeltaDescriptor>(payload);
    ASSERT_EQ(descriptor.lsn, Lsn {1});
    ASSERT_EQ(descriptor.page_id, Id {12});
    ASSERT_EQ(descriptor.deltas.size(), 3);
    for (std::size_t i {}; i < 3; ++i) {
        ASSERT_EQ(descriptor.deltas[i].offset, delta[i].offset);
        ASSERT_EQ(descriptor.deltas[i].data, image.range(delta[i].offset, delta[i].size));
    }
}

TEST_F(WalTests, UnderstandsVacuumRecords)
{
    ASSERT_OK(wal->start_writing());
    ASSERT_EQ(wal->bytes_written(), 0);
    ASSERT_OK(wal->log_vacuum(true, nullptr));
    ASSERT_OK(wal->log_vacuum(false, nullptr));
    ASSERT_OK(wal->flush());

    std::vector<std::string> payloads;
    ASSERT_OK(read_segment(Id {1}, &payloads));
    ASSERT_EQ(payloads.size(), 2);

    auto payload = decode_payload(payloads[0]);
    ASSERT_TRUE(std::holds_alternative<VacuumDescriptor>(payload));
    auto descriptor = std::get<VacuumDescriptor>(payload);
    ASSERT_EQ(descriptor.lsn, Lsn {1});
    ASSERT_TRUE(descriptor.is_start);

    payload = decode_payload(payloads[1]);
    ASSERT_TRUE(std::holds_alternative<VacuumDescriptor>(payload));
    descriptor = std::get<VacuumDescriptor>(payload);
    ASSERT_EQ(descriptor.lsn, Lsn {2});
    ASSERT_FALSE(descriptor.is_start);
}

} // namespace calicodb
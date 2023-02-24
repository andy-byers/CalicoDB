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

class WalComponentTests
    : public InMemoryTest,
      public testing::Test
{
public:
    static constexpr Size PAGE_SIZE {0x200};
    const std::string WAL_PREFIX {"test/wal-"};

    WalComponentTests()
        : m_writer_tail(wal_block_size(PAGE_SIZE), '\0'),
          m_reader_tail(wal_block_size(PAGE_SIZE), '\0'),
          m_reader_data(wal_block_size(PAGE_SIZE), '\0')
    {}

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

    [[nodiscard]]
    auto make_reader(Id id) -> WalReader
    {
        EXPECT_OK(storage->new_reader(encode_segment_name(WAL_PREFIX, id), &m_reader_file));
        return WalReader {*m_reader_file, m_reader_tail};
    }

    [[nodiscard]]
    auto make_writer(Id id) -> WalWriter
    {
        EXPECT_OK(storage->new_logger(encode_segment_name(WAL_PREFIX, id), &m_writer_file));
        return WalWriter {*m_writer_file, m_writer_tail};
    }

    [[nodiscard]]
    static auto wal_write(WalWriter &writer, Lsn lsn, const Slice &data) -> Status
    {
        std::string buffer(sizeof(lsn), '\0');
        buffer.append(data.to_string());
        WalPayloadIn payload {lsn, buffer};
        return writer.write(payload);
    }

    [[nodiscard]]
    static auto wal_read_with_status(WalReader &reader, std::string &out, Lsn *lsn = nullptr) -> Status
    {
        out.resize(wal_scratch_size(PAGE_SIZE));
        Span buffer {out};

        Calico_Try(reader.read(buffer));
        WalPayloadOut payload {buffer};
        if (lsn != nullptr) {
            *lsn = payload.lsn();
        }
        out = payload.data().to_string();
        return Status::ok();
    }

    [[nodiscard]]
    static auto wal_read(WalReader &reader, Lsn *lsn = nullptr) -> std::string
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
    for (; lsn.value < PAGE_SIZE * 5; ++lsn.value) {
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
    for (Size i {1}; i < PAGE_SIZE * 2; ++i) {
        ASSERT_OK(wal_write(writer, Lsn {i}, Tools::integral_key(i)));
    }
    ASSERT_OK(writer.flush());
    auto reader = make_reader(Id::root());
    for (Size i {1}; i < PAGE_SIZE * 2; ++i) {
        ASSERT_EQ(wal_read(reader), Tools::integral_key(i));
    }
    assert_reader_is_done(reader);
}

TEST_F(WalComponentTests, HandlesRecordsAcrossSparseBlocks)
{
    auto writer = make_writer(Id::root());
    for (Size i {1}; i < PAGE_SIZE * 2; ++i) {
        ASSERT_OK(wal_write(writer, Lsn {i}, Tools::integral_key(i)));
        if (rand() % 8 == 0) {
            ASSERT_OK(writer.flush());
        }
    }
    ASSERT_OK(writer.flush());
    auto reader = make_reader(Id::root());
    for (Size i {1}; i < PAGE_SIZE * 2; ++i) {
        ASSERT_EQ(wal_read(reader), Tools::integral_key(i));
    }
    assert_reader_is_done(reader);
}

TEST_F(WalComponentTests, Corruption)
{
    // Don't flush the writer, so it leaves a partial record in the WAL.
    auto writer = make_writer(Id::root());
    for (Size i {1}; i < PAGE_SIZE * 2; ++i) {
        ASSERT_OK(wal_write(writer, Lsn {i}, Tools::integral_key(i)));
    }
    ASSERT_LT(writer.flushed_lsn(), Lsn {PAGE_SIZE*2 - 1});

    auto reader = make_reader(Id::root());
    for (Size i {1}; i < PAGE_SIZE * 2; ++i) {
        std::string data;
        auto s = wal_read_with_status(reader, data);
        if (s.is_corruption()) {
            break;
        }
        ASSERT_OK(s);
        ASSERT_EQ(data, Tools::integral_key(i));
    }
    assert_reader_is_done(reader);
}

} // <anonymous>
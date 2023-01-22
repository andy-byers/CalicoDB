#include "record.h"
#include "utils/encoding.h"
#include "utils/system.h"

namespace Calico {

auto write_wal_record_header(Span out, const WalRecordHeader &header) -> void
{
    out[0] = header.type;
    out.advance();

    put_u16(out, header.size);
    out.advance(sizeof(std::uint16_t));

    put_u32(out, header.crc);
}

auto read_wal_record_header(Slice in) -> WalRecordHeader
{
    WalRecordHeader header;
    header.type = WalRecordHeader::Type {in[0]};
    in.advance();

    header.size = get_u16(in);
    in.advance(sizeof(std::uint16_t));

    header.crc = get_u32(in);
    return header;
}

auto read_wal_payload_header(Slice in) -> WalPayloadHeader
{
    return WalPayloadHeader {get_u64(in)};
}

auto split_record(WalRecordHeader &lhs, Slice payload, Size available_size) -> WalRecordHeader
{
    CALICO_EXPECT_NE(lhs.type, WalRecordHeader::Type::FIRST);
    CALICO_EXPECT_EQ(lhs.size, payload.size());
    CALICO_EXPECT_LT(available_size, WalRecordHeader::SIZE + payload.size()); // Only call this if we actually need a split.
    auto rhs = lhs;

    lhs.size = static_cast<std::uint16_t>(available_size - WalRecordHeader::SIZE);
    rhs.size = static_cast<std::uint16_t>(payload.size() - lhs.size);
    rhs.type = WalRecordHeader::Type::LAST;

    if (lhs.type == WalRecordHeader::Type::FULL) {
        lhs.type = WalRecordHeader::Type::FIRST;
    } else {
        CALICO_EXPECT_EQ(lhs.type, WalRecordHeader::Type::LAST);
        lhs.type = WalRecordHeader::Type::MIDDLE;
    }
    return rhs;
}

template<bool IsLeftMerge>
static auto merge_records(WalRecordHeader &lhs, const WalRecordHeader &rhs) -> Status
{
    [[maybe_unused]]
    static constexpr auto FIRST_TYPE = IsLeftMerge ? WalRecordHeader::FIRST : WalRecordHeader::LAST;
    static constexpr auto LAST_TYPE = IsLeftMerge ? WalRecordHeader::LAST : WalRecordHeader::FIRST;
    CALICO_EXPECT_NE(lhs.type, rhs.type);

    // First merge in the logical record.
    if (lhs.type == WalRecordHeader::Type {}) {
        CALICO_EXPECT_TRUE(rhs.type != WalRecordHeader::MIDDLE &&
                           rhs.type != LAST_TYPE);

        lhs.type = rhs.type;
        lhs.crc = rhs.crc;

    } else {
        CALICO_EXPECT_EQ(lhs.type, FIRST_TYPE);
        if (lhs.crc != rhs.crc)
            return corruption("cannot merge WAL records: fragments do not belong to the same logical record");
        if (rhs.type == LAST_TYPE)
            lhs.type = WalRecordHeader::FULL;
    }
    lhs.size = static_cast<std::uint16_t>(lhs.size + rhs.size);
    return ok();
}

auto merge_records_left(WalRecordHeader &lhs, const WalRecordHeader &rhs) -> Status
{
    return merge_records<true>(lhs, rhs);
}

auto merge_records_right(const WalRecordHeader &lhs, WalRecordHeader &rhs) -> Status
{
    return merge_records<false>(rhs, lhs);
}


static auto encode_payload_type(Span out, XactPayloadType type)
{
    CALICO_EXPECT_FALSE(out.is_empty());
    out[0] = type;
}

auto encode_deltas_payload(Lsn lsn, Id page_id, Slice image, const std::vector<PageDelta> &deltas, Span buffer) -> WalPayloadIn
{
    auto saved = buffer;
    buffer.advance(sizeof(lsn));

    // Payload type (1 B)
    encode_payload_type(buffer, XactPayloadType::DELTA);
    buffer.advance();

    // Page ID (8 B)
    put_u64(buffer, page_id.value);
    buffer.advance(sizeof(page_id));

    // Deltas count (2 B)
    put_u16(buffer, static_cast<std::uint16_t>(deltas.size()));
    buffer.advance(sizeof(std::uint16_t));

    // Deltas (N B)
    for (const auto &[offset, size]: deltas) {
        put_u16(buffer, static_cast<std::uint16_t>(offset));
        buffer.advance(sizeof(std::uint16_t));

        put_u16(buffer, static_cast<std::uint16_t>(size));
        buffer.advance(sizeof(std::uint16_t));

        mem_copy(buffer, image.range(offset, size));
        buffer.advance(size);
    }
    saved.truncate(saved.size() - buffer.size());
    return WalPayloadIn {lsn, saved};
}

auto encode_commit_payload(Lsn lsn, Span buffer) -> WalPayloadIn
{
    auto saved = buffer;
    buffer.advance(sizeof(Lsn));

    // Payload type (1 B)
    encode_payload_type(buffer, XactPayloadType::COMMIT);
    buffer.advance();

    saved.truncate(saved.size() - buffer.size());
    return WalPayloadIn {lsn, saved};
}

auto encode_full_image_payload(Lsn lsn, Id pid, Slice image, Span buffer) -> WalPayloadIn
{
    auto saved = buffer;
    buffer.advance(sizeof(lsn));

    // Payload type (1 B)
    encode_payload_type(buffer, XactPayloadType::FULL_IMAGE);
    buffer.advance();

    // Page ID (8 B)
    put_u64(buffer, pid.value);
    buffer.advance(sizeof(pid));

    // Image (N B)
    mem_copy(buffer, image);
    buffer.advance(image.size());

    saved.truncate(saved.size() - buffer.size());
    return WalPayloadIn {lsn, saved};
}

static auto decode_deltas_payload(WalPayloadOut in) -> DeltaDescriptor
{
    DeltaDescriptor info;
    auto data = in.data();
    info.lsn = in.lsn();

    // Payload type (1 B)
    CALICO_EXPECT_EQ(XactPayloadType {data[0]}, XactPayloadType::DELTA);
    data.advance();

    // Page ID (8 B)
    info.pid.value = get_u64(data);
    data.advance(sizeof(info.pid));

    // Delta count (2 B)
    info.deltas.resize(get_u16(data));
    data.advance(sizeof(std::uint16_t));

    // Deltas (N B)
    std::generate(begin(info.deltas), end(info.deltas), [&data] {
        DeltaDescriptor::Delta delta;
        delta.offset = get_u16(data);
        data.advance(sizeof(std::uint16_t));

        const auto size = get_u16(data);
        data.advance(sizeof(std::uint16_t));

        delta.data = data.range(0, size);
        data.advance(size);
        return delta;
    });
    return info;
}

static auto decode_full_image_payload(WalPayloadOut in) -> FullImageDescriptor
{
    FullImageDescriptor info;
    auto data = in.data();
    info.lsn = in.lsn();

    // Payload type (1 B)
    CALICO_EXPECT_EQ(XactPayloadType {data[0]}, XactPayloadType::FULL_IMAGE);
    data.advance();

    // Page ID (8 B)
    info.pid.value = get_u64(data);
    data.advance(sizeof(Id));

    // Image (n B)
    info.image = data;
    return info;
}

static auto decode_commit_payload(WalPayloadOut in) -> CommitDescriptor
{
    CommitDescriptor info;
    info.lsn = in.lsn();
    return info;
}

auto decode_payload(WalPayloadOut in) -> PayloadDescriptor
{
    switch (XactPayloadType {in.data()[0]}) {
        case XactPayloadType::DELTA:
            return decode_deltas_payload(in);
        case XactPayloadType::FULL_IMAGE:
            return decode_full_image_payload(in);
        case XactPayloadType::COMMIT:
            return decode_commit_payload(in);
        default:
            return std::monostate {};
    }
}

} // namespace Calico
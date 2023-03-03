#include "wal_record.h"
#include "encoding.h"

namespace calicodb
{

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
    header.type = WalRecordType {in[0]};
    in.advance();

    header.size = get_u16(in);
    in.advance(sizeof(std::uint16_t));

    header.crc = get_u32(in);
    return header;
}

auto split_record(WalRecordHeader &lhs, const Slice &payload, std::size_t available_size) -> WalRecordHeader
{
    CDB_EXPECT_NE(lhs.type, kFirstRecord);
    CDB_EXPECT_EQ(lhs.size, payload.size());
    CDB_EXPECT_LT(available_size, WalRecordHeader::kSize + payload.size()); // Only call this if we actually need a split.
    auto rhs = lhs;

    lhs.size = static_cast<std::uint16_t>(available_size - WalRecordHeader::kSize);
    rhs.size = static_cast<std::uint16_t>(payload.size() - lhs.size);
    rhs.type = kLastRecord;

    if (lhs.type == kFullRecord) {
        lhs.type = kFirstRecord;
    } else {
        CDB_EXPECT_EQ(lhs.type, kLastRecord);
        lhs.type = kMiddleRecord;
    }
    return rhs;
}

auto merge_records_left(WalRecordHeader &lhs, const WalRecordHeader &rhs) -> Status
{
    if (lhs.type == rhs.type) {
        return Status::corruption("records should not have same type");
    }

    // First merge in the logical record.
    if (lhs.type == kNoRecord) {
        if (rhs.type == kMiddleRecord || rhs.type == kLastRecord) {
            return Status::corruption("right record has invalid type");
        }

        lhs.type = rhs.type;
        lhs.crc = rhs.crc;

    } else {
        if (lhs.type != kFirstRecord) {
            return Status::corruption("left record has invalid type");
        }
        if (lhs.crc != rhs.crc) {
            return Status::corruption("fragment crc mismatch");
        }
        if (rhs.type == kLastRecord) {
            lhs.type = kFullRecord;
        }
    }
    lhs.size = static_cast<std::uint16_t>(lhs.size + rhs.size);
    return Status::ok();
}

static auto encode_payload_type(Span out, WalPayloadType type)
{
    CDB_EXPECT_FALSE(out.is_empty());
    out[0] = type;
}

auto encode_deltas_payload(Lsn lsn, Id page_id, const Slice &image, const std::vector<PageDelta> &deltas, Span buffer) -> WalPayloadIn
{
    auto saved = buffer;
    buffer.advance(sizeof(lsn));

    // Payload type (1 B)
    encode_payload_type(buffer, WalPayloadType::kDeltaPayload);
    buffer.advance();

    // Page ID (8 B)
    put_u64(buffer, page_id.value);
    buffer.advance(sizeof(page_id));

    // Deltas count (2 B)
    put_u16(buffer, static_cast<std::uint16_t>(deltas.size()));
    buffer.advance(sizeof(std::uint16_t));

    // Deltas (N B)
    for (const auto &[offset, size] : deltas) {
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

auto encode_full_image_payload(Lsn lsn, Id pid, const Slice &image, Span buffer) -> WalPayloadIn
{
    auto saved = buffer;
    buffer.advance(sizeof(lsn));

    // Payload type (1 B)
    encode_payload_type(buffer, WalPayloadType::kFullImagePayload);
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
    CDB_EXPECT_EQ(WalPayloadType {data[0]}, WalPayloadType::kDeltaPayload);
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
    CDB_EXPECT_EQ(WalPayloadType {data[0]}, WalPayloadType::kFullImagePayload);
    data.advance();

    // Page ID (8 B)
    info.pid.value = get_u64(data);
    data.advance(sizeof(Id));

    // Image (n B)
    info.image = data;
    return info;
}

auto decode_payload(WalPayloadOut in) -> PayloadDescriptor
{
    switch (WalPayloadType {in.data()[0]}) {
    case WalPayloadType::kDeltaPayload:
        return decode_deltas_payload(in);
    case WalPayloadType::kFullImagePayload:
        return decode_full_image_payload(in);
    default:
        return std::monostate {};
    }
}

} // namespace calicodb
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

static auto decode_deltas_payload(const Slice &in) -> DeltaDescriptor
{
    DeltaDescriptor info;
    auto data = in.data();

    // Payload type (1 B)
    CDB_EXPECT_EQ(WalPayloadType {*data}, WalPayloadType::kDeltaPayload);
    ++data;

    // LSN (8 B)
    info.lsn.value = get_u64(data);
    data += sizeof(Lsn);

    // Table ID (8 B)
    info.table_id.value = get_u64(data);
    data += sizeof(Id);

    // Page ID (8 B)
    info.page_id.value = get_u64(data);
    data += sizeof(Id);

    // Delta count (2 B)
    info.deltas.resize(get_u16(data));
    data += sizeof(std::uint16_t);

    // Deltas (N B)
    std::generate(begin(info.deltas), end(info.deltas), [&data] {
        DeltaDescriptor::Delta delta;
        delta.offset = get_u16(data);
        data += sizeof(std::uint16_t);

        const auto size = get_u16(data);
        data += sizeof(std::uint16_t);

        delta.data = Slice {data, size};
        data += size;
        return delta;
    });
    return info;
}

static auto decode_full_image_payload(const Slice &in) -> ImageDescriptor
{
    ImageDescriptor info;
    auto data = in.data();

    // Payload type (1 B)
    CDB_EXPECT_EQ(WalPayloadType {*data}, WalPayloadType::kImagePayload);
    ++data;

    // LSN (8 B)
    info.lsn.value = get_u64(data);
    data += sizeof(Lsn);

    // Table ID (8 B)
    info.table_id.value = get_u64(data);
    data += sizeof(Id);

    // Page ID (8 B)
    info.page_id.value = get_u64(data);

    // Image (n B)
    info.image = in.range(ImageDescriptor::kHeaderSize);
    return info;
}

auto decode_payload(const Slice &in) -> PayloadDescriptor
{
    switch (WalPayloadType {in.data()[0]}) {
    case WalPayloadType::kDeltaPayload:
        return decode_deltas_payload(in);
    case WalPayloadType::kImagePayload:
        return decode_full_image_payload(in);
    default:
        return std::monostate {};
    }
}

auto encode_deltas_payload(Lsn lsn, Id table_id, Id page_id, const Slice &image, const ChangeBuffer &deltas, char *buffer) -> Slice
{
    auto saved = buffer;

    // Payload type (1 B)
    *buffer++ = WalPayloadType::kDeltaPayload;

    // LSN (8 B)
    put_u64(buffer, lsn.value);
    buffer += sizeof(Lsn);

    // Table ID (8 B)
    put_u64(buffer, table_id.value);
    buffer += sizeof(Id);

    // Page ID (8 B)
    put_u64(buffer, page_id.value);
    buffer += sizeof(Id);

    // Deltas count (2 B)
    put_u16(buffer, static_cast<std::uint16_t>(deltas.size()));
    buffer += sizeof(std::uint16_t);

    // Deltas (N B)
    std::size_t n {};
    for (const auto &[offset, size] : deltas) {
        put_u16(buffer + n, static_cast<std::uint16_t>(offset));
        n += sizeof(std::uint16_t);

        put_u16(buffer + n, static_cast<std::uint16_t>(size));
        n += sizeof(std::uint16_t);

        std::memcpy(buffer + n, image.data() + offset, size);
        n += size;
    }
    return Slice {saved, DeltaDescriptor::kHeaderSize + n};
}

auto encode_image_payload(Lsn lsn, Id table_id, Id page_id, const Slice &image, char *buffer) -> Slice
{
    auto saved = buffer;

    // Payload type (1 B)
    *buffer++ = WalPayloadType::kImagePayload;

    // LSN (8 B)
    put_u64(buffer, lsn.value);
    buffer += sizeof(lsn);

    // Table ID (8 B)
    put_u64(buffer, table_id.value);
    buffer += sizeof(Id);

    // Page ID (8 B)
    put_u64(buffer, page_id.value);
    buffer += sizeof(Id);

    // Image (N B)
    std::memcpy(buffer, image.data(), image.size());
    return Slice {saved, ImageDescriptor::kHeaderSize + image.size()};
}

auto extract_payload_lsn(const Slice &in) -> Lsn
{
    return {get_u64(in.data() + sizeof(WalPayloadType))};
}

} // namespace calicodb
#include "record.h"
#include "utils/crc.h"
#include "utils/encoding.h"
#include "utils/logging.h"

namespace calico {

auto contains_record(BytesView in) -> bool
{
    if (in.size() > sizeof(WalRecordHeader))
        return get_u64(in) != 0;
    return false;
}

auto write_wal_record_header(Bytes out, const WalRecordHeader &header) -> void
{
    BytesView bytes {reinterpret_cast<const Byte*>(&header), sizeof(header)};
    mem_copy(out, bytes);
}

auto read_wal_record_header(BytesView in) -> WalRecordHeader
{
    WalRecordHeader header {};
    Bytes bytes {reinterpret_cast<Byte*>(&header), sizeof(header)};
    mem_copy(bytes, in.truncate(bytes.size()));
    return header;
}

auto encode_deltas_payload(SequenceId lsn, PageId page_id, BytesView image, const std::vector<PageDelta> &deltas, Bytes out) -> Size
{
    const auto original_size = out.size();

    // Payload type (1 B)
    out[0] = static_cast<Byte>(WalPayloadType::DELTAS);
    out.advance();

    // LSN (8 B)
    put_u64(out, lsn.value);
    out.advance(sizeof(lsn));

    // Page ID (8 B)
    put_u64(out, page_id.value);
    out.advance(sizeof(page_id));

    // Deltas count (2 B)
    put_u16(out, static_cast<std::uint16_t>(deltas.size()));
    out.advance(sizeof(std::uint16_t));

    // Deltas (N B)
    for (const auto &[offset, size]: deltas) {
        put_u16(out, static_cast<std::uint16_t>(offset));
        out.advance(sizeof(std::uint16_t));

        put_u16(out, static_cast<std::uint16_t>(size));
        out.advance(sizeof(std::uint16_t));

        mem_copy(out, image.range(offset, size));
        out.advance(size);
    }
    return original_size - out.size();
}

auto decode_deltas_payload(BytesView in) -> DeltasDescriptor
{
    DeltasDescriptor info;

    // Payload type (1 B)
    CALICO_EXPECT_EQ(decode_payload_type(in), WalPayloadType::DELTAS);
    in.advance();

    // LSN (8 B)
    info.page_lsn.value = get_u64(in);
    in.advance(sizeof(info.page_lsn));

    // Page ID (8 B)
    info.page_id.value = get_u64(in);
    in.advance(sizeof(info.page_id));

    // Deltas count (2 B)
    info.deltas.resize(get_u16(in));
    in.advance(sizeof(std::uint16_t));

    // Deltas (N B)
    for (auto &[offset, bytes]: info.deltas) {
        offset = get_u16(in);
        in.advance(sizeof(std::uint16_t));

        const auto size = get_u16(in);
        in.advance(sizeof(std::uint16_t));

        bytes = in.range(0, size);
        in.advance(size);
    }
    return info;
}

auto encode_commit_payload(SequenceId lsn, Bytes out) -> Size
{
    // Payload type (1 B)
    out[0] = static_cast<Byte>(WalPayloadType::COMMIT);

    // LSN (8 B)
    put_u64(out, lsn.value);
    out.advance(sizeof(lsn));

    return MINIMUM_PAYLOAD_SIZE;
}

auto decode_commit_payload(BytesView in) -> CommitDescriptor
{
    CALICO_EXPECT_EQ(decode_payload_type(in), WalPayloadType::COMMIT);
    return CommitDescriptor {decode_lsn(in)};
}

auto encode_full_image_payload(SequenceId lsn, PageId page_id, BytesView image, Bytes out) -> Size
{
    const auto original_size = out.size();

    // Payload type (1 B)
    out[0] = static_cast<Byte>(WalPayloadType::FULL_IMAGE);
    out.advance();

    // LSN (8 B)
    put_u64(out, lsn.value);
    out.advance(sizeof(lsn));

    // Page ID (8 B)
    put_u64(out, page_id.value);
    out.advance(sizeof(page_id));

    // Image (N B)
    mem_copy(out, image);
    out.advance(image.size());

    return original_size - out.size();
}

auto decode_full_image_payload(BytesView in) -> FullImageDescriptor
{
    CALICO_EXPECT_EQ(decode_payload_type(in), WalPayloadType::FULL_IMAGE);
    in.advance();

    FullImageDescriptor info {};
    // NOTE: Skipping the LSN. Full images don't use this field.
    in.advance(sizeof(SequenceId));
    info.page_id.value = get_u64(in);
    info.image = in.range(sizeof(PageId));
    return info;
}

auto split_record(WalRecordHeader &lhs, BytesView payload, Size available_size) -> WalRecordHeader
{
    CALICO_EXPECT_NE(lhs.type, WalRecordHeader::Type::FIRST);
    CALICO_EXPECT_EQ(lhs.size, payload.size());
    CALICO_EXPECT_LT(available_size, sizeof(lhs) + payload.size()); // Only call this if we actually need a split.
    auto rhs = lhs;

    lhs.size = static_cast<std::uint16_t>(available_size - sizeof(lhs));
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
    static constexpr auto MSG = "cannot merge WAL records";
    CALICO_EXPECT_NE(lhs.type, rhs.type);

    // First merge in the logical record.
    if (lhs.type == WalRecordHeader::Type {}) {
        CALICO_EXPECT_TRUE(rhs.type != WalRecordHeader::MIDDLE &&
                           rhs.type != LAST_TYPE);

        lhs.type = rhs.type;
        lhs.crc = rhs.crc;

    } else {
        CALICO_EXPECT_EQ(lhs.type, FIRST_TYPE);
        if (lhs.crc != rhs.crc) {
            ThreePartMessage message;
            message.set_primary(MSG);
            message.set_detail("fragments do not belong to the same logical record");
            return message.corruption();
        }
        if (rhs.type == LAST_TYPE)
            lhs.type = WalRecordHeader::FULL;
    }
    lhs.size = static_cast<std::uint16_t>(lhs.size + rhs.size);
    return Status::ok();
}

auto merge_records_left(WalRecordHeader &lhs, const WalRecordHeader &rhs) -> Status
{
    return merge_records<true>(lhs, rhs);
}

auto merge_records_right(const WalRecordHeader &lhs, WalRecordHeader &rhs) -> Status
{
    return merge_records<false>(rhs, lhs);
}

} // namespace calico
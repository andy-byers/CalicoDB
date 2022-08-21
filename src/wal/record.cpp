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

auto encode_deltas_payload(PageId page_id, BytesView image, const std::vector<PageDelta> &deltas, Bytes out) -> Size
{
    const auto original_size = out.size();
    out[0] = static_cast<Byte>(WalPayloadType::DELTAS);
    out.advance();

    put_u64(out, page_id.value);
    out.advance(sizeof(page_id.value));

    put_u16(out, static_cast<std::uint16_t>(deltas.size()));
    out.advance(sizeof(std::uint16_t));

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

auto decode_deltas_payload(const WalRecordHeader &header, BytesView in) -> RedoDescriptor
{
    RedoDescriptor redo;
    redo.page_lsn = header.lsn;
    redo.is_commit = false;

    // We assume that the payload type is still at the front of the input data. The caller should have checked and switched on the value prior to calling one
    // of these "decode_*()" functions.
    CALICO_EXPECT_EQ(read_payload_type(in), WalPayloadType::DELTAS);
    in.advance();

    redo.page_id = get_u64(in);
    in.advance(sizeof(redo.page_id));

    redo.deltas.resize(get_u16(in));
    in.advance(sizeof(std::uint16_t));

    redo.is_commit = false;

    for (auto &[offset, bytes]: redo.deltas) {
        offset = get_u16(in);
        in.advance(sizeof(std::uint16_t));

        const auto size = get_u16(in);
        in.advance(sizeof(std::uint16_t));

        bytes = in.range(0, size);
        in.advance(size);
    }
    return redo;
}

auto encode_commit_payload(Bytes in) -> Size
{
    in[0] = static_cast<Byte>(WalPayloadType::COMMIT);
    return sizeof(WalPayloadType);
}

auto decode_commit_payload(const WalRecordHeader &header, BytesView in) -> RedoDescriptor
{
    CALICO_EXPECT_EQ(read_payload_type(in), WalPayloadType::COMMIT);

    RedoDescriptor redo;
    redo.page_lsn = header.lsn;
    redo.is_commit = true;
    return redo;
}

auto encode_full_image_payload(PageId page_id, BytesView image, Bytes out) -> Size
{
    const auto original_size = out.size();

    // This routine should copy from memory owned by the pager to memory owned by the WAL.
    out[0] = static_cast<Byte>(WalPayloadType::FULL_IMAGE);
    out.advance();

    put_u64(out, page_id.value);
    out.advance(sizeof(page_id));

    mem_copy(out, image);
    out.advance(image.size());

    return original_size - out.size();
}

auto decode_full_image_payload(BytesView in) -> UndoDescriptor
{
    // In this case, "in" should contain the exact payload.
    CALICO_EXPECT_EQ(WalPayloadType {in[0]}, WalPayloadType::FULL_IMAGE);
    in.advance();

    UndoDescriptor descriptor {};
    descriptor.page_id = get_u64(in);
    descriptor.image = in.range(sizeof(PageId));
    return descriptor;
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
    static constexpr auto MSG = "cannot merge WAL records";
    static constexpr auto FIRST_TYPE = IsLeftMerge ? WalRecordHeader::FIRST : WalRecordHeader::LAST;
    static constexpr auto LAST_TYPE = IsLeftMerge ? WalRecordHeader::LAST : WalRecordHeader::FIRST;
    CALICO_EXPECT_NE(lhs.type, rhs.type);

    // First merge in the logical record.
    if (lhs.type == WalRecordHeader::Type {}) {
        CALICO_EXPECT_TRUE(rhs.type != WalRecordHeader::MIDDLE &&
                           rhs.type != LAST_TYPE);
        CALICO_EXPECT_EQ(lhs.lsn, 0);

        lhs.type = rhs.type;
        lhs.lsn = rhs.lsn;
        lhs.crc = rhs.crc;

    } else {
        CALICO_EXPECT_EQ(lhs.type, FIRST_TYPE);
        if (lhs.lsn != rhs.lsn || lhs.crc != rhs.crc) {
            ThreePartMessage message;
            message.set_primary(MSG);
            message.set_detail("fragments do not belong to the same logical record");
            return message.corruption();
        }
        if (rhs.type == LAST_TYPE)
            lhs.type = WalRecordHeader::FULL;
    }
    lhs.size += rhs.size;
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
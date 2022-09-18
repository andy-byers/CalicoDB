#include "record.h"
#include "utils/crc.h"
#include "utils/encoding.h"
#include "utils/info_log.h"

namespace calico {

auto write_wal_record_header(Bytes out, const WalRecordHeader &header) -> void
{
    out[0] = header.type;
    out.advance();

    put_u16(out, header.size);
    out.advance(sizeof(header.size));

    put_u32(out, header.crc);
}

auto write_wal_payload_header(Bytes out, const WalPayloadHeader &header) -> void
{
    put_u64(out, header.lsn.value);
}

auto read_wal_record_header(BytesView in) -> WalRecordHeader
{
    WalRecordHeader header {};
    header.type = WalRecordHeader::Type {in[0]};
    in.advance();

    header.size = get_u16(in);
    in.advance(sizeof(header.size));

    header.crc = get_u32(in);
    return header;
}

auto read_wal_payload_header(BytesView in) -> WalPayloadHeader
{
    WalPayloadHeader header {};
    header.lsn.value = get_u64(in);
    return header;
}

auto split_record(WalRecordHeader &lhs, BytesView payload, Size available_size) -> WalRecordHeader
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
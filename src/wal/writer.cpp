#include "writer.h"
#include "utils/crc.h"
#include "utils/types.h"

namespace calicodb
{

auto WalWriter::write(WalPayloadIn payload) -> Status
{
    const auto lsn = payload.lsn();
    CALICO_EXPECT_FALSE(lsn.is_null());
    auto data = payload.m_buffer;
    CALICO_EXPECT_FALSE(data.is_empty());

    WalRecordHeader lhs;
    lhs.type = WRT_Full;
    lhs.size = static_cast<std::uint16_t>(data.size());
    lhs.crc = crc32c::Value(data.data(), data.size());
    lhs.crc = crc32c::Mask(lhs.crc);

    while (!data.is_empty()) {
        auto rest = m_tail;
        // Note that this modifies rest to point to [<m_offset>, <end>) in the tail buffer.
        const auto space_remaining = rest.advance(m_offset).size();
        const auto needs_split = space_remaining < WalRecordHeader::SIZE + data.size();

        if (space_remaining <= WalRecordHeader::SIZE) {
            CALICO_EXPECT_LE(m_tail.size() - m_offset, WalRecordHeader::SIZE);
            CALICO_TRY(flush());
            continue;
        }
        WalRecordHeader rhs;

        if (needs_split) {
            rhs = split_record(lhs, data, space_remaining);
        }

        // We must have room for the whole header and at least 1 payload byte.
        write_wal_record_header(rest, lhs);
        rest.advance(WalRecordHeader::SIZE);
        mem_copy(rest, data.range(0, lhs.size));

        m_offset += WalRecordHeader::SIZE + lhs.size;
        data.advance(lhs.size);
        rest.advance(lhs.size);

        if (needs_split) {
            lhs = rhs;
        } else {
            CALICO_EXPECT_TRUE(data.is_empty());
            // Record is fully in the tail buffer and maybe partially on disk. Next time we flush, this record is guaranteed
            // to be all the way on disk.
            m_last_lsn = lsn;
        }
    }
    return Status::ok();
}

auto WalWriter::flush() -> Status
{
    // Already flushed.
    if (m_offset == 0) {
        return Status::ok();
    }

    // Clear unused bytes at the end of the tail buffer.
    mem_clear(m_tail.range(m_offset));

    auto s = m_file->write(m_tail);
    if (s.is_ok()) {
        m_flushed_lsn = m_last_lsn;
        m_offset = 0;
        ++m_block;
    }
    return s;
}

auto WalWriter::flushed_lsn() const -> Lsn
{
    return m_flushed_lsn;
}

} // namespace calicodb
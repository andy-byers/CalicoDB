// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for contributor names.

#include "wal_writer.h"
#include "crc.h"
#include "types.h"

namespace calicodb
{

auto WalWriter::write(Lsn lsn, const Slice &payload) -> Status
{
    CDB_EXPECT_FALSE(payload.is_empty());
    CDB_EXPECT_FALSE(lsn.is_null());
    auto data = payload;

    WalRecordHeader lhs;
    lhs.type = kFullRecord;
    lhs.size = static_cast<std::uint16_t>(data.size());
    lhs.crc = crc32c::Value(data.data(), data.size());
    lhs.crc = crc32c::Mask(lhs.crc);

    while (!data.is_empty()) {
        auto rest = m_tail;
        // Note that this modifies rest to point to [<m_offset>, <end>) in the tail buffer.
        const auto space_remaining = rest.advance(m_offset).size();
        const auto needs_split = space_remaining < WalRecordHeader::kSize + data.size();

        if (space_remaining <= WalRecordHeader::kSize) {
            CDB_EXPECT_LE(m_tail.size() - m_offset, WalRecordHeader::kSize);
            CDB_TRY(flush());
            continue;
        }
        WalRecordHeader rhs;

        if (needs_split) {
            rhs = split_record(lhs, data, space_remaining);
        }

        // We must have room for the whole header and at least 1 payload byte.
        write_wal_record_header(rest.data(), lhs);
        rest.advance(WalRecordHeader::kSize);
        std::memcpy(rest.data(), data.data(), lhs.size);

        m_offset += WalRecordHeader::kSize + lhs.size;
        data.advance(lhs.size);
        rest.advance(lhs.size);

        if (needs_split) {
            lhs = rhs;
        } else {
            CDB_EXPECT_TRUE(data.is_empty());
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
    std::memset(m_tail.data() + m_offset, 0, m_tail.size() - m_offset);

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
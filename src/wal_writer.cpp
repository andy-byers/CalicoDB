// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "wal_writer.h"
#include "crc.h"

namespace calicodb
{

WalWriter::WalWriter(Logger &file, std::string &tail)
    : m_tail {&tail},
      m_file {&file}
{
    for (std::size_t i = 0; i <= kNumRecordTypes; ++i) {
        const auto type = static_cast<char>(i);
        m_type_crc[i] = crc32c::Value(&type, 1);
    }
}

auto WalWriter::flushed_on_last_write() const -> bool
{
    return m_flushed;
}

auto WalWriter::block_number() const -> std::size_t
{
    return m_block;
}

auto WalWriter::write(const Slice &payload) -> Status
{
    CALICODB_EXPECT_FALSE(payload.is_empty());
    auto rest = payload;
    m_flushed = false;

    while (!rest.is_empty()) {
        if (m_offset + WalRecordHeader::kSize >= m_tail->size()) {
            // Clear the rest of the tail buffer and append it to the log.
            CALICODB_TRY(flush());
            m_flushed = true;
        }
        // There should always be enough room to write the header and 1 payload byte.
        CALICODB_EXPECT_LT(m_offset + WalRecordHeader::kSize, m_tail->size());
        const auto space_for_payload = m_tail->size() - m_offset - WalRecordHeader::kSize;
        const auto fragment_length = std::min(rest.size(), space_for_payload);
        CALICODB_EXPECT_NE(fragment_length, 0);

        WalRecordType type;
        const auto begin = rest.size() == payload.size();
        const auto end = rest.size() == fragment_length;
        if (begin && end) {
            type = kFullRecord;
        } else if (begin) {
            type = kFirstRecord;
        } else if (end) {
            type = kLastRecord;
        } else {
            type = kMiddleRecord;
        }
        char header[WalRecordHeader::kSize];
        *header = type;
        put_u16(header + 1, static_cast<std::uint16_t>(fragment_length));
        put_u32(header + 3, crc32c::Mask(crc32c::Extend(m_type_crc[type], rest.data(), fragment_length)));

        std::memcpy(m_tail->data() + m_offset, header, sizeof(header));
        m_offset += sizeof(header);

        std::memcpy(m_tail->data() + m_offset, rest.data(), fragment_length);
        m_offset += fragment_length;

        rest.advance(fragment_length);
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
    std::memset(m_tail->data() + m_offset, 0, m_tail->size() - m_offset);

    auto s = m_file->write(*m_tail);
    if (s.is_ok()) {
        m_offset = 0;
        ++m_block;
    }
    return s;
}

} // namespace calicodb
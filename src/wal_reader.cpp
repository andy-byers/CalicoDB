// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "wal_reader.h"
#include "crc.h"

namespace calicodb
{

[[nodiscard]] static auto read_tail(Reader &file, std::size_t number, std::string &tail) -> Status
{
    Slice slice;
    CALICODB_TRY(file.read(number * tail.size(), tail.size(), tail.data(), &slice));

    if (slice.is_empty()) {
        return Status::not_found("end of file");
    } else if (slice.size() != tail.size()) {
        return Status::corruption("incomplete block");
    }
    return Status::ok();
}

WalReader::WalReader(Reader &file, std::string &tail)
    : m_tail {&tail},
      m_file {&file}
{
}

auto WalReader::read(std::string &out) -> Status
{
    if (m_offset + m_block == 0) {
        CALICODB_TRY(read_tail(*m_file, 0, *m_tail));
    }
    WalRecordHeader header;
    std::size_t end {};
    out.clear();

    for (;;) {
        const auto has_enough_space = m_tail->size() > m_offset + WalRecordHeader::kSize;
        auto rest = Slice(*m_tail).range(m_offset);

        if (has_enough_space && WalRecordHeader::contains_record(rest)) {
            const auto temp = read_wal_record_header(rest);
            const auto type_crc = crc32c::Value(rest.data(), 1);
            rest.advance(WalRecordHeader::kSize);
            if (temp.size == 0 || temp.size > rest.size()) {
                return Status::corruption("fragment size is invalid");
            }
            const auto expected_crc = crc32c::Unmask(temp.crc);
            const auto computed_crc = crc32c::Extend(type_crc, rest.data(), temp.size);
            if (expected_crc != computed_crc) {
                return Status::corruption("crc mismatch");
            }

            CALICODB_TRY(merge_records_left(header, temp));
            out.resize(end + temp.size);
            std::memcpy(out.data() + end, rest.data(), temp.size);
            m_offset += WalRecordHeader::kSize + temp.size;
            end += temp.size;

            if (header.type == kFullRecord) {
                break;
            }
            if (!rest.is_empty()) {
                continue;
            }
        }
        // Read the next block into the tail buffer.
        auto s = read_tail(*m_file, ++m_block, *m_tail);
        if (!s.is_ok()) {
            if (s.is_not_found() && header.type != kNoRecord) {
                return Status::corruption("encountered a partial record");
            }
            return s;
        }
        m_offset = 0;
    }
    return Status::ok();
}

} // namespace calicodb
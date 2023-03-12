#include "wal_reader.h"
#include "crc.h"

namespace calicodb
{

[[nodiscard]] static auto read_tail(Reader &file, std::size_t number, Span tail) -> Status
{
    auto temp = tail;
    auto read_size = tail.size();
    CDB_TRY(file.read(temp.data(), &read_size, number * tail.size()));

    if (read_size == 0) {
        return Status::not_found("end of file");
    } else if (read_size != tail.size()) {
        return Status::corruption("incomplete block");
    }
    return Status::ok();
}

WalReader::WalReader(Reader &file, Span tail)
    : m_tail {tail},
      m_file {&file}
{
}

auto WalReader::read(Span &payload) -> Status
{
    if (m_offset + m_block == 0) {
        CDB_TRY(read_tail(*m_file, 0, m_tail));
    }
    auto out = payload;
    WalRecordHeader header;

    for (;;) {
        const auto has_enough_space = m_tail.size() > m_offset + WalRecordHeader::kSize;
        auto rest = m_tail.range(m_offset);

        if (has_enough_space && WalRecordHeader::contains_record(rest)) {
            const auto temp = read_wal_record_header(rest);
            rest.advance(WalRecordHeader::kSize);

            CDB_TRY(merge_records_left(header, temp));
            if (temp.size == 0 || temp.size > rest.size()) {
                return Status::corruption("fragment size is invalid");
            }
            std::memcpy(out.data(), rest.data(), temp.size);
            m_offset += WalRecordHeader::kSize + temp.size;
            out.advance(temp.size);

            if (header.type == kFullRecord) {
                payload.truncate(header.size);
                const auto expected_crc = crc32c::Unmask(header.crc);
                const auto computed_crc = crc32c::Value(payload.data(), header.size);
                if (expected_crc != computed_crc) {
                    return Status::corruption("crc mismatch");
                }
                break;
            }
            if (!rest.is_empty()) {
                continue;
            }
        }
        // Read the next block into the tail buffer.
        auto s = read_tail(*m_file, ++m_block, m_tail);
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
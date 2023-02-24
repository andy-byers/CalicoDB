#include "reader.h"
#include "calico/storage.h"
#include "pager/page.h"

namespace Calico {

[[nodiscard]] static auto read_tail(Reader &file, Size number, Span tail) -> Status
{
    auto temp = tail;
    auto read_size = tail.size();
    Calico_Try(file.read(temp.data(), read_size, number * tail.size()));

    if (read_size == 0) {
        return Status::not_found("end of file");
    } else if (read_size != tail.size()) {
        return Status::system_error("incomplete read");
    }
    return Status::ok();
}

WalReader::WalReader(Reader &file, Span tail)
    : m_tail {tail},
      m_file {&file}
{}

auto WalReader::read(Span &payload) -> Status
{
    if (m_offset + m_block == 0) {
        Calico_Try(read_tail(*m_file, 0, m_tail));
    }
    auto out = payload;
    WalRecordHeader header;

    for (;;) {
        const auto has_enough_space = m_tail.size() > m_offset + WalRecordHeader::SIZE;
        auto rest = m_tail.range(m_offset);

        if (has_enough_space && WalRecordHeader::contains_record(rest)) {
            const auto temp = read_wal_record_header(rest);
            rest.advance(WalRecordHeader::SIZE);

            Calico_Try(merge_records_left(header, temp));
            if (temp.size == 0 || temp.size > rest.size()) {
                return Status::corruption("fragment size is invalid");
            }
            mem_copy(out, rest.truncate(temp.size));
            m_offset += WalRecordHeader::SIZE + temp.size;
            out.advance(temp.size);

            if (header.type == WalRecordHeader::Type::FULL) {
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
            if (s.is_not_found() && header.type != WalRecordHeader::EMPTY) {
                return Status::corruption("encountered a partial record");
            }
            return s;
        }
        m_offset = 0;
    }
    return Status::ok();
}

auto WalReader::offset() const -> Size
{
    return m_offset + m_block * m_tail.size();
}

} // namespace Calico
#include "reader.h"
#include "calico/storage.h"
#include "pager/page.h"
#include "utils/logging.h"

namespace Calico {

[[nodiscard]]
static auto read_exact_or_eof(Reader &file, Size offset, Span out) -> Status
{
    auto temp = out;
    auto read_size = out.size();
    Calico_Try_S(file.read(temp.data(), read_size, offset));

    if (read_size == 0) {
        return Status::not_found("reached the end of the file");
    } else if (read_size != out.size()) {
        return Status::system_error("incomplete read");
    }
    return Status::ok();
}

auto LogReader::read(WalPayloadOut &out, Span payload, Span tail) -> Status
{
    CALICO_EXPECT_NE(m_file, nullptr);

    // Lazily read the first block into the tail buffer.
    if (m_offset == 0) {
        Calico_Try_S(read_exact_or_eof(*m_file, 0, tail));
    }

    auto s = read_logical_record(payload, tail);
    if (s.is_ok()) {
        out = WalPayloadOut {payload};
    }
    return s;
}

auto LogReader::read_first_lsn(Lsn &out) -> Status
{
    char buffer [WalRecordHeader::SIZE + WalPayloadHeader::SIZE];
    Span bytes {buffer, sizeof(buffer)};

    // The LogWriter will never flush a block unless it contains at least one record, so the first record should be
    // located at the start of the file.
    auto s = read_exact_or_eof(*m_file, WalRecordHeader::SIZE, bytes);
    if (s.is_ok()) {
        out = read_wal_payload_header(bytes).lsn;
    }
    return s;
}

auto LogReader::read_logical_record(Span &out, Span tail) -> Status
{
    WalRecordHeader header;
    auto payload = out;

    for (; ; ) {
        auto rest = tail;
        const auto has_enough_space = tail.size() - m_offset > WalRecordHeader::SIZE;

        if (has_enough_space) {
            // Note that this modifies rest to point to [<local>, <end>) in the tail buffer.
            if (WalRecordHeader::contains_record(rest.advance(m_offset))) {
                const auto temp = read_wal_record_header(rest);
                rest.advance(WalRecordHeader::SIZE);

                Calico_Try_S(merge_records_left(header, temp));
                if (!temp.size || temp.size > rest.size()) {
                    return Status::corruption("fragment size is invalid");
                }
                mem_copy(payload, rest.truncate(temp.size));
                m_offset += WalRecordHeader::SIZE + temp.size;
                payload.advance(temp.size);
                rest.advance(temp.size);

                if (header.type == WalRecordHeader::Type::FULL) {
                    if (header.crc != crc32c::Value(out.data(), out.truncate(header.size).size())) {
                        return Status::corruption("crc is incorrect");
                    }
                    break;
                }
                if (!rest.is_empty()) {
                    continue;
                }
            }
        }

        // Read the next block into the tail buffer.
        auto s = read_exact_or_eof(*m_file, (m_number+1) * tail.size(), tail);

        if (!s.is_ok()) {
            return s;
        }
        m_offset = 0;
        m_number++;
    }
    return Status::ok();
}

auto WalReader::open() -> Status
{
    const auto first = m_set->id_after(Id::null());
    if (first.is_null()) {
        return Status::not_found("segment collection is empty");
    }
    return open_segment(first);
}

auto WalReader::seek_next() -> Status
{
    const auto next = m_set->id_after(m_current);
    if (!next.is_null()) {
        close_segment();

        if (auto s = open_segment(next); !s.is_ok()) {
            return s.is_not_found() ? Status::corruption("missing wal segment") : s;
        }
        return Status::ok();
    }
    return Status::not_found("reached the last segment");
}

auto WalReader::seek_previous() -> Status
{
    const auto previous = m_set->id_before(m_current);
    if (!previous.is_null()) {
        close_segment();
        return open_segment(previous);
    }
    return Status::not_found("reached the first segment");
}

auto WalReader::read_first_lsn(Id &out) -> Status
{
    prepare_traversal();
    Calico_Try_S(m_reader->read_first_lsn(out));
    m_set->set_first_lsn(m_current, out);
    return Status::ok();
}

auto WalReader::roll(const Callback &callback) -> Status
{
    auto s = Status::ok();
    prepare_traversal();

    while (s.is_ok()) {
        WalPayloadOut payload;

        // If this call succeeds, payload will be modified to point to the exact payload.
        s = m_reader->read(payload, m_data, m_tail);

        // A "not found" error means we have reached EOF.
        if (s.is_not_found()) {
            return Status::ok();
        } else if (!s.is_ok()) {
            return s;
        }
        s = callback(payload);
    }
    return s;
}

auto WalReader::open_segment(Id id) -> Status
{
    CALICO_EXPECT_EQ(m_reader, std::nullopt);
    Reader *file;
    auto s = m_store->new_reader(encode_segment_name(m_prefix, id), &file);
    if (s.is_ok()) {
        m_file.reset(file);
        m_reader = LogReader {*m_file};
        m_current = id;
    }
    return s;
}

auto WalReader::close_segment() -> void
{
    if (m_reader) {
        m_current = Id::null();
        m_reader.reset();
        m_file.reset();
    }
}

auto WalReader::prepare_traversal() -> void
{
    m_reader = LogReader {*m_file};
}

} // namespace Calico
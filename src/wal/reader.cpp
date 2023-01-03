#include "reader.h"
#include "calico/storage.h"
#include "page/page.h"
#include "utils/system.h"

namespace Calico {

template<class ...Args>
[[nodiscard]]
auto read_corruption_error(const std::string &hint_fmt, Args &&...args) -> Status
{
    const auto hint_str = fmt::format(fmt::runtime(hint_fmt), std::forward<Args>(args)...);
    return corruption("cannot raed WAL record: record is corrupted ({})", hint_str);
}

[[nodiscard]]
static auto read_exact_or_eof(RandomReader &file, Size offset, Bytes out) -> Status
{
    auto temp = out;
    CALICO_TRY_S(file.read(temp, offset));

    if (temp.is_empty()) {
        return not_found("reached the end of the file");
    } else if (temp.size() != out.size()) {
        return system_error("incomplete read");
    }
    return ok();
}

auto LogReader::read(WalPayloadOut &out, Bytes payload, Bytes tail) -> Status
{
    // Lazily read the first block into the tail buffer.
    if (m_offset == 0)
        CALICO_TRY_S(read_exact_or_eof(*m_file, 0, tail));

    auto s = read_logical_record(payload, tail);
    if (s.is_ok()) out = WalPayloadOut {payload};
    return s;
}

auto LogReader::read_first_lsn(Id &out) -> Status
{
    // Bytes requires the array size when constructed with a C-style array.
    char buffer [WalPayloadHeader::SIZE];
    Bytes bytes {buffer, sizeof(buffer)};

    // The LogWriter will never flush a block unless it contains at least one record, so the first record should be
    // located at the start of the file.
    auto s = read_exact_or_eof(*m_file, WalRecordHeader::SIZE, bytes);
    if (s.is_ok()) out = read_wal_payload_header(bytes).lsn;
    return s;
}

auto LogReader::read_logical_record(Bytes &out, Bytes tail) -> Status
{
    WalRecordHeader header {};
    auto payload = out;

    for (; ; ) {
        auto rest = tail;
        const auto has_enough_space = tail.size() - m_offset > WalRecordHeader::SIZE;

        if (has_enough_space) {
            // Note that this modifies rest to point to [<local>, <end>) in the tail buffer.
            if (WalRecordHeader::contains_record(rest.advance(m_offset))) {
                const auto temp = read_wal_record_header(rest);
                rest.advance(WalRecordHeader::SIZE);

                CALICO_TRY_S(merge_records_left(header, temp));

                mem_copy(payload, rest.truncate(temp.size));
                m_offset += WalRecordHeader::SIZE + temp.size;
                payload.advance(temp.size);
                rest.advance(temp.size);

                if (header.type == WalRecordHeader::Type::FULL)
                    break;

                if (!rest.is_empty())
                    continue;
            }
        }
        // Read the next block into the tail buffer.
        auto s = read_exact_or_eof(*m_file, (m_number+1) * tail.size(), tail);

        if (!s.is_ok()) {
            // If we have any record fragments read so far, we consider this corruption.
            if (s.is_not_found() && header.type != WalRecordHeader::Type {})
                return read_corruption_error("logical record is incomplete");
            return s;
        }
        m_offset = 0;
        m_number++;
    }
    // Only modify the out parameter's size if we have succeeded.
    out.truncate(out.size() - payload.size());
    return ok();
}

auto WalReader::open() -> Status
{
    const auto first = m_set->id_after(SegmentId::null());
    if (first.is_null())
        return not_found("could not open WAL reader: segment collection is empty");
    return open_segment(first);
}

auto WalReader::seek_next() -> Status
{
    const auto next = m_set->id_after(m_current);
    if (!next.is_null()) {
        close_segment();
        return open_segment(next);
    }
    return not_found("could not seek to next segment: reached the last segment");
}

auto WalReader::seek_previous() -> Status
{
    const auto previous = m_set->id_before(m_current);
    if (!previous.is_null()) {
        close_segment();
        return open_segment(previous);
    }
    return Status::not_found("could not seek to previous segment: reached the first segment");
}

auto WalReader::read_first_lsn(Id &out) -> Status
{
    prepare_traversal();
    return m_reader->read_first_lsn(out);
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
            return ok();
        } else if (!s.is_ok()) {
            return s;
        }
        s = callback(payload);
    }
    return s;
}

auto WalReader::open_segment(SegmentId id) -> Status
{
    CALICO_EXPECT_EQ(m_reader, std::nullopt);
    RandomReader *file {}; // TODO: SequentialReader could be useful for this class!
    auto s = m_store->open_random_reader(m_prefix + id.to_name(), &file);
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
        m_current = SegmentId::null();
        m_reader.reset();
        m_file.reset();
    }
}

auto WalReader::prepare_traversal() -> void
{
    m_reader = LogReader {*m_file};
}

} // namespace Calico
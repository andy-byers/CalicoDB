#include "reader.h"
#include "calico/storage.h"
#include "page/page.h"
#include "utils/logging.h"

namespace calico {

template<class ...Args>
[[nodiscard]]
auto read_corruption_error(const std::string &hint, Args &&...args) -> Status
{
    ThreePartMessage message;
    message.set_primary("cannot read WAL record");
    message.set_detail("record is corrupted");
    message.set_hint(hint, std::forward<Args>(args)...);
    return message.corruption();
}

auto unrecognized_type_error(WalPayloadType type) -> Status
{
    return read_corruption_error("record type \"{:02X}\" is not recognized", static_cast<Byte>(type));
}

[[nodiscard]]
static auto read_exact_or_eof(RandomReader &file, Size offset, Bytes out) -> Status
{
    auto temp = out;
    auto s = file.read(temp, offset);
    if (!s.is_ok()) return s;

    if (temp.is_empty()) {
        return Status::logic_error("reached the end of the file");
    } else if (temp.size() != out.size()) {
        return Status::system_error("incomplete read");
    }
    return s;
}

auto LogReader::read(Bytes &out, Bytes tail) -> Status
{
    // Lazily read the first block into the tail buffer.
    if (m_offset == 0) {
        auto s = read_exact_or_eof(*m_file, 0, tail);
        if (!s.is_ok()) return s;
    }
    return read_logical_record(out, tail);
}

auto LogReader::read_first_lsn(SequenceId &out) -> Status
{
    // Bytes requires the array size when constructed with a C-style array.
    char buffer [sizeof(WalRecordHeader) + MINIMUM_PAYLOAD_SIZE];
    Bytes bytes {buffer, sizeof(buffer)};

    // The LogWriter will never flush a block unless it contains at least one record, so the first record should be
    // located at the start of the file.
    auto s = read_exact_or_eof(*m_file, 0, bytes);
    if (s.is_ok())
        out = decode_lsn(bytes.advance(sizeof(WalRecordHeader)));
    return s;
}

auto LogReader::read_logical_record(Bytes &out, Bytes tail) -> Status
{
    WalRecordHeader header {};
    auto payload = out;

    for (; ; ) {
        auto rest = tail;
        const auto has_enough_space = tail.size() - m_offset > sizeof(header);

        if (has_enough_space) {
            // Note that this modifies rest to point to [<local>, <end>) in the tail buffer.
            if (WalRecordHeader::contains_record(rest.advance(m_offset))) {
                const auto temp = read_wal_record_header(rest);
                rest.advance(sizeof(temp));

                auto s = merge_records_left(header, temp);
                if (!s.is_ok()) return s;

                mem_copy(payload, rest.truncate(temp.size));
                m_offset += sizeof(temp) + temp.size;
                payload.advance(temp.size);
                rest.advance(temp.size);

                if (header.type == WalRecordHeader::Type::FULL)
                    break;

                if (!rest.is_empty())
                    continue;
            }
        }
        m_offset = 0;
        m_number++;

        // Read the next block into the tail buffer.
        auto s = read_exact_or_eof(*m_file, m_number * tail.size(), tail);

        if (!s.is_ok()) {
            // If we have any record fragments read so far, we consider this corruption.
            if (s.is_logic_error() && header.type != WalRecordHeader::Type {})
                return read_corruption_error("logical record is incomplete");
            return s;
        }
    }
    // Only modify the out parameter size if we have succeeded.
    out.truncate(out.size() - payload.size());
    return Status::ok();
}

auto WalReader::open() -> Status
{
    const auto first = m_segments->id_after(SegmentId::null());
    if (first.is_null())
        return Status::not_found("could not open WAL reader: segment collection is empty");
    return open_segment(first);
}

auto WalReader::seek_next() -> Status
{
    const auto next = m_segments->id_after(m_current);
    if (!next.is_null()) {
        close_segment();
        return open_segment(next);
    }
    return Status::not_found("could not seek to next segment: reached the last segment");
}

auto WalReader::seek_previous() -> Status
{
    const auto previous = m_segments->id_before(m_current);
    if (!previous.is_null()) {
        close_segment();
        return open_segment(previous);
    }
    return Status::not_found("could not seek to previous segment: reached the first segment");
}

auto WalReader::read_first_lsn(SequenceId &out) -> Status
{
    prepare_traversal();
    return m_reader->read_first_lsn(out);
}

auto WalReader::roll(const GetPayload &callback) -> Status
{
    auto s = Status::ok();
    prepare_traversal();

    while (s.is_ok()) {
        Bytes payload {m_data};

        // If this call succeeds, payload will be modified to point to the exact payload.
        s = m_reader->read(payload, m_tail);

        // A logic error means we have reached EOF.
        if (s.is_logic_error()) {
            return Status::ok();
        } else if (!s.is_ok()) {
            return s;
        }

        const auto type = decode_payload_type(payload);
        switch (type) {
            case WalPayloadType::DELTAS:
                s = callback(decode_deltas_payload(payload));
                break;
            case WalPayloadType::COMMIT:
                s = callback(decode_commit_payload(payload));
                break;
            case WalPayloadType::FULL_IMAGE:
                s = callback(decode_full_image_payload(payload));
                break;
            default:
                return unrecognized_type_error(type);
        }
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

} // namespace calico
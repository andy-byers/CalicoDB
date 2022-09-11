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
        auto s = read_exact_or_eof(*m_file, m_offset, tail);
        if (!s.is_ok()) return s;
    }
    return read_logical_record(out, tail);
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
            if (rest.advance(m_offset)[14] != '\x00') { // TODO: 14 is the offset of the type field. Maybe put the type field at the front? We're removing the LSN so we can't check that to determine presence of a record here.
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

        // Just hit EOF. If we have any record fragments read so far, we consider this corruption.
        if (!s.is_ok()) {
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

}

auto WalReader::roll(const GetPayload &callback) -> Status
{
    auto s = prepare_traversal();

    while (s.is_ok()) {
        Bytes payload {m_payload};
        s = m_reader->read(payload, m_scratch);
        if (s.is_logic_error()) continue;
        if (!s.is_ok()) return s;

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
    CALICO_EXPECT_NE(m_reader, std::nullopt);
    m_current = SegmentId::null();
    m_reader.reset();
    m_file.reset();
}

auto WalReader::prepare_traversal() -> Status
{
    m_reader = LogReader {*m_file};
}




auto BasicWalReader::open(SegmentId id) -> Status
{
    RandomReader *file {};
    const auto path = m_prefix + id.to_name();
    auto s = m_store->open_random_reader(path, &file);
    if (!s.is_ok()) return s;

    m_segment_id = id;
    return m_reader.attach(file);
}

auto BasicWalReader::close() -> Status
{
    if (m_reader.is_attached())
        delete m_reader.detach();
    return Status::ok();
}

auto BasicWalReader::read_first_lsn(SequenceId &lsn) -> Status
{
    auto s = prepare_traversal();
    if (s.is_ok()) {
        CALICO_EXPECT_TRUE(m_reader.position().is_start());
//        lsn.value = read_wal_record_header(m_reader.remaining()).lsn;

        if (lsn.is_null()) {
            ThreePartMessage message;
            message.set_primary("cannot read first LSN");
            message.set_detail("segment is empty");
            return message.logic_error();
        }
    }
    return s;
}

auto BasicWalReader::redo(const RedoCallback &callback) -> Status
{
    auto s = prepare_traversal();
    if (!s.is_ok()) return s;

    for (; ; ) {
        auto payload = stob(m_payload);
        WalRecordHeader header {};

        s = read_logical_record(header, payload);
        if (s.is_logic_error()) return Status::ok(); // EOF
        if (!s.is_ok()) return s;

        const auto type = decode_payload_type(payload);
        switch (type) {
            case WalPayloadType::DELTAS:
                s = callback(decode_deltas_payload(header, payload));
                break;
            case WalPayloadType::COMMIT:
                s = callback(decode_commit_payload(header, payload));
                break;
            case WalPayloadType::FULL_IMAGE:
                break;
            default:
                return unrecognized_type_error(type);
        }
        if (!s.is_ok()) return s;
    }
}

auto BasicWalReader::undo(const UndoCallback &callback) -> Status
{
    auto s = prepare_traversal();
    if (!s.is_ok()) return s;

    for (; ; ) {
        auto payload = stob(m_payload);
        WalRecordHeader header {};

        s = read_logical_record(header, payload);
        if (s.is_logic_error()) return Status::ok(); // EOF
        if (!s.is_ok()) return s;

        const auto type = decode_payload_type(payload);
        switch (type) {
            case WalPayloadType::FULL_IMAGE:
                s = callback(decode_full_image_payload(payload.truncate(header.size)));
                break;
            case WalPayloadType::DELTAS:
                break;
            case WalPayloadType::COMMIT:
                CALICO_EXPECT_TRUE(false && "error: encountered a commit record during undo");
                break;
            default:
                return unrecognized_type_error(type);
        }
        if (s.is_corruption()) {

        }
        if (!s.is_ok()) return s;
    }
}

auto BasicWalReader::prepare_traversal() -> Status
{
    CALICO_EXPECT_TRUE(m_reader.is_attached());
    return m_reader.reset_position();
}

auto BasicWalReader::read_logical_record(WalRecordHeader &header, Bytes payload) -> Status
{
//    CALICO_EXPECT_EQ(header.lsn, 0);
    for (auto &reader = m_reader; ; ) {
        if (contains_record(reader.remaining())) {
            const auto temp = read_wal_record_header(reader.remaining());
            reader.advance_cursor(sizeof(temp));

            auto s = merge_records_left(header, temp);
            if (!s.is_ok()) return s;

            mem_copy(payload, reader.remaining().truncate(temp.size));
            reader.advance_cursor(temp.size);
            payload.advance(temp.size);

            if (header.type == WalRecordHeader::Type::FULL)
                break;
        } else {
            auto s = reader.advance_block();

            // Just hit EOF. If we have any record fragments read so far, we consider this corruption.
            if (!s.is_ok()) {
//                if (s.is_logic_error() && header.lsn != 0)
//                    return read_corruption_error("logical record with LSN {} is incomplete", header.lsn);
                return s;
            }
        }
    }
    return Status::ok();
}

} // namespace calico
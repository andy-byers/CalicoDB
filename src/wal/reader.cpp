#include "reader.h"
#include "calico/storage.h"
#include "page/page.h"
#include "utils/logging.h"

namespace calico {

auto BasicWalReader::open(SegmentId id) -> Status
{
    RandomReader *file {};
    const auto path = m_prefix + id.to_name();
    auto s = m_store->open_random_reader(path, &file);
    if (!s.is_ok()) return s;

    m_segment_id = id;
    return m_redo_reader.attach(file);
}

auto BasicWalReader::close() -> Status
{
    // We only use one file pointer between the two reader objects.
    if (m_redo_reader.is_attached()) {
        delete m_redo_reader.detach();
    } else if (m_undo_reader.is_attached()) {
        delete m_undo_reader.detach();
    }
    return Status::ok();
}

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

auto BasicWalReader::read_first_lsn(SequenceId &lsn) -> Status
{
    auto s = prepare_forward_traversal();
    if (s.is_ok()) {
        CALICO_EXPECT_TRUE(m_redo_reader.position().is_start());
        lsn.value = read_wal_record_header(m_redo_reader.remaining()).lsn;

        if (lsn.is_null()) {
            ThreePartMessage message;
            message.set_primary("cannot read first LSN");
            message.set_detail("segment is empty");
            return message.logic_error();
        }
    }
    return s;
}

auto BasicWalReader::redo(PositionList &out, const RedoCallback &callback) -> Status
{
    auto s = prepare_forward_traversal();
    if (!s.is_ok()) return s;

    for (; ; ) {
        auto payload = stob(m_payload);
        WalRecordHeader header {};

        PositionList temp;
        s = forward_read_logical_record(header, payload, temp);
        if (s.is_logic_error()) return Status::ok(); // EOF
        if (!s.is_ok()) return s;

        const auto type = read_payload_type(payload);
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
        out.insert(cend(out), cbegin(temp), cend(temp));
    }
}

auto BasicWalReader::undo(const UndoIterator &begin, const UndoIterator &end, const UndoCallback &callback) -> Status
{
    auto s = prepare_reverse_traversal();
    if (!s.is_ok()) return s;

    for (auto itr = begin; itr != end; ) {
        CALICO_EXPECT_EQ(m_segment_id, itr->id);
        auto payload = stob(m_payload);
        WalRecordHeader header {};

        s = reverse_read_logical_record(header, payload, itr, end);
        if (!s.is_ok()) return s;

        // We had to read into the payload buffer from the back end. Adjust so that the payload is pointing at the correct data.
        payload = payload.range(payload.size() - header.size);

        const auto type = read_payload_type(payload);
        switch (type) {
            case WalPayloadType::FULL_IMAGE:
                s = callback(decode_full_image_payload(payload));
                if (!s.is_ok()) return s;
                break;
            case WalPayloadType::DELTAS:
                break;
            case WalPayloadType::COMMIT:
                CALICO_EXPECT_TRUE(false && "error: encountered a commit record during undo");
                break;
            default:
                return unrecognized_type_error(type);
        }
    }
    return Status::ok();
}

auto BasicWalReader::prepare_forward_traversal() -> Status
{
    if (m_undo_reader.is_attached()) {
        auto s = m_redo_reader.attach(m_undo_reader.detach());
        if (!s.is_ok()) return s;
    }
    CALICO_EXPECT_TRUE(m_redo_reader.is_attached());
    if (!m_redo_reader.position().is_start())
        return m_redo_reader.reset_position();
    return Status::ok();
}

auto BasicWalReader::prepare_reverse_traversal() -> Status
{
    auto s = Status::ok();
    if (m_redo_reader.is_attached())
        s = m_undo_reader.attach(m_redo_reader.detach());
    CALICO_EXPECT_TRUE(m_undo_reader.is_attached());
    return s;
}

auto BasicWalReader::forward_read_logical_record(WalRecordHeader &header, Bytes payload, PositionList &positions) -> Status
{
    CALICO_EXPECT_EQ(header.lsn, 0);

    for (auto &reader = m_redo_reader; ; ) {
        if (contains_record(reader.remaining())) {
            positions.emplace_back(RecordPosition{ m_segment_id, reader.position()});
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
                if (s.is_logic_error() && header.lsn != 0)
                    return read_corruption_error("logical record with LSN {} is incomplete", header.lsn);
                return s;
            }
        }
    }
    return Status::ok();
}

auto BasicWalReader::reverse_read_logical_record(WalRecordHeader &header, Bytes payload, UndoIterator &itr, const UndoIterator &end) -> Status
{
    auto offset = payload.size();
    auto &reader = m_undo_reader;
    Bytes bytes;

    for (; itr != end; ) {
        // Get a slice of the reader's tail buffer at the given position.
        auto s = reader.present(itr->pos, bytes);
        if (!s.is_ok()) return s;

        auto temp = read_wal_record_header(bytes);
        bytes.advance(sizeof(temp));

        s = merge_records_right(temp, header);
        if (!s.is_ok()) return s;

        mem_copy(payload.range(offset - temp.size, temp.size), bytes.range(0, temp.size));
        offset -= temp.size;
        itr++;

        if (header.type == WalRecordHeader::Type::FULL)
            break;
    }
    return Status::ok();
}

} // namespace calico
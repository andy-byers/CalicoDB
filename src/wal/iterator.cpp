#include "iterator.h"
#include "calico/storage.h"
#include "page/page.h"
#include "utils/logging.h"

namespace calico {

template<class ...Args>
[[nodiscard]]
static auto read_corruption_error(const std::string &hint, Args &&...args) -> Status
{
    ThreePartMessage message;
    message.set_primary("cannot read WAL record");
    message.set_detail("record is corrupted");
    message.set_hint(hint, std::forward<Args>(args)...);
    return message.corruption();
}

[[nodiscard]]
static auto unrecognized_type_error(WalPayloadType type) -> Status
{
    return read_corruption_error("record type `\\x{:02X}` is not recognized", static_cast<Byte>(type));
}

auto BasicWalIterator::open_segment(SegmentId id) -> Status
{
    RandomReader *file {};
    const auto path = m_prefix + id.to_name();
    auto s = m_store->open_random_reader(path, &file);
    if (s.is_ok()) {
        m_segment_id = id;
        m_positions.clear();
        return m_redo_reader.attach(file);
    }
    return s;
}

auto BasicWalIterator::close_segment() -> void
{
    // We only use one file pointer between the two reader objects.
    if (m_redo_reader.is_attached()) {
        delete m_redo_reader.detach();
    } else if (m_undo_reader.is_attached()) {
        delete m_undo_reader.detach();
    }
    m_segment_id = SegmentId::null();
}

auto BasicWalIterator::open() -> Status
{
    const auto first = m_collection->id_after(SegmentId::null());
    if (first.is_null()) return Status::not_found("could not open WAL iterator: segments collection is empty");
    return open_segment(first);
}

auto BasicWalIterator::read_first_lsn(SequenceId &lsn) -> Status
{
    // NOOP if we're already at the start.
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

auto BasicWalIterator::fast_read_through() -> Status
{
    auto s = prepare_forward_traversal();
    if (!s.is_ok()) return s;

    for (; ; ) {
        WalRecordHeader header {};
        PositionList temp;

        s = forward_handle_logical_record(header, temp, [](auto) {});
        if (s.is_logic_error()) return Status::ok(); // EOF
        if (!s.is_ok()) return s;

        m_positions.insert(cend(m_positions), cbegin(temp), cend(temp));
    }
}

auto BasicWalIterator::redo(const RedoCallback &callback) -> Status
{
    auto s = prepare_forward_traversal();
    if (!s.is_ok()) return s;

    for (; ; ) {
        auto payload = stob(m_payload);
        WalRecordHeader header {};
        PositionList temp;

        s = forward_handle_logical_record(header, temp, [&payload](BytesView chunk) {
            mem_copy(payload, chunk);
            payload.advance(chunk.size());
        });
        if (s.is_logic_error()) return Status::ok(); // EOF
        if (!s.is_ok()) return s;

        const auto type = read_payload_type(m_payload);
        switch (type) {
            case WalPayloadType::DELTAS:
                s = callback(decode_deltas_payload(header, m_payload));
                break;
            case WalPayloadType::COMMIT:
                s = callback(decode_commit_payload(header, m_payload));
                break;
            case WalPayloadType::FULL_IMAGE:
                break;
            default:
                return unrecognized_type_error(type);
        }
        if (!s.is_ok()) return s;
        m_positions.insert(cend(m_positions), cbegin(temp), cend(temp));
    }
}

auto BasicWalIterator::undo(const UndoCallback &callback) -> Status
{
    auto s = Status::ok();

    // If we don't already have record positions, get them as quicly as possible.
    if (m_positions.empty()) {
        s = fast_read_through();
        if (!s.is_ok()) return s;
    }
    s = prepare_reverse_traversal();
    if (!s.is_ok()) return s;

    for (auto itr = crbegin(m_positions); itr != crend(m_positions); ) {
        CALICO_EXPECT_EQ(m_segment_id, itr->id);
        auto payload = stob(m_payload);
        WalRecordHeader header {};

        s = reverse_read_logical_record(header, payload, itr, crend(m_positions));
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

auto BasicWalIterator::prepare_forward_traversal() -> Status
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

auto BasicWalIterator::prepare_reverse_traversal() -> Status
{
    auto s = Status::ok();
    if (m_redo_reader.is_attached())
        s = m_undo_reader.attach(m_redo_reader.detach());
    CALICO_EXPECT_TRUE(m_undo_reader.is_attached());
    return s;
}

auto BasicWalIterator::seek_next_segment() -> Status
{
    auto &segments = m_collection->segments();
    auto itr = segments.upper_bound({m_segment_id});
    if (itr == cend(segments))
        return Status::not_found("could not seek to next segment: reached the last segment");
    close_segment();
    return open_segment(itr->id);
}

auto BasicWalIterator::seek_previous_segment() -> Status
{
    auto &segments = m_collection->segments();
    auto itr = segments.lower_bound({m_segment_id});
    CALICO_EXPECT_NE(itr, cend(segments));
    CALICO_EXPECT_EQ(itr->id, m_segment_id);
    if (itr == cbegin(segments))
        return Status::not_found("could not seek to previous segment: reached the first segment");
    close_segment();
    return open_segment(prev(itr)->id);
}

// Call a callback for each piece of payload belonging to a logical record.
auto BasicWalIterator::forward_handle_logical_record(WalRecordHeader &header, PositionList &positions, const std::function<void(BytesView)> &callback) -> Status
{
    CALICO_EXPECT_EQ(header.lsn, 0);

    for (auto &reader = m_redo_reader; ; ) {
        if (contains_record(reader.remaining())) {
            positions.emplace_back(RecordPosition{ m_segment_id, reader.position()});
            const auto temp = read_wal_record_header(reader.remaining());
            reader.advance_cursor(sizeof(temp));

            auto s = merge_records_left(header, temp);
            if (!s.is_ok()) return s;

            callback(reader.remaining().truncate(temp.size));
            reader.advance_cursor(temp.size);

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

auto BasicWalIterator::reverse_read_logical_record(WalRecordHeader &header, Bytes payload, UndoIterator &itr, const UndoIterator &end) -> Status
{
    auto offset = payload.size();
    auto &reader = m_undo_reader;
    Bytes bytes;

    for (; itr != end; ) {
        // Get a slice of the reader's tail buffer at the given position.
        auto s = reader.fetch_at(itr->pos, bytes);
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
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
    return m_reader.attach(file);
}

auto BasicWalReader::close() -> Status
{
    if (m_reader.is_attached())
        delete m_reader.detach();
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
    auto s = prepare_traversal();
    if (s.is_ok()) {
        CALICO_EXPECT_TRUE(m_reader.position().is_start());
        lsn.value = read_wal_record_header(m_reader.remaining()).lsn;

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

        const auto type = read_payload_type(payload);
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
    if (!m_reader.position().is_start())
        return m_reader.reset_position();
    return Status::ok();
}

auto BasicWalReader::read_logical_record(WalRecordHeader &header, Bytes payload) -> Status
{
    CALICO_EXPECT_EQ(header.lsn, 0);
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
                if (s.is_logic_error() && header.lsn != 0)
                    return read_corruption_error("logical record with LSN {} is incomplete", header.lsn);
                return s;
            }
        }
    }
    return Status::ok();
}

} // namespace calico
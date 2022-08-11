//#include "wal_record.h"
//#include "page/update.h"
//#include "utils/crc.h"
//#include "utils/encoding.h"
//#include "utils/logging.h"
//
//namespace cco {
//
//namespace {
//
//    inline auto is_record_type_valid(WALRecord::Type type) -> bool
//    {
//        return type == WALRecord::Type::FIRST ||
//               type == WALRecord::Type::MIDDLE ||
//               type == WALRecord::Type::LAST ||
//               type == WALRecord::Type::FULL;
//    }
//
//} // namespace
//
//WALPayload::WALPayload(const PageUpdate &param, Bytes scratch)
//{
//    m_data = scratch;
//    auto bytes = scratch;
//
//    put_u64(bytes, param.last_lsn.value);
//    bytes.advance(sizeof(param.last_lsn.value));
//
//    put_u64(bytes, param.page_id.value);
//    bytes.advance(sizeof(param.page_id.value));
//
//    put_u16(bytes, static_cast<std::uint16_t>(param.changes.size()));
//    bytes.advance(sizeof(std::uint16_t));
//
//    for (const auto &change: param.changes) {
//        const auto size = change.before.size();
//        CCO_EXPECT_EQ(size, change.after.size());
//
//        put_u16(bytes, static_cast<std::uint16_t>(change.offset));
//        bytes.advance(sizeof(std::uint16_t));
//
//        put_u16(bytes, static_cast<std::uint16_t>(size));
//        bytes.advance(sizeof(std::uint16_t));
//
//        mem_copy(bytes, change.before, size);
//        bytes.advance(size);
//
//        mem_copy(bytes, change.after, size);
//        bytes.advance(size);
//    }
//    CCO_EXPECT_GE(m_data.size(), bytes.size());
//    m_data.truncate(m_data.size() - bytes.size());
//}
//
//auto WALPayload::is_commit() const -> bool
//{
//    const auto update = decode();
//    return update.page_id.is_null() && update.changes.empty();
//}
//
//auto WALPayload::decode() const -> PageUpdate
//{
//    auto update = PageUpdate {};
//    auto bytes = m_data;
//
//    update.last_lsn.value = get_u64(bytes);
//    bytes.advance(sizeof(update.last_lsn.value));
//
//    update.page_id.value = get_u64(bytes);
//    bytes.advance(sizeof(update.page_id.value));
//
//    update.changes.resize(get_u16(bytes));
//    bytes.advance(sizeof(std::uint16_t));
//
//    for (auto &region: update.changes) {
//        region.offset = get_u16(bytes);
//        bytes.advance(sizeof(std::uint16_t));
//
//        const auto region_size = get_u16(bytes);
//        bytes.advance(sizeof(std::uint16_t));
//
//        region.before = bytes.range(0, region_size);
//        bytes.advance(region_size);
//
//        region.after = bytes.range(0, region_size);
//        bytes.advance(region_size);
//    }
//    CCO_EXPECT_TRUE(bytes.is_empty());
//    return update;
//}
//
//auto WALRecord::commit(SequenceNumber commit_lsn, Bytes scratch) -> WALRecord
//{
//    return WALRecord {{
//        {},
//        PageId::null(),
//        SequenceNumber::null(),
//        commit_lsn,
//    }, scratch};
//}
//
//WALRecord::WALRecord(const PageUpdate &update, Bytes scratch)
//    : m_payload {update, scratch},
//      m_lsn {update.page_lsn},
//      m_backing {scratch},
//      m_crc {crc_32(m_payload.data())},
//      m_type {Type::FULL}
//{}
//
//auto WALRecord::read(BytesView in) -> Result<bool>
//{
//    static constexpr auto ERROR_PRIMARY = "cannot read WAL record";
//
//    // lsn (4B)
//    m_lsn.value = get_u64(in);
//    in.advance(sizeof(m_lsn.value));
//
//    // No more values in the buffer (empty space in the buffer must be zeroed and LSNs
//    // start with 1).
//    if (m_lsn.is_null())
//        return false;
//
//    // crc (4B)
//    m_crc = get_u32(in);
//    in.advance(sizeof(uint32_t));
//
//    // type (1B)
//    m_type = static_cast<Type>(in[0]);
//    in.advance(sizeof(Type));
//
//    if (!is_record_type_valid(m_type)) {
//        ThreePartMessage message;
//        message.set_primary(ERROR_PRIMARY);
//        message.set_detail("record type 0x{:02X} is unrecognized", int(m_type));
//        return Err {message.corruption()};
//    }
//
//    // x (2B)
//    const auto payload_size = get_u16(in);
//    in.advance(sizeof(uint16_t));
//
//    if (payload_size == 0 || payload_size > in.size()) {
//        ThreePartMessage message;
//        message.set_primary(ERROR_PRIMARY);
//        message.set_detail("payload size {} is out of range", payload_size);
//        message.set_detail("must be in [1, {}]", in.size());
//        return Err {message.corruption()};
//    }
//
//    m_payload.m_data = m_backing.range(0, payload_size);
//
//    // payload (xB)
//    mem_copy(m_payload.m_data, in, payload_size);
//    return true;
//}
//
//auto WALRecord::write(Bytes out) const noexcept -> void
//{
//    CCO_EXPECT_GE(out.size(), size());
//
//    // lsn (8B)
//    put_u64(out, m_lsn.value);
//    out.advance(sizeof(m_lsn.value));
//
//    // crc (4B)
//    put_u32(out, static_cast<std::uint32_t>(m_crc));
//    out.advance(sizeof(std::uint32_t));
//
//    // type (1B)
//    CCO_EXPECT_TRUE(is_record_type_valid(m_type));
//    out[0] = static_cast<Byte>(m_type);
//    out.advance(sizeof(Type));
//
//    // x (2B)
//    const auto payload_size = m_payload.m_data.size();
//    CCO_EXPECT_NE(payload_size, 0);
//    put_u16(out, static_cast<std::uint16_t>(payload_size));
//    out.advance(sizeof(std::uint16_t));
//
//    // payload (xB)
//    mem_copy(out, m_payload.m_data, payload_size);
//}
//
//auto WALRecord::is_consistent() const -> bool
//{
//    CCO_EXPECT_EQ(m_type, Type::FULL);
//    return m_crc == crc_32(m_payload.data());
//}
//
///*
// * Valid Splits:
// *     .-------------------------------.
// *     |  Before  =  Left    +  Right  |
// *     :----------.----------.---------:
// *     |  FULL    |  FIRST   |  LAST   |
// *     |  LAST    |  MIDDLE  |  LAST   |
// *     '----------'----------'---------'
// */
//auto WALRecord::split(Index offset_in_payload) -> WALRecord
//{
//    CCO_EXPECT_LT(offset_in_payload, m_payload.m_data.size());
//    WALRecord rhs;
//    rhs.m_backing = m_backing.range(offset_in_payload); // TODO: Added...
//    rhs.m_payload.m_data = m_payload.m_data; // TODO: Added...
//
//    rhs.m_payload.m_data.advance(offset_in_payload);
//    m_payload.m_data.truncate(offset_in_payload);
//
//
//    rhs.m_lsn = m_lsn;
//    rhs.m_crc = m_crc;
//    rhs.m_type = Type::LAST;
//
//    CCO_EXPECT_NE(m_type, Type::EMPTY);
//    CCO_EXPECT_NE(m_type, Type::FIRST);
//    if (m_type == Type::FULL) {
//        m_type = Type::FIRST;
//    } else {
//        CCO_EXPECT_EQ(m_type, Type::LAST);
//        m_type = Type::MIDDLE;
//    }
//    return rhs;
//}
//
///*
// * Valid Merges:
// *     .-------------------------------.
// *     |  Left    +  Right   =  After  |
// *     :----------.----------.---------:
// *     |  EMPTY   |  FIRST   |  FIRST  |
// *     |  EMPTY   |  FULL    |  FULL   |
// *     |  FIRST   |  MIDDLE  |  FIRST  |
// *     |  FIRST   |  LAST    |  FULL   |
// *     '----------'----------'---------'
// */
//auto WALRecord::merge(const WALRecord &rhs) -> Result<void>
//{
//    static constexpr auto ERROR_PRIMARY = "cannot merge WAL records";
//
//    CCO_EXPECT_TRUE(is_record_type_valid(rhs.m_type));
//    const auto new_size = m_payload.m_data.size() + rhs.m_payload.m_data.size();
//    m_payload.m_data = m_backing;
//    m_payload.append(rhs.m_payload);
//    m_payload.m_data.truncate(new_size);
//
//    if (m_type == Type::EMPTY) {
//        if (rhs.m_type == Type::MIDDLE || rhs.m_type == Type::LAST) {
//            ThreePartMessage message;
//            message.set_primary(ERROR_PRIMARY);
//            message.set_detail("record types are incompatible");
//            return Err {message.corruption()};
//        }
//
//        m_type = rhs.m_type;
//        m_lsn = rhs.m_lsn;
//        m_crc = rhs.m_crc;
//
//    } else {
//        CCO_EXPECT_EQ(m_type, Type::FIRST);
//
//        if (m_lsn != rhs.m_lsn || m_crc != rhs.m_crc) {
//            ThreePartMessage message;
//            message.set_primary(ERROR_PRIMARY);
//            message.set_detail("parts do not belong to the same logical record");
//            return Err {message.corruption()};
//        }
//
//        // We have just completed a record.
//        if (rhs.m_type == Type::LAST)
//            m_type = Type::FULL;
//    }
//    return {};
//}
//
//} // namespace cco
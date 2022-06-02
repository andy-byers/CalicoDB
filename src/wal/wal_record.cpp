
#include "exception.h"
#include "wal_record.h"
#include "utils/crc.h"
#include "utils/encoding.h"

namespace cub {

namespace {

    inline auto is_record_type_valid(WALRecord::Type type) -> bool
    {
        return type == WALRecord::Type::EMPTY ||
               type == WALRecord::Type::FIRST ||
               type == WALRecord::Type::MIDDLE ||
               type == WALRecord::Type::LAST ||
               type == WALRecord::Type::FULL;
    }

} // <anonymous>

WALPayload::WALPayload(const Parameters &param)
{
    m_data.resize(HEADER_SIZE);
    auto bytes = _b(m_data);;

    put_uint32(bytes, param.previous_lsn.value);
    bytes.advance(sizeof(uint32_t));

    put_uint32(bytes, param.page_id.value);
    bytes.advance(sizeof(uint32_t));

    put_uint16(bytes, static_cast<uint16_t>(param.changes.size()));

    for (const auto &change: param.changes) {
        const auto size = change.before.size();;
        CUB_EXPECT_EQ(size, change.after.size());
        auto chunk = std::string(UPDATE_HEADER_SIZE + 2*size, '\x00');;
        bytes = _b(chunk);

        put_uint16(bytes, static_cast<uint16_t>(change.offset));
        bytes.advance(sizeof(uint16_t));

        put_uint16(bytes, static_cast<uint16_t>(size));
        bytes.advance(sizeof(uint16_t));

        mem_copy(bytes, change.before, size);
        bytes.advance(size);

        mem_copy(bytes, change.after, size);
        m_data += chunk;
    }
}

auto WALPayload::is_commit() const -> bool
{
    const auto update = decode();;
    return update.page_id.is_null() && update.changes.empty();
}

auto WALPayload::decode() const -> PageUpdate
{
    auto update = PageUpdate{};;
    auto bytes = _b(m_data);;

    update.previous_lsn.value = get_uint32(bytes);
    bytes.advance(sizeof(uint32_t));

    update.page_id.value = get_uint32(bytes);
    bytes.advance(sizeof(uint32_t));

    update.changes.resize(get_uint16(bytes));
    bytes.advance(sizeof(uint16_t));

    for (auto &region: update.changes) {
        region.offset = get_uint16(bytes);
        bytes.advance(sizeof(uint16_t));

        const auto region_size = get_uint16(bytes);;
        bytes.advance(sizeof(uint16_t));

        region.before = bytes.range(0, region_size);
        bytes.advance(region_size);

        region.after = bytes.range(0, region_size);
        bytes.advance(region_size);
    }
    return update;
}

auto WALRecord::commit(LSN commit_lsn) -> WALRecord
{
    return WALRecord{{
        {},
        PID::null(),
        LSN::null(),
        commit_lsn,
    }};
}

WALRecord::WALRecord(const Parameters &param)
    : m_payload{param}
    , m_lsn{param.lsn}
    , m_crc{crc_32(m_payload.data())}
    , m_type{Type::FULL} {}

auto WALRecord::read(BytesView in) -> void
{
    // lsn (4B)
    m_lsn.value = get_uint32(in.data());
    in.advance(sizeof(uint32_t));

    // No more values in the buffer (empty space in the buffer must be zeroed and LSNs
    // start with 1).
    if (m_lsn.is_null())
        return;

    // crc (4B)
    m_crc = get_uint32(in.data());
    in.advance(sizeof(uint32_t));

    // type (1B)
    m_type = Type(in[0]);
    in.advance(sizeof(Type));

    if (!is_record_type_valid(m_type))
        throw CorruptionError{"WAL record type is invalid"};

    // x (2B)
    const auto payload_size = get_uint16(in.data());;
    m_payload.m_data.resize(payload_size);
    in.advance(sizeof(uint16_t));

    // payload (xB)
    mem_copy(_b(m_payload.m_data), in, payload_size);
}

auto WALRecord::write(Bytes out) const noexcept -> void
{
    CUB_EXPECT_GE(out.size(), size());

    // lsn (4B)
    put_uint32(out, static_cast<uint32_t>(m_lsn.value));
    out.advance(sizeof(uint32_t));

    // crc (4B)
    put_uint32(out, static_cast<uint32_t>(m_crc));
    out.advance(sizeof(uint32_t));

    // type (1B)
    out[0] = static_cast<Byte>(m_type);
    out.advance(sizeof(Type));

    // x (2B)
    const auto payload_size = m_payload.m_data.size();;
    put_uint16(out, static_cast<uint16_t>(payload_size));
    out.advance(sizeof(uint16_t));

    // payload (xB)
    mem_copy(out, _b(m_payload.m_data), payload_size);
}

auto WALRecord::is_consistent() const -> bool
{
    CUB_EXPECT_EQ(m_type, Type::FULL);
    return m_crc == crc_32(m_payload.data());
}

/*
 * Valid Splits:
 *     .-------------------------------.
 *     |  Before  =  Left    +  Right  |
 *     :----------.----------.---------:
 *     |  FULL    |  FIRST   |  LAST   |
 *     |  LAST    |  MIDDLE  |  LAST   |
 *     '----------'----------'---------'
 */
auto WALRecord::split(Index offset_in_payload) -> WALRecord
{
    CUB_EXPECT_LT(offset_in_payload, m_payload.m_data.size());
    WALRecord rhs;

    rhs.m_payload.m_data = m_payload.m_data.substr(offset_in_payload);
    m_payload.m_data.resize(offset_in_payload);

    rhs.m_lsn = m_lsn;
    rhs.m_crc = m_crc;
    rhs.m_type = Type::LAST;

    CUB_EXPECT_NE(m_type, Type::EMPTY);
    CUB_EXPECT_NE(m_type, Type::FIRST);
    if (m_type == Type::FULL) {
        m_type = Type::FIRST;
    } else {
        CUB_EXPECT_EQ(m_type, Type::LAST);
        m_type = Type::MIDDLE;
    }
    return rhs;
}

/*
 * Valid Merges:
 *     .-------------------------------.
 *     |  Left    +  Right   =  After  |
 *     :----------.----------.---------:
 *     |  EMPTY   |  FIRST   |  FIRST  |
 *     |  EMPTY   |  FULL    |  FULL   |
 *     |  FIRST   |  MIDDLE  |  FIRST  |
 *     |  FIRST   |  LAST    |  FULL   |
 *     '----------'----------'---------'
 */
auto WALRecord::merge(WALRecord rhs) -> void
{
    CUB_EXPECT_NE(rhs.type(), Type::EMPTY);
    m_payload.append(rhs.m_payload);

    if (m_type == Type::EMPTY) {
        CUB_EXPECT_NE(rhs.m_type, Type::MIDDLE);
        CUB_EXPECT_NE(rhs.m_type, Type::LAST);

        m_type = rhs.m_type;
        m_lsn = rhs.m_lsn;
        m_crc = rhs.m_crc;
    } else {
        CUB_EXPECT_EQ(m_type, Type::FIRST);

        if (m_lsn != rhs.m_lsn)
            throw CorruptionError {"WAL records have mismatched LSNs"};
        if (m_crc != rhs.m_crc)
            throw CorruptionError {"WAL records have mismatched CRCs"};

        // We have just completed a record.
        if (rhs.m_type == Type::LAST)
            m_type = Type::FULL;
    }
}

} // cub
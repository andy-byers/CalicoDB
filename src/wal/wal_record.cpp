#include "wal_record.h"
#include "page/update.h"
#include "utils/crc.h"
#include "utils/encoding.h"

namespace cco {

using namespace page;
using namespace utils;

namespace {

    inline auto is_record_type_valid(WALRecord::Type type) -> bool
    {
        return type == WALRecord::Type::FIRST ||
               type == WALRecord::Type::MIDDLE ||
               type == WALRecord::Type::LAST ||
               type == WALRecord::Type::FULL;
    }

} // <anonymous>

WALPayload::WALPayload(const PageUpdate &param)
{
    m_data.resize(HEADER_SIZE);
    auto bytes = stob(m_data);

    put_u32(bytes, param.previous_lsn.value);
    bytes.advance(sizeof(uint32_t));

    put_u32(bytes, param.page_id.value);
    bytes.advance(sizeof(uint32_t));

    put_u16(bytes, static_cast<uint16_t>(param.changes.size()));

    for (const auto &change: param.changes) {
        const auto size = change.before.size();
        CCO_EXPECT_EQ(size, change.after.size());
        auto chunk = std::string(UPDATE_HEADER_SIZE + 2*size, '\x00');
        bytes = stob(chunk);

        put_u16(bytes, static_cast<uint16_t>(change.offset));
        bytes.advance(sizeof(uint16_t));

        put_u16(bytes, static_cast<uint16_t>(size));
        bytes.advance(sizeof(uint16_t));

        mem_copy(bytes, change.before, size);
        bytes.advance(size);

        mem_copy(bytes, change.after, size);
        m_data += chunk;
    }
}

auto WALPayload::is_commit() const -> bool
{
    const auto update = decode();
    return update.page_id.is_null() && update.changes.empty();
}

auto WALPayload::decode() const -> PageUpdate
{
    auto update = PageUpdate{};
    auto bytes = stob(m_data);

    update.previous_lsn.value = get_u32(bytes);
    bytes.advance(sizeof(uint32_t));

    update.page_id.value = get_u32(bytes);
    bytes.advance(sizeof(uint32_t));

    update.changes.resize(get_u16(bytes));
    bytes.advance(sizeof(uint16_t));

    for (auto &region: update.changes) {
        region.offset = get_u16(bytes);
        bytes.advance(sizeof(uint16_t));

        const auto region_size = get_u16(bytes);
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

WALRecord::WALRecord(const PageUpdate &update):
      m_payload {update},
      m_lsn {update.lsn},
      m_crc {crc_32(m_payload.data())},
      m_type {Type::FULL} {}

auto WALRecord::read(BytesView in) -> Result<bool>
{
    static constexpr auto ERROR_PRIMARY = "cannot read WAL record";

    // lsn (4B)
    m_lsn.value = get_u32(in.data());
    in.advance(sizeof(uint32_t));

    // No more values in the buffer (empty space in the buffer must be zeroed and LSNs
    // start with 1).
    if (m_lsn.is_null())
        return false;

    // crc (4B)
    m_crc = get_u32(in.data());
    in.advance(sizeof(uint32_t));

    // type (1B)
    m_type = static_cast<Type>(in[0]);
    in.advance(sizeof(Type));

    if (!is_record_type_valid(m_type))
        return Err {Error::corruption(ERROR_PRIMARY)};

    // x (2B)
    const auto payload_size = get_u16(in.data());
    in.advance(sizeof(uint16_t));

    // Every record stores at least 1 payload byte.
    if (!payload_size || payload_size > in.size())
        return Err {Error::corruption(ERROR_PRIMARY)};

    m_payload.m_data.resize(payload_size);

    // payload (xB)
    mem_copy(stob(m_payload.m_data), in, payload_size);
    return true;
}

auto WALRecord::write(Bytes out) const noexcept -> void
{
    CCO_EXPECT_GE(out.size(), size());

    // lsn (4B)
    put_u32(out, static_cast<uint32_t>(m_lsn.value));
    out.advance(sizeof(uint32_t));

    // crc (4B)
    put_u32(out, static_cast<uint32_t>(m_crc));
    out.advance(sizeof(uint32_t));

    // type (1B)
    CCO_EXPECT_TRUE(is_record_type_valid(m_type));
    out[0] = static_cast<Byte>(m_type);
    out.advance(sizeof(Type));

    // x (2B)
    const auto payload_size = m_payload.m_data.size();
    CCO_EXPECT_NE(payload_size, 0);
    put_u16(out, static_cast<uint16_t>(payload_size));
    out.advance(sizeof(uint16_t));

    // payload (xB)
    mem_copy(out, stob(m_payload.m_data), payload_size);
}

auto WALRecord::is_consistent() const -> bool
{
    CCO_EXPECT_EQ(m_type, Type::FULL);
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
    CCO_EXPECT_LT(offset_in_payload, m_payload.m_data.size());
    WALRecord rhs;

    rhs.m_payload.m_data = m_payload.m_data.substr(offset_in_payload);
    m_payload.m_data.resize(offset_in_payload);

    rhs.m_lsn = m_lsn;
    rhs.m_crc = m_crc;
    rhs.m_type = Type::LAST;

    CCO_EXPECT_NE(m_type, Type::EMPTY);
    CCO_EXPECT_NE(m_type, Type::FIRST);
    if (m_type == Type::FULL) {
        m_type = Type::FIRST;
    } else {
        CCO_EXPECT_EQ(m_type, Type::LAST);
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
auto WALRecord::merge(const WALRecord &rhs) -> Result<void>
{
    static constexpr auto ERROR_PRIMARY = "cannot merge WAL records";

    CCO_EXPECT_TRUE(is_record_type_valid(rhs.m_type));
    m_payload.append(rhs.m_payload);

    if (m_type == Type::EMPTY) {
        if (rhs.m_type == Type::MIDDLE || rhs.m_type == Type::LAST)
            return Err {Error::corruption(ERROR_PRIMARY)};

        m_type = rhs.m_type;
        m_lsn = rhs.m_lsn;
        m_crc = rhs.m_crc;

    } else {
        CCO_EXPECT_EQ(m_type, Type::FIRST);

        if (m_lsn != rhs.m_lsn || m_crc != rhs.m_crc)
            return Err {Error::corruption(ERROR_PRIMARY)};

        // We have just completed a record.
        if (rhs.m_type == Type::LAST)
            m_type = Type::FULL;
    }
    return {};
}

} // cco
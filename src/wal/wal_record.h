/**
*
* References
*   (1) https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-IFile-Format
*/

#ifndef CCO_WAL_WAL_RECORD_H
#define CCO_WAL_WAL_RECORD_H

#include "calico/status.h"
#include "page/page.h"
#include "page/update.h"
#include "utils/identifier.h"
#include "utils/result.h"
#include "utils/utils.h"

namespace cco {

// WAL payload format:
//
//   .-------------- Payload Header -------------.   .------------------ Update Header ----------------.
//   .-------------------.--------------.--------.   .-------------.--------.-------------.------------.
//   | previous_lsn (4B) | page_id (4B) | N (2B) |   | offset (2B) | y (2B) | before (yB) | after (yB) | (x N)
//   '-------------------'--------------'--------'   '-------------'--------'-------------'------------'
//   0                   4              8        10  s             s+2      s+4           s+y+4        s+y*2+4
//

/**
 * The variable-length payload field of a WAL record.
 */
class WALPayload {
public:
    friend class WALRecord;
    static constexpr Size HEADER_SIZE = 10;
    static constexpr Size UPDATE_HEADER_SIZE = 4;

    WALPayload() = default;
    ~WALPayload() = default;
    explicit WALPayload(const page::PageUpdate&);
    [[nodiscard]] auto is_commit() const -> bool;
    [[nodiscard]] auto decode() const -> page::PageUpdate;

    [[nodiscard]] auto data() const -> BytesView
    {
        return stob(m_data);
    }

    auto append(const WALPayload &rhs) -> void
    {
        m_data += rhs.m_data;
    }

private:
    std::string m_data; ///< Payload contents
};

/**
 * A container that makes data compatible with the WAL storage format.
 */
class WALRecord {
public:
    static constexpr Size HEADER_SIZE = 11;

    enum class Type: Byte {
        EMPTY  = 0x00,
        FIRST  = 0x12,
        MIDDLE = 0x23,
        LAST   = 0x34,
        FULL   = 0x45,
    };

    static auto commit(LSN) -> WALRecord;
    WALRecord() = default;
    explicit WALRecord(const page::PageUpdate&);
    ~WALRecord() = default;

    [[nodiscard]] auto lsn() const -> LSN
    {
        return m_lsn;
    }

    [[nodiscard]] auto crc() const -> Index
    {
        return m_crc;
    }

    [[nodiscard]] auto type() const -> Type
    {
        return m_type;
    }

    [[nodiscard]] auto size() const -> Size
    {
        return m_payload.m_data.size() + HEADER_SIZE;
    }

    [[nodiscard]] auto payload() const -> const WALPayload&
    {
        return m_payload;
    }

    [[nodiscard]] auto is_commit() const -> bool
    {
        CCO_EXPECT_EQ(m_type, Type::FULL);
        return m_payload.is_commit();
    }

    [[nodiscard]] auto decode() const -> page::PageUpdate
    {
        return m_payload.decode();
    }

    [[nodiscard]] auto is_consistent() const -> bool;
    [[nodiscard]] auto read(BytesView) -> Result<bool>;
    [[nodiscard]] auto merge(const WALRecord&) -> Result<void>;
    auto write(Bytes) const noexcept -> void;
    auto split(Index) -> WALRecord;

private:
    WALPayload m_payload;
    LSN m_lsn;
    Index m_crc {};
    Type m_type {Type::EMPTY};
};

} // cco

#endif // CCO_WAL_WAL_RECORD_H
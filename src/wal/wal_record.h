/**
*
* References
*   (1) https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-IFile-Format
*/

#ifndef CUB_WAL_WAL_RECORD_H
#define CUB_WAL_WAL_RECORD_H

#include "common.h"
#include "page/page.h"
#include "utils/types.h"
#include "utils/utils.h"

namespace cub {

// WAL record format, modified from (1):
//
//   .-------------- Record Header -------------.
//   .----------.----------.-----------.--------.--------------.
//   | lsn (4B) | crc (4B) | type (1B) | x (2B) | payload (xB) |
//   '----------'----------'-----------'--------'--------------'
//   0          4          8           9        11             11+x
//
// WAL payload format:
//
//   .-------------- Payload Header -------------.   .------------------ PageUpdate Header ----------------.
//   .-------------------.--------------.--------.   .-------------.--------.-------------.------------.
//   | previous_lsn (4B) | page_id (4B) | N (2B) |   | offset (2B) | y (2B) | before (yB) | after (yB) | (x N)
//   '-------------------'--------------'--------'   '-------------'--------'-------------'------------'
//   0                   4              8        10  s             s+2      s+4           s+y+4        s+y*2+4
//

class WALPayload {
public:
    friend class WALRecord;
    static constexpr Size HEADER_SIZE = 10;
    static constexpr Size UPDATE_HEADER_SIZE = 4;

    struct Parameters {
        std::vector<ChangedRegion> changes;
        PID page_id;
        LSN previous_lsn;
        LSN lsn;
    };

    WALPayload() = default;
    ~WALPayload() = default;
    explicit WALPayload(const Parameters&);
    [[nodiscard]] auto is_commit() const -> bool;
    [[nodiscard]] auto decode() const -> PageUpdate;
    [[nodiscard]] auto data() const -> BytesView {return _b(m_data);}
    auto append(const WALPayload &rhs) -> void {m_data += rhs.m_data;}

private:
    std::string m_data;
};

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

    using Parameters = WALPayload::Parameters;

    WALRecord() = default;
    explicit WALRecord(const Parameters&);
    ~WALRecord() = default;
    [[nodiscard]] auto is_consistent() const -> bool;
    [[nodiscard]] auto is_commit() const -> bool;
    [[nodiscard]] auto lsn() const -> LSN;
    [[nodiscard]] auto crc() const -> Index;
    [[nodiscard]] auto type() const -> Type;
    [[nodiscard]] auto size() const -> Size;
    [[nodiscard]] auto payload() const -> const WALPayload&;
    auto read(BytesView) -> void;
    auto write(Bytes) const noexcept -> void;
    auto split(Index) -> WALRecord;
    auto merge(WALRecord) -> void;

private:
    WALPayload m_payload;
    LSN m_lsn;
    Index m_crc {};
    Type m_type {Type::EMPTY};
};

} // db

#endif // CUB_WAL_WAL_RECORD_H

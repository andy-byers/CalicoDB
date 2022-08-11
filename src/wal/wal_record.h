///**
//*
//* References
//*   (1) https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-IFile-Format
//*/
//
//#ifndef CCO_WAL_WAL_RECORD_H
//#define CCO_WAL_WAL_RECORD_H
//
//#include "calico/status.h"
//#include "page/page.h"
//#include "utils/identifier.h"
//#include "utils/result.h"
//#include "utils/utils.h"
//
//namespace cco {
//
///**
// * The variable-length payload field of a WAL record.
// */
//class WALPayload {
//public:
//    friend class WALRecord;
//    static constexpr Size HEADER_SIZE {18};
//    static constexpr Size UPDATE_HEADER_SIZE {4};
//
//    WALPayload() = default;
//
//    explicit WALPayload(Bytes scratch)
//        : m_data {scratch}
//    {}
//
//    ~WALPayload() = default;
//    WALPayload(const PageUpdate &, Bytes);
//    [[nodiscard]] auto is_commit() const -> bool;
//    [[nodiscard]] auto decode() const -> PageUpdate;
//
//    [[nodiscard]] auto data() const -> BytesView
//    {
//        return m_data;
//    }
//
//    auto append(const WALPayload &rhs) -> void
//    {
//        mem_copy(m_data.range(m_cursor), rhs.m_data);
//        m_cursor += rhs.m_data.size();
//    }
//
//private:
//    // TODO: Beware that m_data may be exactly the size of an entire payload, if the payload was made with the PageUpdate constructor, otherwise, it is the full scratch memory
//    //       and one should use the WALRecord payload size instead.
//    Bytes m_data; ///< Payload contents
//    Index m_cursor {};
//};
//
///**
// * A container that makes data compatible with the WAL storage format.
// */
//class WALRecord {
//public:
//    static constexpr Size HEADER_SIZE {15};
//    static constexpr auto MINIMUM_SIZE = HEADER_SIZE + 1;
//
//    enum class Type : Byte {
//        EMPTY = 0x00,
//        FIRST = 0x12,
//        MIDDLE = 0x23,
//        LAST = 0x34,
//        FULL = 0x45,
//    };
//
//    WALRecord() = default;
//
//    explicit WALRecord(Bytes scratch)
//        : m_backing {scratch}
//    {}
//
//    static auto commit(SequenceNumber, Bytes) -> WALRecord;
//    WALRecord(const PageUpdate &, Bytes);
//    ~WALRecord() = default;
//
//    [[nodiscard]] auto lsn() const -> SequenceNumber
//    {
//        return m_lsn;
//    }
//
//    [[nodiscard]] auto crc() const -> Index
//    {
//        return m_crc;
//    }
//
//    [[nodiscard]] auto type() const -> Type
//    {
//        return m_type;
//    }
//
//    [[nodiscard]] auto size() const -> Size
//    {
//        return m_payload.m_data.size() + HEADER_SIZE;
//    }
//
//    [[nodiscard]] auto payload() const -> const WALPayload &
//    {
//        return m_payload;
//    }
//
//    [[nodiscard]] auto is_commit() const -> bool
//    {
//        CCO_EXPECT_EQ(m_type, Type::FULL);
//        return m_payload.is_commit();
//    }
//
//    [[nodiscard]] auto decode() const -> PageUpdate
//    {
//        auto decoded = m_payload.decode();
//        decoded.page_lsn = m_lsn;
//        return decoded;
//    }
//
//    [[nodiscard]] auto is_consistent() const -> bool;
//    [[nodiscard]] auto read(BytesView) -> Result<bool>;
//    [[nodiscard]] auto merge(const WALRecord &) -> Result<void>;
//    auto write(Bytes) const noexcept -> void;
//    auto split(Index) -> WALRecord;
//
//    auto TEST_corrupt_crc() -> void
//    {
//        m_crc++;
//    }
//
//private:
//    WALPayload m_payload;
//    SequenceNumber m_lsn;
//    Bytes m_backing;
//    Index m_crc {};
//    Type m_type {Type::EMPTY};
//};
//
//} // namespace cco
//
//#endif // CCO_WAL_WAL_RECORD_H
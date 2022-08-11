//#ifndef CCO_WAL_INTERFACE_H
//#define CCO_WAL_INTERFACE_H
//
//#include "utils/identifier.h"
//#include "utils/result.h"
//#include "wal/wal_record.h"
//#include <optional>
//
//namespace cco {
//
//class BufferPool;
//class Storage;
//class IFile;
//class Page;
//
//constexpr auto WAL_PREFIX = "wal-";
//using SegmentId = Identifier<std::uint64_t>;
//
//inline auto id_to_name(SegmentId id) -> std::string
//{
//    CCO_EXPECT_FALSE(id.is_null());
//    static constexpr Size MAX_WIDTH {8};
//    const auto digits = std::to_string(id.value);
//    return WAL_PREFIX + std::string(MAX_WIDTH - digits.size(), '0') + digits;
//}
//
//inline auto name_to_id(BytesView name) -> SegmentId
//{
//    auto digits = name.range(4);
//
//    // Don't call std::stoul() if it's going to throw an exception.
//    const auto is_valid = std::all_of(digits.data(), digits.data() + digits.size(), [](auto c) {return std::isdigit(c);});
//
//    if (!is_valid)
//        return SegmentId::null();
//
//    return SegmentId {std::stoull(btos(digits))};
//}
//
//struct WALParameters {
//    BufferPool *pool {};
//    Storage &directory;
//    spdlog::sink_ptr log_sink;
//    Size page_size {};
//    SequenceNumber flushed_lsn {};
//};
//
//struct WALRecordPosition {
//    Index block_id {};
//    Index offset {};
//};
//
//struct WALSegment {
//    std::vector<WALRecordPosition> positions;
//    SegmentId id;
//    SequenceNumber start;
//    bool has_commit {};
//};
//
//class IWALWriter {
//public:
//    using Position = WALRecordPosition;
//    virtual ~IWALWriter() = default;
//    virtual auto is_open() -> bool = 0;
//    virtual auto needs_segmentation() -> bool = 0;
//
//    /**
//     * Open the writer on a WAL segment file.
//     *
//     * The provided segment file must be empty. WAL writers keep track of their position in the segment file. If there
//     * is already data in the file, the position will be reported incorrectly.
//     *
//     * @return A result object that provides error information in the failure case.
//     */
//    virtual auto open(std::unique_ptr<IFile>) -> Result<void> = 0;
//    virtual auto close() -> Result<void> = 0;
//    virtual auto flushed_lsn() const -> SequenceNumber = 0;
//    virtual auto last_lsn() const -> SequenceNumber = 0;
//
//    /**
//     * Check if there are records waiting to be flushed.
//     *
//     * The WAL writer stores records in an internal buffer. Once this buffer reaches capacity, it is appended to the appropriate
//     * WAL segment file.
//     *
//     * @return True if there are records to be flushed, false otherwise.
//     */
//    virtual auto has_pending() const -> bool = 0;
//
//    /**
//     * Check if there are records already written to the segment file.
//     *
//     * @return True if there are records to be flushed, false otherwise.
//     */
//    virtual auto has_committed() const -> bool = 0;
//    virtual auto append(WALRecord) -> Result<Position> = 0;
//    virtual auto truncate() -> Result<void> = 0;
//    virtual auto flush() -> Result<void> = 0;
//    virtual auto set_flushed_lsn(SequenceNumber) -> void = 0;
//};
//
//class IWALReader {
//public:
//    using Position = WALRecordPosition;
//    virtual ~IWALReader() = default;
//    virtual auto is_open() -> bool = 0;
//    virtual auto is_empty() -> Result<bool> = 0;
//    virtual auto open(std::unique_ptr<IFile>) -> Result<void> = 0;
//    virtual auto read(Position &) -> Result<WALRecord> = 0;
//    virtual auto close() -> Result<void> = 0;
//    virtual auto reset() -> void = 0;
//};
//
//} // namespace cco
//
//#endif // CCO_WAL_INTERFACE_H

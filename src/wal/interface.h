#ifndef CCO_WAL_INTERFACE_H
#define CCO_WAL_INTERFACE_H

#include "page/file_header.h"
#include "utils/result.h"
#include "wal_record.h"
#include <optional>

namespace cco {

class IBufferPool;
class IDirectory;
class IFile;
class Page;

constexpr auto WAL_PREFIX = "wal-";
using SegmentId = Identifier<std::uint32_t>;

inline auto id_to_name(SegmentId id) -> std::string
{
    CCO_EXPECT_FALSE(id.is_null());
    static constexpr Size MAX_WIDTH {8};
    const auto digits = std::to_string(id.value);
    return WAL_PREFIX + std::string(MAX_WIDTH - digits.size(), '0') + digits;
}

inline auto name_to_id(BytesView name) -> SegmentId
{
    auto digits = name.range(4);

    // Don't call std::stoul() if it's going to throw an exception.
    const auto is_valid = std::all_of(digits.data(), digits.data() + digits.size(), [](auto c) {return std::isdigit(c);});

    if (!is_valid)
        return SegmentId::null();

    return SegmentId {static_cast<std::uint32_t>(std::stoul(btos(digits)))};
}

struct WALParameters {
    IBufferPool *pool {};
    IDirectory &directory;
    spdlog::sink_ptr log_sink;
    Size page_size {};
    SequenceNumber flushed_lsn {};
};

struct WALRecordPosition {
    Index block_id {};
    Index offset {};
};

struct WALSegment {
    std::vector<WALRecordPosition> positions;
    SegmentId id;
    SequenceNumber start;
    bool has_commit {};
};

class IWALManager {
public:
    virtual ~IWALManager() = default;
    [[nodiscard]] virtual auto has_pending() const -> bool = 0;
    [[nodiscard]] virtual auto flushed_lsn() const -> SequenceNumber = 0;
    [[nodiscard]] virtual auto truncate(SegmentId) -> Result<void> = 0;
    [[nodiscard]] virtual auto flush() -> Result<void> = 0;
    [[nodiscard]] virtual auto append(Page &) -> Result<void> = 0;
    [[nodiscard]] virtual auto recover() -> Result<void> = 0;
    [[nodiscard]] virtual auto commit() -> Result<void> = 0;
    [[nodiscard]] virtual auto abort() -> Result<void> = 0;
    [[nodiscard]] virtual auto close() -> Result<void> = 0;
    [[nodiscard]] virtual auto cleanup() -> Result<void> = 0;
    virtual auto track(Page &) -> void = 0;
    virtual auto discard(Page &) -> void = 0;
    virtual auto save_header(FileHeaderWriter &) -> void = 0;
    virtual auto load_header(const FileHeaderReader &) -> void = 0;
};

class IWALWriter {
public:
    using Position = WALRecordPosition;
    virtual ~IWALWriter() = default;
    [[nodiscard]] virtual auto is_open() -> bool = 0;
    [[nodiscard]] virtual auto needs_segmentation() -> bool = 0;

    /**
     * Open the writer on a WAL segment file.
     *
     * The provided segment file must be empty. WAL writers keep track of their position in the segment file. If there
     * is already data in the file, the position will be reported incorrectly.
     *
     * @return A result object that provides error information in the failure case.
     */
    [[nodiscard]] virtual auto open(std::unique_ptr<IFile>) -> Result<void> = 0;
    [[nodiscard]] virtual auto close() -> Result<void> = 0;
    [[nodiscard]] virtual auto flushed_lsn() const -> SequenceNumber = 0;
    [[nodiscard]] virtual auto last_lsn() const -> SequenceNumber = 0;

    /**
     * Check if there are records waiting to be flushed.
     *
     * The WAL writer stores records in an internal buffer. Once this buffer reaches capacity, it is appended to the appropriate
     * WAL segment file.
     *
     * @return True if there are records to be flushed, false otherwise.
     */
    [[nodiscard]] virtual auto has_pending() const -> bool = 0;

    /**
     * Check if there are records already written to the segment file.
     *
     * @return True if there are records to be flushed, false otherwise.
     */
    [[nodiscard]] virtual auto has_committed() const -> bool = 0;
    [[nodiscard]] virtual auto append(WALRecord) -> Result<Position> = 0;
    [[nodiscard]] virtual auto truncate() -> Result<void> = 0;
    [[nodiscard]] virtual auto flush() -> Result<void> = 0;
    virtual auto set_flushed_lsn(SequenceNumber) -> void = 0;
};

class IWALReader {
public:
    using Position = WALRecordPosition;
    virtual ~IWALReader() = default;
    [[nodiscard]] virtual auto is_open() -> bool = 0;
    [[nodiscard]] virtual auto is_empty() -> Result<bool> = 0;
    [[nodiscard]] virtual auto open(std::unique_ptr<IFile>) -> Result<void> = 0;
    [[nodiscard]] virtual auto read(Position &) -> Result<WALRecord> = 0;
    [[nodiscard]] virtual auto close() -> Result<void> = 0;
    virtual auto reset() -> void = 0;
};

} // namespace cco

#endif // CCO_WAL_INTERFACE_H

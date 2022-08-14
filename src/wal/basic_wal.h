#ifndef CALICO_WAL_BASIC_WAL_H
#define CALICO_WAL_BASIC_WAL_H

#include "calico/wal.h"
#include "helpers.h"
#include "utils/result.h"
#include "utils/types.h"
#include "wal/record.h"
#include <optional>

namespace calico {

class Pager;
class Storage;
class AppendWriter;
class RandomReader;
class Page;

//struct WALParameters {
//    Storage &store;
//    spdlog::sink_ptr sink;
//    Size page_size {};
//    SeqNum flushed_lsn {};
//};

//struct WALRecordPosition {
//    Size block_id {};
//    Size offset {};
//};
//
//struct WALSegment {
//    std::vector<WALRecordPosition> positions;
//    SegmentId id;
//    SeqNum start;
//    bool has_commit {};
//};
//
class BasicWriteAheadLog: public WriteAheadLog {
public:
    struct Parameters {
        Storage *store {};
        spdlog::sink_ptr sink;
        Size block_size {};
    };

    [[nodiscard]]
    auto is_enabled() const -> bool override
    {
        return true;
    }

    ~BasicWriteAheadLog() override = default;
    [[nodiscard]] static auto open(const Parameters&, WriteAheadLog**) -> Status;
    [[nodiscard]] auto flushed_lsn() const -> std::uint64_t override;
    [[nodiscard]] auto current_lsn() const -> std::uint64_t override;
    [[nodiscard]] auto log_image(std::uint64_t page_id, BytesView image) -> Status override;
    [[nodiscard]] auto log_deltas(std::uint64_t page_id, BytesView image, const std::vector<PageDelta> &deltas) -> Status override;
    [[nodiscard]] auto log_commit() -> Status override;
    [[nodiscard]] auto stop() -> Status override;
    [[nodiscard]] auto start() -> Status override;
    [[nodiscard]] auto redo_all(const RedoCallback &callback) -> Status override;
    [[nodiscard]] auto undo_last(const UndoCallback &callback) -> Status override;
    auto save_state(FileHeader &) -> void override;
    auto load_state(const FileHeader &) -> void override;

private:
    explicit BasicWriteAheadLog(const Parameters &param)
        : m_logger {create_logger(param.sink, "wal")},
          m_store {param.store}
    {}

    std::shared_ptr<spdlog::logger> m_logger;
    Storage *m_store {};

    BasicWalReader m_reader;
    BasicWalWriter m_writer;
};


class DisabledWriteAheadLog: public WriteAheadLog {
public:
    ~DisabledWriteAheadLog() override = default;

    [[nodiscard]]
    auto is_enabled() const -> bool override
    {
        return false;
    }

    [[nodiscard]]
    auto flushed_lsn() const -> std::uint64_t override
    {
        return 0;
    }

    [[nodiscard]]
    auto current_lsn() const -> std::uint64_t override
    {
        return 0;
    }

    [[nodiscard]]
    auto log_image(std::uint64_t, BytesView) -> Status override
    {
        return Status::ok();
    }

    [[nodiscard]]
    auto log_deltas(std::uint64_t, BytesView, const std::vector<PageDelta> &) -> Status override
    {
        return Status::ok();
    }

    [[nodiscard]]
    auto log_commit() -> Status override
    {
        return Status::ok();
    }

    [[nodiscard]]
    auto stop() -> Status override
    {
        return Status::ok();
    }

    [[nodiscard]]
    auto start() -> Status override
    {
        return Status::ok();
    }

    [[nodiscard]]
    auto redo_all(const RedoCallback &) -> Status override
    {
        return Status::ok();
    }

    [[nodiscard]]
    auto undo_last(const UndoCallback &) -> Status override
    {
        return Status::ok();
    }

    auto save_state(FileHeader &) -> void override {}
    auto load_state(const FileHeader &) -> void override {}
};

////class IWALWriter {
////public:
////    using Position = WALRecordPosition;
////    virtual ~IWALWriter() = default;
////    virtual auto is_open() -> bool = 0;
////    virtual auto needs_segmentation() -> bool = 0;
////
////    /**
////     * Open the writer on a WAL segment file.
////     *
////     * The provided segment file must be empty. WAL writers keep track of their position in the segment file. If there
////     * is already data in the file, the position will be reported incorrectly.
////     *
////     * @return A result object that provides error information in the failure case.
////     */
////    virtual auto open(std::unique_ptr<IFile>) -> Result<void> = 0;
////    virtual auto close() -> Result<void> = 0;
////    virtual auto flushed_lsn() const -> SeqNum = 0;
////    virtual auto last_lsn() const -> SeqNum = 0;
////
////    /**
////     * Check if there are records waiting to be flushed.
////     *
////     * The WAL writer stores records in an internal buffer. Once this buffer reaches capacity, it is appended to the appropriate
////     * WAL segment file.
////     *
////     * @return True if there are records to be flushed, false otherwise.
////     */
////    virtual auto has_pending() const -> bool = 0;
////
////    /**
////     * Check if there are records already written to the segment file.
////     *
////     * @return True if there are records to be flushed, false otherwise.
////     */
////    virtual auto has_committed() const -> bool = 0;
////    virtual auto append(WALRecord) -> Result<Position> = 0;
////    virtual auto truncate() -> Result<void> = 0;
////    virtual auto advance_block() -> Result<void> = 0;
////    virtual auto set_flushed_lsn(SeqNum) -> void = 0;
////};
////
////class IWALReader {
////public:
////    using Position = WALRecordPosition;
////    virtual ~IWALReader() = default;
////    virtual auto is_open() -> bool = 0;
////    virtual auto is_empty() -> Result<bool> = 0;
////    virtual auto open(std::unique_ptr<IFile>) -> Result<void> = 0;
////    virtual auto read(Position &) -> Result<WALRecord> = 0;
////    virtual auto close() -> Result<void> = 0;
////    virtual auto reset() -> void = 0;
////};
//
} // namespace calico

#endif // CALICO_WAL_BASIC_WAL_H

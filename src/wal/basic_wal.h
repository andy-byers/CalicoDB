#ifndef CALICO_WAL_BASIC_WAL_H
#define CALICO_WAL_BASIC_WAL_H

#include "calico/wal.h"
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

constexpr auto WAL_PREFIX = "wal";

//struct WALParameters {
//    Storage &store;
//    Pager &pager;
//    spdlog::sink_ptr sink;
//    Size page_size {};
//    SeqNum flushed_lsn {};
//};
//
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
//class BasicWriteAheadLog: public WriteAheadLog {
//public:
//    ~BasicWriteAheadLog() override = default;
//    static auto open(Storage&, WriteAheadLog**) -> Status;
//
//    [[nodiscard]]
//    auto is_enabled() const -> bool override
//    {
//        return true;
//    }
//    auto flushed_lsn() const -> std::uint64_t override;
//    auto current_lsn() const -> std::uint64_t override;
//    auto log_image(std::uint64_t page_id, BytesView image) -> Status override;
//    auto log_deltas(std::uint64_t page_id, BytesView image, const std::vector<PageDelta> &deltas) -> Status override;
//    auto log_commit() -> Status override;
//    auto stop() -> Status override;
//    auto start() -> Status override;
//    auto redo_all(const RedoCallback &callback) -> Status override;
//    auto undo_last(const UndoCallback &callback) -> Status override;
//    auto save_state(FileHeader &) -> void override;
//    auto load_state(const FileHeader &) -> void override;
//
//private:
//    explicit BasicWriteAheadLog(const WALParameters &);
//    [[nodiscard]] auto setup(const WALParameters &) -> Result<void>;
//    [[nodiscard]] auto open_reader_segment(SegmentId) -> Result<void>;
//    [[nodiscard]] auto open_writer_segment(SegmentId) -> Result<void>;
//    [[nodiscard]] auto advance_writer(SeqNum, bool) -> Result<void>;
//    [[nodiscard]] auto undo_segment(const WALSegment&) -> Result<void>;
//    [[nodiscard]] auto roll_forward(std::vector<WALRecordPosition> &) -> Result<bool>;
//    [[nodiscard]] auto roll_backward(const std::vector<WALRecordPosition> &) -> Result<void>;
//    [[nodiscard]] auto read_next(WALExplorer &, std::vector<WALRecordPosition> &) -> Result<WALRecord>;
//    [[nodiscard]] auto ensure_initialized() -> Result<void>;
//
//    auto writer_wait_on_event(std::unique_lock<std::mutex> &lock) -> void;
//    auto manager_wait_on_writer(std::unique_lock<std::mutex> &) -> void;
//    auto writer_signal_manager() -> void;
//    auto manager_signal_writer(PageUpdate) -> void;
//
//    std::unique_ptr<IWALReader> m_reader;
//    std::unique_ptr<IWALWriter> m_writer;
//    std::shared_ptr<spdlog::logger> m_logger;
//    BufferPool *m_pool {};
//    Storage *m_home {};
//    bool m_has_pending {};
//
//    using CommitEvent = Event<0, SeqNum>; // TODO: More state in this class (less in WALWriter), and more state passed through these?
//    using AbortEvent = Event<1>;
//    using AppendEvent = Event<2, PageUpdate>;
//    using ExitEvent = Event<3>;
//    using WALEventQueue = EventQueue<CommitEvent, AbortEvent, AppendEvent, ExitEvent>;
//
//    WALEventQueue m_queue;
//    mutable std::mutex m_queue_mutex;
//    mutable std::mutex m_busy_mutex;
//    std::condition_variable m_queue_cond;
//    std::condition_variable m_busy_cond;
//    std::queue<PageUpdate> m_pending_updates;
//    std::string m_record_scratch;
//    std::vector<WALSegment> m_completed_segments;
//    WALSegment m_current_segment;
//    Status m_writer_status {Status::ok()};
//    std::optional<std::thread> m_writer_task;
//    SeqNum m_next_lsn;
//    bool m_is_busy {};
//};
//

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
////    virtual auto flush() -> Result<void> = 0;
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

#ifndef CCO_WAL_WAL_MANAGER_H
#define CCO_WAL_WAL_MANAGER_H

#include "interface.h"
#include "event_queue.h"
#include <condition_variable>
#include <optional>
#include <mutex>
#include <thread>
#include "page/update.h"
#include "utils/queue.h"
#include "utils/tracker.h"

namespace cco {

class IDirectory;
class WALExplorer;

class WALManager : public IWALManager {
public:
    ~WALManager() override;
    [[nodiscard]] static auto open(const WALParameters &) -> Result<std::unique_ptr<IWALManager>>;
    [[nodiscard]] auto close() -> Result<void> override;
    [[nodiscard]] auto has_pending() const -> bool override;
    [[nodiscard]] auto flushed_lsn() const -> SequenceNumber override;
    [[nodiscard]] auto truncate(SegmentId) -> Result<void> override;
    [[nodiscard]] auto flush() -> Result<void> override;
    [[nodiscard]] auto append(Page &) -> Result<void> override;
    [[nodiscard]] auto recover() -> Result<void> override;
    [[nodiscard]] auto abort() -> Result<void> override;
    [[nodiscard]] auto commit() -> Result<void> override;
    [[nodiscard]] auto cleanup() -> Result<void> override;
    [[nodiscard]] auto spawn_writer() -> Result<void> override;
    auto teardown() -> void override;
    auto discard(Page &) -> void override;
    auto track(Page &) -> void override;
    auto save_header(FileHeaderWriter &) -> void override;
    auto load_header(const FileHeaderReader &) -> void override;

private:
    explicit WALManager(const WALParameters &);
    [[nodiscard]] auto setup(const WALParameters &) -> Result<void>;
    [[nodiscard]] auto open_reader_segment(SegmentId) -> Result<void>;
    [[nodiscard]] auto open_writer_segment(SegmentId) -> Result<void>;
    [[nodiscard]] auto advance_writer(SequenceNumber, bool) -> Result<void>;
    [[nodiscard]] auto undo_segment(const WALSegment&) -> Result<void>;
    [[nodiscard]] auto roll_forward(std::vector<WALRecordPosition> &) -> Result<bool>;
    [[nodiscard]] auto roll_backward(const std::vector<WALRecordPosition> &) -> Result<void>;
    [[nodiscard]] auto read_next(WALExplorer &, std::vector<WALRecordPosition> &) -> Result<WALRecord>;
    [[nodiscard]] auto ensure_initialized() -> Result<void>;

    auto writer_wait_on_event(std::unique_lock<std::mutex> &lock) -> void;
    auto manager_wait_on_writer(std::unique_lock<std::mutex> &) -> void;
    auto writer_signal_manager() -> void;
    auto manager_signal_writer(PageUpdate) -> void;

    Tracker m_tracker;
    std::unique_ptr<IWALReader> m_reader;
    std::unique_ptr<IWALWriter> m_writer;
    std::shared_ptr<spdlog::logger> m_logger;
    IBufferPool *m_pool {};
    IDirectory *m_home {};
    bool m_has_pending {};

    using CommitEvent = Event<0, SequenceNumber>; // TODO: More state in this class (less in WALWriter), and more state passed through these?
    using AbortEvent = Event<1>;
    using AppendEvent = Event<2, PageUpdate>;
    using ExitEvent = Event<3>;
    using WALEventQueue = EventQueue<CommitEvent, AbortEvent, AppendEvent, ExitEvent>;

    WALEventQueue m_queue;
    mutable std::mutex m_queue_mutex;
    mutable std::mutex m_busy_mutex;
    std::condition_variable m_queue_cond;
    std::condition_variable m_busy_cond;
    std::queue<PageUpdate> m_pending_updates;
    std::string m_record_scratch;
    std::vector<WALSegment> m_completed_segments;
    WALSegment m_current_segment;
    Status m_writer_status {Status::ok()};
    std::optional<std::thread> m_writer_task;
    SequenceNumber m_next_lsn;
    bool m_is_busy {};
};

} // namespace cco

#endif // CCO_WAL_WAL_MANAGER_H

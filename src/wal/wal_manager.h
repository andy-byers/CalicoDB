#ifndef CCO_WAL_WAL_MANAGER_H
#define CCO_WAL_WAL_MANAGER_H

#include "interface.h"
#include "page/update.h"
#include "utils/queue.h"
#include "utils/tracker.h"

namespace cco {

class IDirectory;
class WALExplorer;

// TODO: Mechanism for writing the WAL in the background. More pertinent, however, is optimizing the WALRecord constructor, which
//       is the current bottleneck.
//class BackgroundWriter {
//public:
//    BackgroundWriter(std::unique_ptr<IWALWriter>, Queue<WALRecord>&);
//    [[nodiscard]] auto append(Page &) -> Result<void>;
//    [[nodiscard]] auto commit() -> Result<void>;
//private:
//    Queue<WALRecord> *m_queue;
//    std::unique_ptr<IWALWriter> m_writer;
//    bool m_is_running {true};
//};

class WALManager : public IWALManager {
public:
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
    auto discard(Page &) -> void override;
    auto track(Page &) -> void override;
    auto save_header(FileHeaderWriter &) -> void override;
    auto load_header(const FileHeaderReader &) -> void override;

private:
    explicit WALManager(const WALParameters &);
    [[nodiscard]] auto open_reader_segment(SegmentId) -> Result<void>;
    [[nodiscard]] auto open_writer_segment(SegmentId) -> Result<void>;
    [[nodiscard]] auto advance_writer(SequenceNumber, bool) -> Result<void>;
    [[nodiscard]] auto undo_segment(const WALSegment&) -> Result<void>;
    [[nodiscard]] auto maybe_redo_segment(WALSegment&) -> Result<void>;
    [[nodiscard]] auto roll_forward(std::vector<WALRecordPosition> &) -> Result<bool>;
    [[nodiscard]] auto roll_backward(const std::vector<WALRecordPosition> &) -> Result<void>;
    [[nodiscard]] auto read_next(WALExplorer &, std::vector<WALRecordPosition> &) -> Result<WALRecord>;
    [[nodiscard]] auto ensure_initialized() -> Result<void>;

    Tracker m_tracker;
    std::unique_ptr<IWALReader> m_reader;
    std::unique_ptr<IWALWriter> m_writer;
    std::shared_ptr<spdlog::logger> m_logger;
    std::vector<WALSegment> m_segments;
    std::string m_scratch;
    Queue<WALRecord> m_write_queue;
    WALSegment m_current;
    IBufferPool *m_pool {};
    IDirectory *m_home {};
    bool m_has_pending {};
};

} // namespace cco

#endif // CCO_WAL_WAL_MANAGER_H

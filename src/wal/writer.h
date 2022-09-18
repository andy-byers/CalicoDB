#ifndef CALICO_WAL_WAL_WRITER_H
#define CALICO_WAL_WAL_WRITER_H

#include "helpers.h"
#include "utils/crc.h"
#include "wal.h"
#include <memory>
#include <optional>
#include <queue>
#include <spdlog/logger.h>
#include <thread>

#include "utils/worker.h"

namespace calico {

class LogWriter {
public:
    // NOTE: LogWriter must always be created on an empty segment file.
    LogWriter(AppendWriter &file, Bytes tail, std::atomic<SequenceId> &flushed_lsn)
        : m_flushed_lsn {&flushed_lsn},
          m_file {&file},
          m_tail {tail}
    {}

    [[nodiscard]]
    auto block_count() const -> Size
    {
        return m_number;
    }

    // NOTE: If either of these methods return a non-OK status, the state of this object is unspecified for the most part.
    //       However, we can still query the block count to see if we've actually written out any blocks.
    [[nodiscard]] auto write(WalPayloadIn payload) -> Status;
    [[nodiscard]] auto flush() -> Status;

private:
    [[nodiscard]] auto clear_rest_and_flush(Size local_offset) -> Status;

    std::atomic<SequenceId> *m_flushed_lsn {};
    SequenceId m_last_lsn {};
    AppendWriter *m_file {};
    Bytes m_tail;
    Size m_number {};
    Size m_offset {};
};

class WalWriter {
public:

    // The main thread will block after the worker has queued up this number of write requests.
    static constexpr Size WORKER_CAPACITY {16};

    WalWriter(Storage &store, WalCollection &segments, LogScratchManager &scratch, Bytes tail, std::atomic<SequenceId> &flushed_lsn, std::string prefix, Size wal_limit)
        : m_worker {WORKER_CAPACITY, [this](const auto &event) {
              return on_event(event);
          }},
          m_prefix(std::move(prefix)),
          m_flushed_lsn {&flushed_lsn},
          m_scratch {&scratch},
          m_segments {&segments},
          m_store {&store},
          m_tail {tail},
          m_wal_limit {wal_limit}
    {}

    [[nodiscard]]
    auto status() const -> Status
    {
        // Can be either OK or SYSTEM_ERROR.
        return m_worker.status();
    }

    // NOTE: open() should be called immediately after construction. This object is not valid unless open() returns OK.
    //       When this object is no longer needed, destroy() should be called.
    [[nodiscard]] auto open() -> Status;
    [[nodiscard]] auto destroy() && -> Status;

    auto write(WalPayloadIn payload) -> void;

    // NOTE: advance() will block until the writer has advanced to a new segment. It should be called after writing
    //       a commit record so that everything is written to disk before we return, and the writer is set up on the
    //       next segment. flush() will also block until the tail buffer has been flushed.
    auto advance() -> void;
    auto flush() -> void;

private:
    struct AdvanceToken {};
    struct FlushToken {};

    using Event = std::variant<WalPayloadIn, AdvanceToken, FlushToken>;

    [[nodiscard]] auto on_event(const Event &event) -> Status;
    [[nodiscard]] auto advance_segment() -> Status;
    [[nodiscard]] auto open_segment(SegmentId) -> Status;
    auto close_segment() -> Status;

    Worker<Event> m_worker;
    std::string m_prefix;
    std::optional<LogWriter> m_writer;
    std::atomic<SequenceId> *m_flushed_lsn {};
    std::unique_ptr<AppendWriter> m_file;
    LogScratchManager *m_scratch {};
    WalCollection *m_segments {};
    Storage *m_store {};
    Bytes m_tail;
    Size m_wal_limit {};
};

} // namespace calico

#endif // CALICO_WAL_WAL_WRITER_H
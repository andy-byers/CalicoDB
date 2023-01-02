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

namespace Calico {

class LogWriter {
public:
    // NOTE: LogWriter must always be created on an empty segment file.
    LogWriter(AppendWriter &file, Bytes tail, std::atomic<Id> &flushed_lsn)
        : m_tail {tail},
          m_flushed_lsn {&flushed_lsn},
          m_file {&file}
    {}

    [[nodiscard]]
    auto block_count() const -> Size
    {
        return m_number;
    }

    // NOTE: If either of these methods return a non-OK status, the state of this object is unspecified, except for the block
    //       count, which remains valid.
    [[nodiscard]] auto write(WalPayloadIn payload) -> Status;
    [[nodiscard]] auto flush() -> Status;

private:
    Bytes m_tail;
    std::atomic<Id> *m_flushed_lsn {};
    AppendWriter *m_file {};
    Id m_last_lsn {};
    Size m_number {};
    Size m_offset {};
};

class WalWriter {
public:
    WalWriter(Storage &store, WalCollection &segments, Bytes tail, std::atomic<Id> &flushed_lsn, std::string prefix, Size wal_limit)
        : m_worker {WORKER_CAPACITY, [this](const auto &event) {
              return on_event(event);
          }},
          m_prefix(std::move(prefix)),
          m_flushed_lsn {&flushed_lsn},
          m_set {&segments},
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
    std::atomic<Id> *m_flushed_lsn {};
    std::unique_ptr<AppendWriter> m_file;
    WalCollection *m_set {};
    Storage *m_store {};
    Bytes m_tail;
    Size m_wal_limit {};
};

} // namespace Calico

#endif // CALICO_WAL_WAL_WRITER_H
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
    LogWriter(AppendWriter &file, Span tail, std::atomic<Id> &flushed_lsn)
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
    Span m_tail;
    std::atomic<Id> *m_flushed_lsn {};
    AppendWriter *m_file {};
    Lsn m_last_lsn {};
    Size m_number {};
    Size m_offset {};
};

class WalWriter {
public:
    struct Parameters {
        Slice prefix;
        Span tail;
        Storage *storage {};
        System *system {};
        WalSet *set {};
        std::atomic<Id> *flushed_lsn {};
        Size wal_limit {};
    };

    explicit WalWriter(const Parameters &param);

    [[nodiscard]] auto destroy() && -> Status;
    auto write(WalPayloadIn payload) -> void;
    // NOTE: advance() will block until the writer has advanced to a new segment. It should be called after writing
    //       a commit record so that everything is written to disk before we return, and the writer is set up on the
    //       next segment. flush() will also block until the tail buffer has been flushed.
    auto advance() -> void;
    auto flush() -> void;

private:
    [[nodiscard]] auto advance_segment() -> Status;
    [[nodiscard]] auto open_segment(SegmentId) -> Status;
    auto close_segment() -> Status;

    std::string m_prefix;
    std::optional<LogWriter> m_writer;
    std::unique_ptr<AppendWriter> m_file;
    std::atomic<Id> *m_flushed_lsn {};
    Storage *m_storage {};
    System *system {};
    WalSet *m_set {};
    SegmentId m_current;
    Span m_tail;
    Size m_wal_limit {};
};

} // namespace Calico

#endif // CALICO_WAL_WAL_WRITER_H
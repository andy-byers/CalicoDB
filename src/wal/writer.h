#ifndef CALICO_WAL_WAL_WRITER_H
#define CALICO_WAL_WAL_WRITER_H

#include "helpers.h"
#include "utils/crc.h"
#include "wal.h"
#include <memory>
#include <optional>

namespace Calico {

class LogWriter {
public:
    // NOTE: LogWriter must always be created on an empty segment file.
    LogWriter(Logger &file, Span tail, Lsn &flushed_lsn)
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
    Lsn *m_flushed_lsn {};
    Logger *m_file {};
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
        ErrorBuffer *error {};
        WalSet *set {};
        Lsn *flushed_lsn {};
        Size wal_limit {};
    };

    explicit WalWriter(const Parameters &param);

    auto destroy() && -> void;
    auto write(WalPayloadIn payload) -> void;
    // NOTE: advance() will block until the writer has advanced to a new segment. It should be called after writing
    //       a commit record so that everything is written to disk before we return, and the writer is set up on the
    //       next segment. flush() will also block until the tail buffer has been flushed.
    auto advance() -> void;
    auto flush() -> void;

private:
    [[nodiscard]] auto advance_segment() -> Status;
    [[nodiscard]] auto open_segment(Id) -> Status;
    auto close_segment() -> Status;

    std::string m_prefix;
    std::optional<LogWriter> m_writer;
    std::unique_ptr<Logger> m_file;
    Lsn *m_flushed_lsn {};
    Storage *m_storage {};
    ErrorBuffer *m_error {};
    WalSet *m_set {};
    Id m_current;
    Span m_tail;
    Size m_wal_limit {};
};

} // namespace Calico

#endif // CALICO_WAL_WAL_WRITER_H
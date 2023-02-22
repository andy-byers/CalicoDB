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
    LogWriter(Logger &file, Span tail, Size file_size = 0)
        : m_tail {tail},
          m_file {&file},
          m_block {file_size / tail.size()},
          m_offset {file_size % tail.size()}
    {
        // NOTE: File must be a multiple of the block size.
        CALICO_EXPECT_EQ(m_offset, 0);
    }

    [[nodiscard]]
    auto block_count() const -> Size
    {
        return m_block;
    }

    // NOTE: If either of these methods return a non-OK status, the state of this object is unspecified, except for the block
    //       count, which remains valid.
    [[nodiscard]] auto write(WalPayloadIn payload) -> Status;
    [[nodiscard]] auto flush() -> Status;

    // NOTE: Only valid if the writer has flushed.
    [[nodiscard]] auto flushed_lsn() const -> Lsn;

private:
    Span m_tail;
    Lsn m_flushed_lsn;
    Logger *m_file {};
    Lsn m_last_lsn {};
    Size m_block {};
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
        Size wal_limit {};
    };

    explicit WalWriter(const Parameters &param);

    auto destroy() && -> void;
    auto write(WalPayloadIn payload) -> void;
    auto advance() -> void;
    auto flush() -> void;
    auto sync() -> void;

    [[nodiscard]] auto flushed_lsn() const -> Lsn;

private:
    [[nodiscard]] auto advance_segment() -> Status;
    [[nodiscard]] auto open_segment(Id) -> Status;
    auto close_segment() -> Status;

    std::string m_prefix;
    std::optional<LogWriter> m_writer;
    std::unique_ptr<Logger> m_file;
    Storage *m_storage {};
    ErrorBuffer *m_error {};
    WalSet *m_set {};
    Id m_current;
    Span m_tail;
    Size m_wal_limit {};
};

} // namespace Calico

#endif // CALICO_WAL_WAL_WRITER_H
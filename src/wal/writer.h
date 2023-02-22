#ifndef CALICO_WAL_WAL_WRITER_H
#define CALICO_WAL_WAL_WRITER_H

#include "helpers.h"
#include "utils/crc.h"
#include "wal.h"
#include <memory>
#include <optional>

namespace Calico {

class WalWriter {
public:
    WalWriter(Logger &file, Span tail, Size file_size = 0)
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

class WalWriter_ {
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

    explicit WalWriter_(const Parameters &param);

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
    std::optional<WalWriter> m_writer;
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
#ifndef CALICODB_WAL_WRITER_H
#define CALICODB_WAL_WRITER_H

#include "wal_record.h"

namespace calicodb
{

class WalWriter
{
public:
    WalWriter(Logger &file, Span tail)
        : m_tail {tail},
          m_file {&file}
    {
    }

    [[nodiscard]] auto block_count() const -> std::size_t
    {
        return m_block;
    }

    // NOTE: If either of these methods return a non-OK status, the state of this object is unspecified, except for the block
    //       count, which remains valid.
    [[nodiscard]] auto write(Lsn lsn, const Slice &payload) -> Status;
    [[nodiscard]] auto flush() -> Status;

    // NOTE: Only valid if the writer has flushed.
    [[nodiscard]] auto flushed_lsn() const -> Lsn;

private:
    Span m_tail;
    Lsn m_flushed_lsn;
    Logger *m_file {};
    Lsn m_last_lsn {};
    std::size_t m_block {};
    std::size_t m_offset {};
};

} // namespace calicodb

#endif // CALICODB_WAL_WRITER_H
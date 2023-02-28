#ifndef CALICODB_WAL_READER_H
#define CALICODB_WAL_READER_H

#include "wal_record.h"

namespace calicodb
{

class WalReader
{
    Span m_tail;
    Reader *m_file {};
    std::size_t m_offset {};
    std::size_t m_block {};

public:
    explicit WalReader(Reader &file, Span tail);
    [[nodiscard]] auto read(Span &payload) -> Status;
};

} // namespace calicodb

#endif // CALICODB_WAL_READER_H
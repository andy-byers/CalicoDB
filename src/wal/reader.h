#ifndef CALICO_WAL_READER_H
#define CALICO_WAL_READER_H

#include "helpers.h"
#include "record.h"
#include "utils/crc.h"
#include "wal.h"
#include <optional>

namespace Calico {

class WalReader {
    Span m_tail;
    Reader *m_file {};
    Size m_offset {};
    Size m_block {};

public:
    explicit WalReader(Reader &file, Span tail);
    [[nodiscard]] auto read(Span &payload) -> Status;
    [[nodiscard]] auto offset() const -> Size;
};

} // namespace Calico

#endif // CALICO_WAL_READER_H
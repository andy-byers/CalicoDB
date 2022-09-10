#ifndef CALICO_WAL_READER_H
#define CALICO_WAL_READER_H

#include "helpers.h"
#include "record.h"
#include "utils/crc.h"
#include "wal.h"
#include <optional>
#include <thread>

namespace calico {

class Storage;

class BasicWalReader {
public:
    explicit BasicWalReader(Storage &store, std::string prefix, Size page_size)
        : m_reader {wal_block_size(page_size)},
          m_prefix {std::move(prefix)},
          m_payload(wal_scratch_size(page_size), '\x00'),
          m_store {&store}
    {}

    [[nodiscard]] auto open(SegmentId id) -> Status;
    [[nodiscard]] auto close() -> Status;
    [[nodiscard]] auto read_first_lsn(SequenceId&) -> Status;
    [[nodiscard]] auto redo(const RedoCallback &) -> Status;
    [[nodiscard]] auto undo(const UndoCallback &) -> Status;

private:
    [[nodiscard]] auto prepare_traversal() -> Status;
    [[nodiscard]] auto read_logical_record(WalRecordHeader &header, Bytes payload) -> Status;

    SegmentId m_segment_id;
    SequentialLogReader m_reader;
    std::string m_prefix;
    std::string m_payload;
    Storage *m_store {};
};

} // namespace calico

#endif // CALICO_WAL_READER_H
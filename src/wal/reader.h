/**
*
* References
*   (1) https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-IFile-Format
*/

#ifndef CALICO_WAL_READER_H
#define CALICO_WAL_READER_H

#include "calico/wal.h"
#include "helpers.h"
#include "record.h"
#include "utils/crc.h"
#include <optional>
#include <thread>

namespace calico {

class Storage;

class BasicWalReader {
public:
    using PositionList = std::vector<RecordPosition>;
    using UndoIterator = PositionList::const_reverse_iterator;

    explicit BasicWalReader(Storage &store, std::string dirname, Size page_size)
        : m_redo_reader {page_size * WAL_BLOCK_SCALE},
          m_undo_reader {page_size * WAL_BLOCK_SCALE},
          m_redo_filter {[](auto type) {
              return type == WalPayloadType::DELTAS || type == WalPayloadType::COMMIT;
          }},
          m_undo_filter {[](auto type) {
              return type == WalPayloadType::FULL_IMAGE;
          }},
          m_dirname {std::move(dirname)},
          m_payload(page_size * WAL_SCRATCH_SCALE, '\x00'),
          m_store {&store}
    {}

    [[nodiscard]] auto open(SegmentId id) -> Status;
    [[nodiscard]] auto close() -> Status;
    [[nodiscard]] auto read_first_lsn(SequenceId&) -> Status;
    [[nodiscard]] auto redo(PositionList &, const RedoCallback &) -> Status;
    [[nodiscard]] auto undo(const UndoIterator&, const UndoIterator&, const UndoCallback &) -> Status;

private:
    [[nodiscard]] auto prepare_forward_traversal() -> Status;
    [[nodiscard]] auto prepare_reverse_traversal() -> Status;
    [[nodiscard]] auto forward_read_logical_record(WalRecordHeader &header, Bytes payload, PositionList &positions) -> Status;
    [[nodiscard]] auto reverse_read_logical_record(WalRecordHeader &header, Bytes payload, UndoIterator &current, const UndoIterator &end) -> Status;

    SegmentId m_segment_id;
    SequentialLogReader m_redo_reader;
    RandomLogReader m_undo_reader;
    WalFilter m_redo_filter;
    WalFilter m_undo_filter;
    std::string m_dirname;
    std::string m_payload;
    Storage *m_store {};
};

} // namespace cco

#endif // CALICO_WAL_READER_H
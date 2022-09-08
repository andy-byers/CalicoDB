#ifndef CALICO_WAL_ITERATOR_H
#define CALICO_WAL_ITERATOR_H

#include "wal.h"
#include "helpers.h"
#include "record.h"

namespace calico {

class Storage;

class BasicWalIterator: public WalIterator {
public:
    explicit BasicWalIterator(Storage &store, WalCollection &collection, std::string prefix, Size page_size)
        : m_undo_reader {wal_block_size(page_size)},
          m_redo_reader {wal_block_size(page_size)},
          m_undo_filter {[](auto type) {
              return type == WalPayloadType::FULL_IMAGE;
          }},
          m_redo_filter {[](auto type) {
              return type == WalPayloadType::DELTAS || type == WalPayloadType::COMMIT;
          }},
          m_prefix {std::move(prefix)},
          m_payload(wal_scratch_size(page_size), '\x00'),
          m_store {&store},
          m_collection {&collection}
    {}

    ~BasicWalIterator() override
    {
        close_segment();
    }

    // Should be called by the WAL object before handing over to other components to use.
    [[nodiscard]] auto open() -> Status;

    [[nodiscard]] auto seek_next_segment() -> Status override;
    [[nodiscard]] auto seek_previous_segment() -> Status override;
    [[nodiscard]] auto read_first_lsn(SequenceId&) -> Status override;
    [[nodiscard]] auto redo(const RedoCallback&) -> Status override;
    [[nodiscard]] auto undo(const UndoCallback&) -> Status override;

private:
    using PositionList = std::vector<RecordPosition>;
    using UndoIterator = PositionList::const_reverse_iterator;

    auto close_segment() -> void;
    [[nodiscard]] auto open_segment(SegmentId) -> Status;
    [[nodiscard]] auto prepare_forward_traversal() -> Status;
    [[nodiscard]] auto prepare_reverse_traversal() -> Status;
    [[nodiscard]] auto forward_handle_logical_record(WalRecordHeader &header, PositionList &positions, const std::function<void(BytesView)> &callback) -> Status;
    [[nodiscard]] auto reverse_read_logical_record(WalRecordHeader &header, Bytes payload, UndoIterator &current, const UndoIterator &end) -> Status;
    [[nodiscard]] auto fast_read_through() -> Status;

    PositionList m_positions;
    RandomLogReader m_undo_reader;
    SequentialLogReader m_redo_reader;
    WalFilter m_undo_filter;
    WalFilter m_redo_filter;
    SegmentId m_segment_id;
    std::string m_prefix;
    std::string m_payload;
    Storage *m_store {};
    WalCollection *m_collection {};
};

} // namespace calico

#endif // CALICO_WAL_ITERATOR_H
#ifndef CALICO_WAL_READER_H
#define CALICO_WAL_READER_H

#include "helpers.h"
#include "record.h"
#include "utils/crc.h"
#include "wal.h"
#include <optional>
#include <thread>

namespace calico {

class LogReader final {
public:
    explicit LogReader(RandomReader &file)
        : m_file {&file}
    {}

    // NOTE: If this method returns a non-OK status, the state of this object is unspecified.
    [[nodiscard]] auto read(Bytes &out, Bytes tail) -> Status;

private:
    [[nodiscard]] auto read_logical_record(Bytes &out, Bytes tail) -> Status;

    // NOTE: Doesn't take ownership of the file.
    RandomReader *m_file {};
    Size m_number {};
    Size m_offset {};
};

class WalReader final {
public:
    WalReader(Storage &store, WalCollection &segments, std::string prefix, Bytes scratch, Bytes payload)
        : m_prefix {std::move(prefix)},
          m_store {&store},
          m_segments {&segments},
          m_scratch {scratch},
          m_payload {payload}
    {}

    ~WalReader()
    {
        if (!m_current.is_null())
            close_segment();
    }

    [[nodiscard]] auto open() -> Status;
    [[nodiscard]] auto seek_next() -> Status;
    [[nodiscard]] auto seek_previous() -> Status;
    [[nodiscard]] auto read_first_lsn(SequenceId&) -> Status;
    [[nodiscard]] auto roll(const GetPayload&) -> Status;

private:
    [[nodiscard]] auto prepare_traversal() -> Status;
    [[nodiscard]] auto open_segment(SegmentId) -> Status;
    auto close_segment() -> void;

    std::string m_prefix;
    std::optional<LogReader> m_reader;
    std::unique_ptr<RandomReader> m_file;
    Storage *m_store {};
    WalCollection *m_segments {};
    Bytes m_scratch;
    Bytes m_payload;
    SegmentId m_current;
};

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
    LogReader_ m_reader;
    std::string m_prefix;
    std::string m_payload;
    Storage *m_store {};
};

} // namespace calico

#endif // CALICO_WAL_READER_H
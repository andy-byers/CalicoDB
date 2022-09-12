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

    // NOTE: If either of these methods returns a non-OK status, the state of this object is unspecified.
    [[nodiscard]] auto read_first_lsn(SequenceId &out) -> Status;
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

    // NOTE: The "has_commit" field of the WAL segment descriptor returned by this method is not meaningful until the
    //       WAL segments have been explored during the first phase of recovery. TODO: Or store the LSN of the last commit??? and use that???
    [[nodiscard]] auto segment() const -> WalSegment;

private:
    [[nodiscard]] auto open_segment(SegmentId) -> Status;
    auto prepare_traversal() -> void;
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

} // namespace calico

#endif // CALICO_WAL_READER_H
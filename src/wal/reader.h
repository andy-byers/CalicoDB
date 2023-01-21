#ifndef CALICO_WAL_READER_H
#define CALICO_WAL_READER_H

#include "helpers.h"
#include "record.h"
#include "utils/crc.h"
#include "wal.h"
#include <optional>
#include <thread>

namespace Calico {

class LogReader final {
public:
    explicit LogReader(RandomReader &file)
        : m_file {&file}
    {}

    // NOTE: If either of these methods returns a non-OK status, the state of this object is unspecified.
    [[nodiscard]] auto read_first_lsn(Lsn &out) -> Status;
    [[nodiscard]] auto read(WalPayloadOut &out, Span payload, Span tail) -> Status;

private:
    [[nodiscard]] auto read_logical_record(Span &out, Span tail) -> Status;

    // NOTE: Doesn't take ownership of the file.
    RandomReader *m_file {};
    Size m_number {};
    Size m_offset {};
};

class WalReader final {
public:
    using Callback = WriteAheadLog::Callback;

    WalReader(Storage &store, WalSet &segments, std::string prefix, Span tail, Span data)
        : m_prefix {std::move(prefix)},
          m_store {&store},
          m_set {&segments},
          m_tail {tail},
          m_data {data}
    {}

    ~WalReader()
    {
        if (!m_current.is_null())
            close_segment();
    }

    [[nodiscard]]
    auto segment_id() const -> SegmentId
    {
        return m_current;
    }

    [[nodiscard]] auto open() -> Status;
    [[nodiscard]] auto seek_next() -> Status;
    [[nodiscard]] auto seek_previous() -> Status;

    // TODO: LevelDB caches first LSNs of WAL segments. If we end up reading this info a lot, we may want to do the same. We could use this method
    //       for that, and store the cache in WALCollection, so it lives longer than this object, which is meant to be transient.
    [[nodiscard]] auto read_first_lsn(Id &) -> Status;
    [[nodiscard]] auto roll(const Callback&) -> Status;

    // NOTE: Necessary due to the non-trivial destructor.
    WalReader(WalReader &&rhs) noexcept = default;
    auto operator=(WalReader &&rhs) noexcept -> WalReader & = default;

private:
    [[nodiscard]] auto open_segment(SegmentId) -> Status;
    auto prepare_traversal() -> void;
    auto close_segment() -> void;

    std::string m_prefix;
    std::optional<LogReader> m_reader;
    std::unique_ptr<RandomReader> m_file;
    Storage *m_store {};
    WalSet *m_set {};
    Span m_tail;
    Span m_data;
    SegmentId m_current;
};

} // namespace Calico

#endif // CALICO_WAL_READER_H
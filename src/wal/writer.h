#ifndef CALICO_WAL_WAL_WRITER_H
#define CALICO_WAL_WAL_WRITER_H

#include "helpers.h"
#include "utils/crc.h"
#include "wal.h"
#include <memory>
#include <optional>
#include <queue>
#include <spdlog/logger.h>
#include <thread>

#include "utils/worker.h"

namespace Calico {

class LogWriter {
public:
    // NOTE: LogWriter must always be created on an empty segment file.
    LogWriter(AppendWriter &file, Bytes tail, std::atomic<Id> &flushed_lsn)
        : m_tail {tail},
          m_flushed_lsn {&flushed_lsn},
          m_file {&file}
    {}

    [[nodiscard]]
    auto block_count() const -> Size
    {
        return m_number;
    }

    // NOTE: If either of these methods return a non-OK status, the state of this object is unspecified, except for the block
    //       count, which remains valid.
    [[nodiscard]] auto write(WalPayloadIn payload) -> Status;
    [[nodiscard]] auto flush() -> Status;

private:
    Bytes m_tail;
    std::atomic<Id> *m_flushed_lsn {};
    AppendWriter *m_file {};
    Id m_last_lsn {};
    Size m_number {};
    Size m_offset {};
};

class WalWriter {
public:
    struct Parameters {
        Storage *storage {};
        WalSet *set {};
        Bytes tail;
        std::atomic<Id> *flushed_lsn {};
        std::string prefix;
        Size wal_limit {};
        Size capacity {};
    };

    explicit WalWriter(const Parameters &param)
        : m_worker {param.capacity, [this](const auto &event) {
              return on_event(event);
          }},
          m_prefix {param.prefix},
          m_flushed_lsn {param.flushed_lsn},
          m_set {param.set},
          m_store {param.storage},
          m_tail {param.tail},
          m_wal_limit {param.wal_limit}
    {}

    [[nodiscard]]
    auto status() const -> Status
    {
        // Can be either OK or SYSTEM_ERROR.
        return m_worker.status();
    }

    // NOTE: open() should be called immediately after construction. This object is not valid unless open() returns OK.
    //       When this object is no longer needed, destroy() should be called.
    [[nodiscard]] auto open() -> Status;
    [[nodiscard]] auto destroy() && -> Status;

    auto write(WalPayloadIn payload) -> void;

    // NOTE: advance() will block until the writer has advanced to a new segment. It should be called after writing
    //       a commit record so that everything is written to disk before we return, and the writer is set up on the
    //       next segment. flush() will also block until the tail buffer has been flushed.
    auto advance() -> void;
    auto flush() -> void;

private:
    struct AdvanceToken {};
    struct FlushToken {};

    using Event = std::variant<WalPayloadIn, AdvanceToken, FlushToken>;

    [[nodiscard]] auto on_event(const Event &event) -> Status;
    [[nodiscard]] auto advance_segment() -> Status;
    [[nodiscard]] auto open_segment(SegmentId) -> Status;
    auto close_segment() -> Status;

    Worker<Event> m_worker;
    std::string m_prefix;
    std::optional<LogWriter> m_writer;
    std::atomic<Id> *m_flushed_lsn {};
    std::unique_ptr<AppendWriter> m_file;
    WalSet *m_set {};
    Storage *m_store {};
    Bytes m_tail;
    Size m_wal_limit {};
};

class WalWriterTask {
public:
    struct Parameters {
        Slice prefix;
        Bytes tail;
        Storage *storage {};
        System *system {};
        WalSet *set {};
        std::atomic<Id> *flushed_lsn {};
        Size wal_limit {};
        Size capacity {};
    };

    explicit WalWriterTask(const Parameters &param)
        : m_work {param.capacity},
          m_prefix {param.prefix.to_string()},
          m_flushed_lsn {param.flushed_lsn},
          m_storage {param.storage},
          m_system {param.system},
          m_set {param.set},
          m_tail {param.tail},
          m_wal_limit {param.wal_limit}
    {
        CALICO_EXPECT_FALSE(m_prefix.empty());
        CALICO_EXPECT_NE(m_flushed_lsn, nullptr);
        CALICO_EXPECT_NE(m_storage, nullptr);
        CALICO_EXPECT_NE(m_system, nullptr);
        CALICO_EXPECT_NE(m_set, nullptr);
    }

    auto operator()() -> void
    {
        const auto event = m_work.try_dequeue();

        if (!event.has_value())
            return;

        if (std::holds_alternative<WalPayloadIn>(*event)) {
            auto payload = std::get<WalPayloadIn>(*event);
            CALICO_ERROR_IF(m_writer->write(payload));
            if (m_writer->block_count() >= m_wal_limit)
                CALICO_ERROR_IF(advance_segment());
        } else if (std::holds_alternative<AdvanceToken>(*event)) {
            CALICO_ERROR_IF(advance_segment());
        } else {
            CALICO_EXPECT_TRUE(std::holds_alternative<FlushToken>(*event));
            auto s = m_writer->flush();
            // Throw away logic errors due to the tail buffer being empty.
            CALICO_ERROR_IF(s.is_logic_error() ? ok() : s);
        }
    }

    // NOTE: open() should be called immediately after construction. This object is not valid unless open() returns OK.
    //       When this object is no longer needed, destroy() should be called.
    [[nodiscard]] auto open() -> Status;
    [[nodiscard]] auto destroy() && -> Status;

    auto write(WalPayloadIn payload) -> void;

    // NOTE: advance() will block until the writer has advanced to a new segment. It should be called after writing
    //       a commit record so that everything is written to disk before we return, and the writer is set up on the
    //       next segment. flush() will also block until the tail buffer has been flushed.
    auto advance() -> void;
    auto flush() -> void;

private:
    struct AdvanceToken {};
    struct FlushToken {};

    using Event = std::variant<WalPayloadIn, AdvanceToken, FlushToken>;

    [[nodiscard]] auto advance_segment() -> Status;
    [[nodiscard]] auto open_segment(SegmentId) -> Status;
    auto close_segment() -> Status;

    Queue<Event> m_work;
    std::string m_prefix;
    std::optional<LogWriter> m_writer;
    std::unique_ptr<AppendWriter> m_file;
    std::atomic<Id> *m_flushed_lsn {};
    Storage *m_storage {};
    System *m_system {};
    WalSet *m_set {};
    Bytes m_tail;
    Size m_wal_limit {};
};

} // namespace Calico

#endif // CALICO_WAL_WAL_WRITER_H
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

#include "worker.h"

namespace calico {

class WalRecordWriter {
public:
    using SetFlushedLsn = std::function<void(SequenceId)>;

    explicit WalRecordWriter(Size buffer_size)
        : m_buffer {buffer_size}
    {}

    [[nodiscard]]
    auto is_attached() const -> bool
    {
        return m_file != nullptr;
    }

    [[nodiscard]]
    auto block_count() const -> Size
    {
        // TODO: This is reset in detach! Make sure we don't need it!
        return m_block_count;
    }

    [[nodiscard]]
    auto has_written() const -> bool
    {
        return m_buffer.block_number() || m_buffer.block_offset();
    }

    auto attach(AppendWriter *file) -> void
    {
        CALICO_EXPECT_FALSE(is_attached());
        m_last_lsn = SequenceId::null();
        m_file.reset(file);
    }

    auto detach(const SetFlushedLsn &update) -> Status
    {
        CALICO_EXPECT_TRUE(is_attached());
        auto s = Status::ok();
        if (m_buffer.block_offset())
            s = append_block();
        if (s.is_ok())
            s = m_file->sync();
        if (s.is_ok() && has_written())
            update(m_last_lsn);
        m_block_count = 0;
        m_buffer.reset();
        m_last_lsn = SequenceId::null();
        m_file.reset();
        return s;
    }

    auto write(SequenceId lsn, BytesView payload, const SetFlushedLsn &update) -> Status
    {
        CALICO_EXPECT_TRUE(is_attached());
        CALICO_EXPECT_FALSE(lsn.is_null());
        const SequenceId last_lsn {lsn.value - 1};

        WalRecordHeader lhs {};
        lhs.lsn = lsn.value;
        lhs.type = WalRecordHeader::Type::FULL;
        lhs.size = static_cast<std::uint16_t>(payload.size());
        lhs.crc = crc_32(payload);

        for (; ; ) {
            const auto space_remaining = m_buffer.remaining().size();
            const auto can_fit_some = space_remaining > sizeof(WalRecordHeader);
            const auto can_fit_all = space_remaining >= sizeof(WalRecordHeader) + payload.size();

            if (can_fit_some) {
                WalRecordHeader rhs {};

                if (!can_fit_all)
                    rhs = split_record(lhs, payload, space_remaining);

                // We must have room for the whole header and at least 1 payload byte.
                write_wal_record_header(m_buffer.remaining(), lhs);
                m_buffer.advance_cursor(sizeof(lhs));
                mem_copy(m_buffer.remaining(), payload.range(0, lhs.size));
                m_buffer.advance_cursor(lhs.size);
                payload.advance(lhs.size);

                if (can_fit_all) {
                    CALICO_EXPECT_TRUE(payload.is_empty());
                    break;
                }
                lhs = rhs;
                continue;
            }
            auto s = append_block();
            if (!s.is_ok()) return s;

            // This may happen more than once, but should still be correct (when the current record spans multiple blocks).
            if (!last_lsn.is_null())
                update(last_lsn);
        }
        m_last_lsn = lsn;
        return Status::ok();
    }

    auto append_block() -> Status
    {
        return m_buffer.advance_block([this] {
            // Clear unused bytes at the end of the tail buffer.
            mem_clear(m_buffer.remaining());
            auto s = m_file->write(m_buffer.block());
            //            if (s.is_ok()) s = m_file->sync(); // TODO
            m_block_count += s.is_ok();
            return s;
        });
    }

private:
    std::unique_ptr<AppendWriter> m_file;
    SequenceId m_last_lsn;
    WalBuffer m_buffer;
    Size m_block_count {};
};

class BackgroundWriter {
public:
    struct Parameters {
        Storage *store {};
        LogScratchManager *scratch {};
        WalCollection *collection {};
        std::atomic<SequenceId> *flushed_lsn {};
        std::shared_ptr<spdlog::logger> logger;
        std::string prefix;
        Size block_size {};
        Size wal_limit {};
    };

    enum class EventType {
        STOP_WRITER = 1,
        FLUSH_BLOCK,
        LOG_FULL_IMAGE,
        LOG_DELTAS,
        LOG_COMMIT,
    };

    struct Event {
        EventType type {};
        SequenceId lsn;
        std::optional<NamedScratch> buffer {};
        Size size {};
        bool is_waiting {};
    };

    explicit BackgroundWriter(const Parameters &param)
        : m_background {
              [this](const auto &s, const auto &e) {
                  return s.is_ok() ? on_event(s, e) : s;
              },
              [this](const auto &s) {
                  return on_cleanup(s);
              }
          },
          m_logger {param.logger},
          m_flushed_lsn {param.flushed_lsn},
          m_writer {param.block_size},
          m_prefix {param.prefix},
          m_scratch {param.scratch},
          m_collection {param.collection},
          m_store {param.store},
          m_wal_limit {param.wal_limit},
          m_guard {*param.store, m_writer, *param.collection, *param.flushed_lsn, param.prefix}
    {}

    ~BackgroundWriter() = default;

    [[nodiscard]]
    auto is_running() const -> bool
    {
        return m_is_running;
    }

    auto dispatch(Event event) -> void
    {
        // TODO: Make event.is_waiting a parameter to this method instead of a struct member...
        m_background.dispatch(event, event.is_waiting);
    }

    auto startup() -> Status
    {
        auto s = m_guard.start();
        m_is_running = s.is_ok();
        if (!m_is_running)
            handle_error(m_guard, s);
        return s;
    }

    auto teardown() -> Status
    {
        m_is_running = false;
        return std::move(m_background).destroy();
    }

    [[nodiscard]]
    auto status() -> Status
    {
        return m_background.status();
    }

private:

    [[nodiscard]]
    auto run_stop(SegmentGuard &guard) -> Status
    {
        if (!guard.is_started()) // TODO
            return Status::ok();

        if (m_writer.has_written())
            return guard.finish(false);

        const auto id = guard.id();
        auto s = guard.abort();
        if (!s.is_ok()) handle_error(guard, s);
        return m_store->remove_file(m_prefix + id.to_name());
    }

    [[nodiscard]]
    auto needs_segmentation() const -> bool
    {
        return m_writer.block_count() > m_wal_limit;
    }

    auto on_event(const Status &status, const Event &event) -> Status
    {
        // We already encountered an error and have it assigned to our current status.
        if (!status.is_ok())
            return Status::ok();

        auto s = Status::ok();
        bool should_segment {};
        bool has_commit {};

        const auto handle_event = [&] {
            auto [
                type,
                lsn,
                buffer,
                size,
                is_waiting
            ] = event;

            switch (type) {
                case EventType::LOG_FULL_IMAGE:
                case EventType::LOG_DELTAS:
                    CALICO_EXPECT_TRUE(buffer.has_value());
                    s = emit_payload(lsn, (*buffer)->truncate(size));
                    should_segment = needs_segmentation();
                    break;
                case EventType::LOG_COMMIT:
                    s = emit_commit(lsn);
                    should_segment = s.is_ok();
                    has_commit = true;
                    break;
                case EventType::FLUSH_BLOCK:
                    s = m_writer.append_block();
                    m_flushed_lsn->store(lsn);
                    break;
                case EventType::STOP_WRITER:
                    s = run_stop(m_guard);
                    break;
                default:
                    CALICO_EXPECT_TRUE(false && "unrecognized WAL event type");
            }
        };

        if (status.is_ok() || event.type == EventType::STOP_WRITER)
            handle_event();

        // Replace the scratch memory so that the main thread can reuse it. This is internally synchronized.
        if (event.buffer) m_scratch->put(*event.buffer);


        if (s.is_ok() && should_segment) {
            s = advance_segment(m_guard, has_commit);
            if (s.is_ok()) m_flushed_lsn->store(event.lsn);
        }
        return s;
    }

    auto on_cleanup(const Status &) -> Status
    {
        return run_stop(m_guard); // TODO
    }

    auto background_writer() -> void;
    [[nodiscard]] auto emit_payload(SequenceId lsn, BytesView payload) -> Status;
    [[nodiscard]] auto emit_commit(SequenceId lsn) -> Status;
    [[nodiscard]] auto advance_segment(SegmentGuard &guard, bool has_commit) -> Status;
    auto handle_error(SegmentGuard &guard, Status e) -> void;

    BackgroundWorker<Event> m_background;
    std::shared_ptr<spdlog::logger> m_logger;
    std::atomic<SequenceId> *m_flushed_lsn {};
    WalRecordWriter m_writer;
    std::string m_prefix;
    LogScratchManager *m_scratch {};
    WalCollection *m_collection {};
    Storage *m_store {};
    Size m_wal_limit {};
    SegmentGuard m_guard; // TODO: This is kinda pointless now that we're storing it as a member...
    bool m_is_running {};
};

class BasicWalWriter {
public:
    struct Parameters {
        Storage *store {};
        WalCollection *collection {};
        std::atomic<SequenceId> *flushed_lsn {};
        std::atomic<SequenceId> *pager_lsn {};
        std::shared_ptr<spdlog::logger> logger;
        std::string prefix;
        Size page_size {};
        Size wal_limit {};
    };

    explicit BasicWalWriter(const Parameters &param)
        : m_flushed_lsn {param.flushed_lsn},
          m_pager_lsn {param.pager_lsn},
          m_scratch {wal_scratch_size(param.page_size)},
          m_background {{
              param.store,
              &m_scratch,
              param.collection,
              m_flushed_lsn,
              param.logger,
              param.prefix,
              wal_block_size(param.page_size),
              param.wal_limit,
          }}
    {
        (void)m_pager_lsn;
    }

    [[nodiscard]]
    auto is_running() const -> bool
    {
        return m_background.is_running();
    }

    [[nodiscard]]
    auto status() -> Status
    {
        return m_background.status();
    }

    [[nodiscard]]
    auto last_lsn() const -> SequenceId
    {
        return m_last_lsn;
    }

    auto start() -> void;
    auto stop() -> void;
    auto flush_block() -> void;
    auto log_full_image(PageId page_id, BytesView image) -> void;
    auto log_deltas(PageId page_id, BytesView image, const std::vector<PageDelta> &deltas) -> void;
    auto log_commit() -> void;

private:
    std::atomic<SequenceId> *m_flushed_lsn {};
    std::atomic<SequenceId> *m_pager_lsn {};
    SequenceId m_last_lsn;
    LogScratchManager m_scratch;
    BackgroundWriter m_background;
};

} // namespace calico

#endif // CALICO_WAL_WAL_WRITER_H
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

namespace calico {

class LogWriter {
public:
    using SetFlushedLsn = std::function<void(SequenceId)>;

    explicit LogWriter(Size buffer_size)
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
    };

    explicit BackgroundWriter(const Parameters &param)
        : m_background {
            [this](const auto &e) {
                return on_event(e);
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
          m_store {param.store},
          m_wal_limit {param.wal_limit},
          m_guard {*param.store, m_writer, *param.collection, *param.flushed_lsn, param.prefix}
    {
        auto s = m_guard.start();
        m_is_running = s.is_ok();
        if (!m_is_running)
            handle_error(m_guard, s);
    }

    ~BackgroundWriter() = default;

    auto dispatch(Event event, bool should_wait = false) -> void
    {
        m_background.dispatch(event, should_wait);
    }

    [[nodiscard]]
    auto destroy() && -> Status
    {
        m_is_running = false;
        return std::move(m_background).destroy();
    }

    [[nodiscard]]
    auto status() const -> Status
    {
        return m_background.status();
    }

private:

    [[nodiscard]]
    auto needs_segmentation() const -> bool
    {
        return m_writer.block_count() > m_wal_limit;
    }

    auto on_event(const Event &event) -> Status
    {
        auto s = Status::ok();
        bool should_segment {};
        bool has_commit {};

        auto [
            type,
            lsn,
            buffer,
            size
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
            default:
                CALICO_EXPECT_TRUE(false && "unrecognized WAL event type");
        }

        // Replace the scratch memory so that the main thread can reuse it. This is internally synchronized.
        if (event.buffer)
            m_scratch->put(*event.buffer);

        if (s.is_ok() && should_segment) {
            s = advance_segment(m_guard, has_commit);
            if (s.is_ok()) m_flushed_lsn->store(event.lsn);
        }
        return s;
    }

    auto on_cleanup(const Status &) -> void
    {
        if (!m_guard.is_started()) // TODO
            return;

        if (m_writer.has_written()) {
            auto s = m_guard.finish(false);
            if (!s.is_ok()) handle_error(m_guard, s);
            return;
        }

        const auto id = m_guard.id();
        auto s = m_guard.abort();
        if (!s.is_ok()) handle_error(m_guard, s);

        s = m_store->remove_file(m_prefix + id.to_name());
        if (!s.is_ok()) handle_error(m_guard, s);
    }

    [[nodiscard]] auto emit_payload(SequenceId lsn, BytesView payload) -> Status;
    [[nodiscard]] auto emit_commit(SequenceId lsn) -> Status;
    [[nodiscard]] auto advance_segment(SegmentGuard &guard, bool has_commit) -> Status;
    auto handle_error(SegmentGuard &guard, Status e) -> void;

    Worker<Event> m_background;
    std::shared_ptr<spdlog::logger> m_logger;
    std::atomic<SequenceId> *m_flushed_lsn {};
    LogWriter m_writer;
    std::string m_prefix;
    LogScratchManager *m_scratch {};
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
        std::shared_ptr<spdlog::logger> logger;
        std::string prefix;
        Size page_size {};
        Size wal_limit {};
    };

    explicit BasicWalWriter(const Parameters &param)
        : m_flushed_lsn {param.flushed_lsn},
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
        m_last_lsn = m_flushed_lsn->load();
    }

    [[nodiscard]]
    auto status() const -> Status
    {
        return m_background.status();
    }

    [[nodiscard]]
    auto last_lsn() const -> SequenceId
    {
        return m_last_lsn;
    }

    auto stop() -> Status;
    auto flush_block() -> void;
    auto log_full_image(PageId page_id, BytesView image) -> void;
    auto log_deltas(PageId page_id, BytesView image, const std::vector<PageDelta> &deltas) -> void;
    auto log_commit() -> void;

private:
    std::atomic<SequenceId> *m_flushed_lsn {};
    SequenceId m_last_lsn;
    LogScratchManager m_scratch;
    BackgroundWriter m_background;
};

} // namespace calico

#endif // CALICO_WAL_WAL_WRITER_H
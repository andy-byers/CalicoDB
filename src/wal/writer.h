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

namespace calico {

/**
 * A helper class for the WAL background thread that takes care of writing records to the current segment file.
 */
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
        std::string prefix;
        Size block_size {};
        Size wal_limit {};
    };

    enum class EventType {
        STOP_WRITER,
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
        : m_flushed_lsn {param.flushed_lsn},
          m_writer {param.block_size},
          m_prefix {param.prefix},
          m_scratch {param.scratch},
          m_collection {param.collection},
          m_store {param.store},
          m_wal_limit {param.wal_limit}
    {}

    ~BackgroundWriter() = default;

    [[nodiscard]]
    auto is_running() const -> bool
    {
        return m_state.is_running;
    }

    auto dispatch(Event event) -> void
    {
        if (event.is_waiting) {
            dispatch_and_wait(event);
        } else {
            dispatch_and_return(event);
        }
    }

    auto startup() -> void
    {
        CALICO_EXPECT_FALSE(is_running());
        m_state.errors.clear();
        m_state.is_running = true;
        m_state.thread = std::thread {[this] {
            background_writer();
        }};
    }

    auto teardown() -> void
    {
        CALICO_EXPECT_TRUE(m_state.is_running);
        m_state.events.finish();
        m_state.thread->join();
        m_state.thread.reset();
        m_state.is_running = false;
        m_state.events.restart();
    }

    [[nodiscard]]
    auto next_status() -> Status
    {
        std::lock_guard lock {m_state.mu};
        if (m_state.errors.empty()) return Status::ok();
        auto s = m_state.errors.front();
        m_state.errors.pop_front();
        return s;
    }

private:

    auto dispatch_and_return(Event event) -> void
    {
        CALICO_EXPECT_FALSE(event.is_waiting);
        std::lock_guard lock {m_state.mu};
        m_state.events.enqueue(event);
    }

    auto dispatch_and_wait(Event event) -> void
    {
        CALICO_EXPECT_TRUE(event.is_waiting);
        m_is_waiting.store(true);
        m_state.events.enqueue(event);
        std::unique_lock lock {m_state.mu};
        m_state.cv.wait(lock, [this] {
            return !m_is_waiting.load();
        });
    }

    auto add_error(const Status &e) -> void
    {
        std::lock_guard lock {m_state.mu};
        m_state.errors.emplace_back(e);
    }

    [[nodiscard]]
    auto run_stop(SegmentGuard &guard) -> Status
    {
        if (m_writer.has_written())
            return guard.finish(false);

        const auto id = m_collection->current_segment().id;
        auto s = guard.abort();

        return m_store->remove_file(m_prefix + id.to_name());
    }

    [[nodiscard]]
    auto needs_segmentation() const -> bool
    {
        return m_writer.block_count() > m_wal_limit;
    }

    auto background_writer() -> void;
    [[nodiscard]] auto emit_payload(SequenceId lsn, BytesView payload) -> Status;
    [[nodiscard]] auto emit_commit(SequenceId lsn) -> Status;
    [[nodiscard]] auto advance_segment(SegmentGuard &guard, bool has_commit) -> Status;
    auto handle_error(SegmentGuard &guard, Status e) -> void;

    struct {
        Queue<Event> events {32}; ///< Internally synchronized queue.
        mutable std::mutex mu;
        std::condition_variable cv;
        std::list<Status> errors;
        std::optional<std::thread> thread;
        bool is_running {};
    } m_state;

    std::atomic<SequenceId> *m_flushed_lsn {};
    std::atomic<bool> m_is_waiting {};
    WalRecordWriter m_writer;
    std::string m_prefix;
    LogScratchManager *m_scratch {};
    WalCollection *m_collection {};
    Storage *m_store {};
    Size m_wal_limit {};
};

class BasicWalWriter {
public:
    struct Parameters {
        Storage *store {};
        WalCollection *collection {};
        std::atomic<SequenceId> *flushed_lsn {};
        std::string dirname;
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
              param.dirname,
              wal_block_size(param.page_size),
              param.wal_limit,
          }}
    {}

    [[nodiscard]]
    auto is_running() const -> bool
    {
        return m_background.is_running();
    }

    [[nodiscard]]
    auto next_status() -> Status
    {
        return m_background.next_status();
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
    SequenceId m_last_lsn;
    LogScratchManager m_scratch;
    BackgroundWriter m_background;
};

} // namespace calico

#endif // CALICO_WAL_WAL_WRITER_H
#ifndef CALICO_WAL_WAL_WRITER_H
#define CALICO_WAL_WAL_WRITER_H

#include "calico/wal.h"
#include "utils/crc.h"
#include "helpers.h"
#include <queue>
#include <memory>
#include <optional>
#include <thread>
#include <spdlog/logger.h>

namespace calico {

/**
 * A helper class for the WAL background thread that takes care of writing records to the current segment file.
 */
class WalRecordWriter {
public:
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
    auto has_written() const -> Size
    {
        return m_buffer.block_number() || m_buffer.block_offset();
    }

    auto attach(AppendWriter *file) -> void
    {
        CALICO_EXPECT_FALSE(is_attached());
        m_file.reset(file);
    }

    auto detach() -> Status
    {
        CALICO_EXPECT_TRUE(is_attached());
        auto s = Status::ok();
        if (m_buffer.block_offset())
            s = append_block();
        if (s.is_ok())
            s = m_file->sync();
        m_block_count = 0;
        m_buffer.reset();
        m_file.reset();
        return s;
    }

    template<class UpdateLsn>
    auto write(SequenceId lsn, BytesView payload, const UpdateLsn &update) -> Status
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
        return Status::ok();
    }

private:
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

    std::unique_ptr<AppendWriter> m_file;
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
    };

    enum class EventType {
        START_WRITER,
        STOP_WRITER,
        LOG_FULL_IMAGE,
        LOG_DELTAS,
        LOG_COMMIT,
    };

    struct Event {
        EventType type {};
        SequenceId lsn;
        std::optional<NamedScratch> buffer;
        Size size {};
    };

    explicit BackgroundWriter(const Parameters &param)
        : m_flushed_lsn {param.flushed_lsn},
          m_writer {param.block_size},
          m_prefix {param.prefix},
          m_scratch {param.scratch},
          m_collection {param.collection},
          m_store {param.store}
    {}

    ~BackgroundWriter() = default;

    [[nodiscard]]
    auto is_running() const -> bool
    {
        return m_state.is_running.load();
    }

    auto dispatch(Event event) -> void
    {
        std::lock_guard lock {m_state.mu};
        m_state.events.emplace_back(event);
        m_state.cv.notify_one();
    }

    auto startup() -> void
    {
        CALICO_EXPECT_FALSE(is_running());
        m_state.errors.clear();
        m_state.events.emplace_back(Event {
            EventType::START_WRITER,
            SequenceId::null(),
            std::nullopt,
        });
        m_state.thread = std::thread {
            background_writer,
            this
        };
        m_state.cv.notify_one();
    }

    auto teardown() -> void
    {
        const auto request_stop = [this] {
            std::lock_guard lock {m_state.mu};
            m_state.events.emplace_back(Event {
                EventType::STOP_WRITER,
                SequenceId::null(),
                std::nullopt,
            });
            m_state.cv.notify_one();
        };
        if (is_running()) {
            request_stop();
            m_state.thread->join();
            m_state.thread.reset();
        }
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
    [[nodiscard]]
    auto run_start(SegmentGuard &guard) -> Status
    {
        return guard.start(m_writer, *m_collection);
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
        return m_writer.block_count() > 128; // TODO: Make this tunable?
    }

    static auto background_writer(BackgroundWriter *writer) -> void*;
    [[nodiscard]] auto emit_payload(SequenceId lsn, BytesView payload) -> Status;
    [[nodiscard]] auto emit_commit(SegmentGuard &guard, SequenceId lsn) -> Status;
    [[nodiscard]] auto advance_segment(SegmentGuard &guard, bool has_commit) -> Status;
    auto handle_error(SegmentGuard &guard, Status e) -> void*;

    struct {
        mutable std::mutex mu;
        std::condition_variable cv;
        std::deque<Event> events;
        std::list<Status> errors;
        std::atomic<bool> is_running {};
        std::optional<std::thread> thread;
    } m_state;

    std::atomic<SequenceId> *m_flushed_lsn {};
    WalRecordWriter m_writer;
    std::string m_prefix;
    LogScratchManager *m_scratch {};
    WalCollection *m_collection {};
    Storage *m_store {};
};

class BasicWalWriter {
public:
    struct Parameters {
        Storage *store {};
        WalCollection *collection {};
        std::atomic<SequenceId> *flushed_lsn {};
        std::string dirname;
        Size page_size {};
    };

    explicit BasicWalWriter(const Parameters &param)
        : m_scratch {param.page_size * WAL_SCRATCH_SCALE},
          m_background {{
              param.store,
              &m_scratch,
              param.collection,
              param.flushed_lsn,
              param.dirname,
              param.page_size * WAL_BLOCK_SCALE,
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

    auto start() -> void;
    auto stop() -> void;
    auto log_full_image(SequenceId lsn, PageId page_id, BytesView image) -> void;
    auto log_deltas(SequenceId lsn, PageId page_id, BytesView image, const std::vector<PageDelta> &deltas) -> void;
    auto log_commit(SequenceId lsn) -> void;

private:
    LogScratchManager m_scratch;
    BackgroundWriter m_background;
};

} // namespace calico

#endif // CALICO_WAL_WAL_WRITER_H
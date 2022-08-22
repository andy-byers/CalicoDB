#include "writer.h"
#include "utils/logging.h"
#include "utils/types.h"
#include <optional>

namespace calico {

auto BackgroundWriter::background_writer(BackgroundWriter *writer) -> void*
{
    using namespace std::chrono_literals;

    auto &[mu, cv, events, self] = writer->m_state;
    auto &flushed_lsn = *writer->m_flushed_lsn;
    auto &scratch = *writer->m_scratch;
    auto is_running = true;

    SegmentGuard current {*writer->m_store, writer->m_prefix};

    while (is_running) {
        std::unique_lock lock {mu};
        cv.wait(lock, [writer] {
            return !writer->m_state.events.empty();
        });

        auto [
            type,
            lsn,
            buffer,
            size
        ] = events.front();
        events.pop_front();

        lock.unlock();

        auto s = Status::ok();
        switch (type) {
            case EventType::LOG_FULL_IMAGE:
            case EventType::LOG_DELTAS:
                CALICO_EXPECT_TRUE(buffer.has_value());
                s = writer->emit_payload(lsn, buffer->data().truncate(size));
                break;
            case EventType::LOG_COMMIT:
                s = writer->emit_commit(current, lsn);
                break;
            case EventType::START_WRITER:
                s = writer->run_start(current);
                break;
            case EventType::STOP_WRITER:
                s = writer->run_stop(current);
                is_running = false;
                break;
            default:
                CALICO_EXPECT_TRUE(false && "unrecognized WAL event type");
        }

        // Replace the scratch memory so that the main thread can reuse it. This is internally synchronized.
        if (buffer) scratch.put(*buffer);

        // Shouldn't need segmentation on this round unless we wrote something.
        if (s.is_ok() && writer->needs_segmentation()) {
            s = writer->advance_segment(current, false);

            if (s.is_ok())
                flushed_lsn.store(lsn);
        }

        // TODO: Clean up unneeded segments. We'll use the m_first_lsn member from BasicWriteAheadLog...

        if (!s.is_ok())
            return writer->handle_error(current, s);
    }
    return nullptr;
}

auto BackgroundWriter::handle_error(SegmentGuard &guard, Status s) -> void*
{
    CALICO_EXPECT_FALSE(s.is_ok());
    std::lock_guard lock {m_state.mu};
    m_status = std::move(s);
    m_state.events.clear();
    m_state.thread->detach();
    m_state.thread.reset();

    const auto id = m_collection->current_segment().id;
    if (guard.is_started()) (void)guard.abort();
    (void)m_store->remove_file(m_prefix + id.to_name());
    return nullptr;
}

auto BackgroundWriter::emit_payload(SequenceId lsn, BytesView payload) -> Status
{
    return m_writer.write(lsn, payload, [this](auto flushed_lsn) {
        m_flushed_lsn->store(flushed_lsn);
    });
}

auto BackgroundWriter::emit_commit(SegmentGuard &guard, SequenceId lsn) -> Status
{
    std::string scratch(1, '\x00');
    const auto payload_size = encode_commit_payload(stob(scratch));
    CALICO_EXPECT_EQ(payload_size, 1);

    auto s = emit_payload(lsn, stob(scratch));
    if (s.is_ok()) {
        s = advance_segment(guard, true);
        if (s.is_ok()) m_flushed_lsn->store(lsn);
    }
    return s;
}

auto BackgroundWriter::advance_segment(SegmentGuard &guard, bool has_commit) -> Status // TODO: Weird semantics...
{
    if (guard.is_started()) {
        auto s = guard.finish(has_commit);
        if (!s.is_ok()) return s;
    }
    return run_start(guard);
}

auto BasicWalWriter::start() -> Status
{
    return m_writer.startup();
}

auto BasicWalWriter::stop() -> Status
{
    return m_writer.teardown();
}

auto BasicWalWriter::log_full_image(SequenceId lsn, PageId page_id, BytesView image) -> void
{
    auto buffer = m_scratch.get();
    const auto size = encode_full_image_payload(page_id, image, buffer.data());

    m_writer.dispatch({
        BackgroundWriter::EventType::LOG_FULL_IMAGE,
        lsn,
        buffer,
        size,
    });
}

auto BasicWalWriter::log_deltas(SequenceId lsn, PageId page_id, BytesView image, const std::vector<PageDelta> &deltas) -> void
{
    auto buffer = m_scratch.get();
    const auto size = encode_deltas_payload(page_id, image, deltas, buffer.data());

    m_writer.dispatch({
        BackgroundWriter::EventType::LOG_DELTAS,
        lsn,
        buffer,
        size,
    });
}

auto BasicWalWriter::log_commit(SequenceId lsn) -> void
{
    m_writer.dispatch({
        BackgroundWriter::EventType::LOG_COMMIT,
        lsn,
        std::nullopt,
        0,
    });
}

} // namespace calico
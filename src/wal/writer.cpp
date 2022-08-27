#include "writer.h"
#include "utils/logging.h"
#include "utils/types.h"
#include <optional>

namespace calico {

auto BackgroundWriter::background_writer() -> void
{
    SegmentGuard guard {*m_store, m_prefix};

    auto s = guard.start(m_writer, *m_collection, *m_flushed_lsn);
    if (!s.is_ok()) {
        handle_error(guard, s);
        return;
    }

    for (; ; ) {
        auto event = m_state.events.dequeue();
        if (!event.has_value()) break;

        auto [
            type,
            lsn,
            buffer,
            size,
            is_waiting
        ] = *event;

        bool should_segment {};
        bool has_commit {};

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
                s = run_stop(guard);
                break;
            default:
                CALICO_EXPECT_TRUE(false && "unrecognized WAL event type");
        }

        // Replace the scratch memory so that the main thread can reuse it. This is internally synchronized.
        if (buffer) m_scratch->put(*buffer);

        if (s.is_ok() && should_segment) {
            s = advance_segment(guard, has_commit);
            if (s.is_ok()) m_flushed_lsn->store(lsn);
        }

        if (!s.is_ok())
            handle_error(guard, s);

        if (is_waiting) {
            m_is_waiting.store(false);
            m_state.cv.notify_one();
        }

        // TODO: Clean up obsolete segments...

    }
}

auto BackgroundWriter::handle_error(SegmentGuard &guard, Status e) -> void
{
    CALICO_EXPECT_FALSE(e.is_ok());
    add_error(e);

    // We still want to try and finish the segment. We may need it to roll back changes.
    e = run_stop(guard);
    if (!e.is_ok())
        add_error(e);
}

auto BackgroundWriter::emit_payload(SequenceId lsn, BytesView payload) -> Status
{
    return m_writer.write(lsn, payload, [this](auto flushed_lsn) {
        m_flushed_lsn->store(flushed_lsn);
    });
}

auto BackgroundWriter::emit_commit(SequenceId lsn) -> Status
{
    static constexpr char payload[] {WalPayloadType::COMMIT, '\x00'};
    return emit_payload(lsn, stob(payload));
}

auto BackgroundWriter::advance_segment(SegmentGuard &guard, bool has_commit) -> Status
{
    if (guard.is_started()) {
        auto s = guard.finish(has_commit);
        if (!s.is_ok()) return s;
    }
    return guard.start(m_writer, *m_collection, *m_flushed_lsn);
}

auto BasicWalWriter::start() -> void
{
    m_last_lsn = m_flushed_lsn->load();
    m_background.startup();
}

auto BasicWalWriter::stop() -> void
{
    m_background.dispatch(BackgroundWriter::Event {
        BackgroundWriter::EventType::STOP_WRITER,
        m_last_lsn,
        std::nullopt,
        0,
        true,
    });
    m_background.teardown();
}

auto BasicWalWriter::flush_block() -> void
{
    m_background.dispatch(BackgroundWriter::Event {
        BackgroundWriter::EventType::FLUSH_BLOCK,
        m_last_lsn,
        std::nullopt,
        0,
    });
}

auto BasicWalWriter::log_full_image(PageId page_id, BytesView image) -> void
{
    auto buffer = m_scratch.get();
    const auto size = encode_full_image_payload(page_id, image, *buffer);

    m_background.dispatch({
        BackgroundWriter::EventType::LOG_FULL_IMAGE,
        ++m_last_lsn,
        buffer,
        size,
    });
}

auto BasicWalWriter::log_deltas(PageId page_id, BytesView image, const std::vector<PageDelta> &deltas) -> void
{
    auto buffer = m_scratch.get();
    const auto size = encode_deltas_payload(page_id, image, deltas, *buffer);

    m_background.dispatch({
        BackgroundWriter::EventType::LOG_DELTAS,
        ++m_last_lsn,
        buffer,
        size,
    });
}

auto BasicWalWriter::log_commit() -> void
{
    m_background.dispatch({
        BackgroundWriter::EventType::LOG_COMMIT,
        ++m_last_lsn,
        std::nullopt,
        0,
        true,
    });
}

} // namespace calico
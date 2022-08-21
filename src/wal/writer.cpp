#include <chrono>
#include "writer.h"
#include "basic_wal.h"
#include "calico/store.h"
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

    while (is_running) {
        std::unique_lock lock {mu};
        cv.wait(lock, [writer] { // TODO: Wake up periodically using wait_for() and check some flag to determine if we should exit instead of having a "STOP" event?
            return !writer->m_state.events.empty();
        });
//        cv.wait_for(lock, 500us);
//        if (events.empty()) continue;
        auto [type, lsn, buffer, size] = events.front();
        events.pop_front();
        lock.unlock();

        auto s = Status::ok();
        switch (type) {
            case EventType::LOG_DELTAS:
                CALICO_EXPECT_TRUE(buffer.has_value());
                s = writer->emit_payload(lsn, buffer->data().truncate(size));
                break;
            case EventType::LOG_FULL_IMAGE:
                CALICO_EXPECT_TRUE(buffer.has_value());
                s = writer->emit_payload(lsn, buffer->data().truncate(size));
                break;
            case EventType::LOG_COMMIT:
                CALICO_EXPECT_FALSE(buffer.has_value());
                s = writer->emit_commit(lsn);
                break;
            case EventType::PAUSE_WRITER:
                CALICO_EXPECT_FALSE(buffer.has_value());
                s = writer->advance_segment(false);
                // Main thread should be waiting in standby().
                cv.notify_one();
                break;
            case EventType::STOP_WRITER:
                CALICO_EXPECT_FALSE(buffer.has_value());
                s = writer->try_close_segment(lsn);
                is_running = false;
                break;

            default:
                CALICO_EXPECT_TRUE(false && "unrecognized WAL event type");
        }

        // Replace the scratch memory so that the main thread can reuse it. This is internally synchronized.
        if (buffer) scratch.put(*buffer);

        // Shouldn't need segmentation on this round unless we wrote something.
        if (s.is_ok() && writer->needs_segmentation()) {
            s = writer->advance_segment(false);

            if (s.is_ok())
                flushed_lsn.store(lsn);
        }

        // TODO: Clean up unneeded segments. We'll use the m_first_lsn member from BasicWriteAheadLog...

        if (!s.is_ok())
            return writer->handle_error(s);
    }
    return nullptr;
}

auto BackgroundWriter::handle_error(Status s) -> void*
{
    CALICO_EXPECT_FALSE(s.is_ok());
    std::lock_guard guard {m_state.mu};
    m_status = std::move(s);
    m_state.events.clear();
    m_state.thread->detach();
    m_state.thread.reset();
    (void)m_writer.detach(); // TODO: Store multiple status objects? Or just log additional errors?
    return nullptr;
}

auto BackgroundWriter::emit_payload(SequenceId lsn, BytesView payload) -> Status
{
    const auto is_first = !m_writer.is_attached();
    auto s = Status::ok();

    if (is_first) {
        s = open_on_segment();
        if (!s.is_ok()) return s;
    }

    s = m_writer.write(lsn, payload, [this](auto flushed_lsn) {
        m_flushed_lsn->store(flushed_lsn);
    });

    if (!s.is_ok())
        m_collection->abort_segment();

    return s;
}

auto BackgroundWriter::emit_commit(SequenceId lsn) -> Status
{
    std::string scratch(1, '\x00');
    const auto payload_size = encode_commit_payload(stob(scratch));
    CALICO_EXPECT_EQ(payload_size, 1);

    auto s = emit_payload(lsn, stob(scratch));
    if (s.is_ok()) {
        s = advance_segment(true);
        if (s.is_ok()) m_flushed_lsn->store(lsn);
    }
    return s;
}

auto BackgroundWriter::advance_segment(bool has_commit) -> Status // TODO: Weird semantics...
{
    if (m_writer.is_attached()) {
        CALICO_EXPECT_TRUE(m_collection->is_segment_started());
        m_current_id.value++;
        m_collection->finish_segment(has_commit);

        auto s = m_writer.detach();
        if (!s.is_ok()) return s;

        return open_on_segment();
    }
    return Status::ok();
}

auto BasicWalWriter::start() -> Status
{
    return m_writer.startup();
}

auto BasicWalWriter::pause() -> Status
{
    return m_writer.standby();
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

//
//    // Clean up obsolete segments.
//    const auto pager_lsn = m_pager_lsn->load();
//    for (; ; ) {
//        const auto front = m_segments->front();
//        const auto is_flushed = pager_lsn >= front.first_lsn;
//        const auto is_previous = front.id < m_positions->front().id;
//        if (!is_flushed || !is_previous)
//            break;
//
//        s = m_store->remove_file(prefix + m_segments->front().id.to_name());
//        if (!s.is_ok()) return s;
//        m_segments->erase(cbegin(*m_segments));
//    }

} // namespace calico
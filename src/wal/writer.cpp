#include "writer.h"
#include "utils/logging.h"
#include "utils/types.h"
#include <optional>

namespace calico {

auto BackgroundWriter::handle_error(SegmentGuard &guard, Status e) -> void
{
    CALICO_EXPECT_FALSE(e.is_ok());
    m_logger->error(e.what());

    if (guard.is_started()) {
        e = guard.finish(false);
        if (!e.is_ok()) {
            m_logger->error("(1/2) cannot complete segment after error");
            m_logger->error("(2/2) {}", e.what());
        }
    }
}

auto BackgroundWriter::emit_payload(SequenceId lsn, BytesView payload) -> Status
{
if(payload.size()==521)fmt::print(stderr,"app LSN:{}\n", lsn.value);

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
    if (!guard.id().is_null()) {
        auto s = guard.finish(has_commit);
        if (!s.is_ok()) return s;
    }
    return guard.start();
}

auto BasicWalWriter::stop() -> Status
{
    return std::move(m_background).destroy();
}

auto BasicWalWriter::flush_block() -> void
{
    m_background.dispatch(BackgroundWriter::Event {
        BackgroundWriter::EventType::FLUSH_BLOCK,
        m_last_lsn,
        std::nullopt,
        0,
    }, true);
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
    }, true);
}

} // namespace calico
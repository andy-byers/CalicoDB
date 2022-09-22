#include "recovery.h"
#include "page/page.h"
#include "pager/pager.h"
#include "utils/info_log.h"
#include "wal/wal.h"

namespace calico {

auto encode_deltas_payload(PageId page_id, BytesView image, const std::vector<PageDelta> &deltas, Bytes out) -> Size
{
    const auto original_size = out.size();

    // Payload type (1 B)
    out[0] = static_cast<Byte>(XactPayloadType::DELTAS);
    out.advance();

    // Page ID (8 B)
    put_u64(out, page_id.value);
    out.advance(sizeof(page_id));

    // Deltas count (2 B)
    put_u16(out, static_cast<std::uint16_t>(deltas.size()));
    out.advance(sizeof(std::uint16_t));

    // Deltas (N B)
    for (const auto &[offset, size]: deltas) {
        put_u16(out, static_cast<std::uint16_t>(offset));
        out.advance(sizeof(std::uint16_t));

        put_u16(out, static_cast<std::uint16_t>(size));
        out.advance(sizeof(std::uint16_t));

        mem_copy(out, image.range(offset, size));
        out.advance(size);
    }
    return original_size - out.size();
}

auto encode_commit_payload(Bytes out) -> Size
{
    // Payload type (1 B)
    out[0] = static_cast<Byte>(XactPayloadType::COMMIT);
    out.advance();

    return MINIMUM_PAYLOAD_SIZE;
}

auto encode_full_image_payload(PageId page_id, BytesView image, Bytes out) -> Size
{
    const auto original_size = out.size();

    // Payload type (1 B)
    out[0] = static_cast<Byte>(XactPayloadType::FULL_IMAGE);
    out.advance();

    // Page ID (8 B)
    put_u64(out, page_id.value);
    out.advance(sizeof(page_id));

    // Image (N B)
    mem_copy(out, image);
    out.advance(image.size());

    return original_size - out.size();
}

static auto decode_deltas_payload(WalPayloadOut in) -> DeltasDescriptor
{
    DeltasDescriptor info;
    auto data = in.data();
    info.lsn = in.lsn();

    // Payload type (1 B)
    CALICO_EXPECT_EQ(XactPayloadType {data[0]}, XactPayloadType::DELTAS);
    data.advance();

    // Page ID (8 B)
    info.pid.value = get_u64(data);
    data.advance(sizeof(info.pid));

    // Deltas count (2 B)
    info.deltas.resize(get_u16(data));
    data.advance(sizeof(std::uint16_t));

    // Deltas (N B)
    std::generate(begin(info.deltas), end(info.deltas), [&data] {
        DeltasDescriptor::Delta delta;
        delta.offset = get_u16(data);
        data.advance(sizeof(std::uint16_t));

        const auto size = get_u16(data);
        data.advance(sizeof(std::uint16_t));

        delta.data = data.range(0, size);
        data.advance(size);
        return delta;
    });
    return info;
}

static auto decode_full_image_payload(WalPayloadOut in) -> FullImageDescriptor
{
    FullImageDescriptor info;
    auto data = in.data();
    info.lsn = in.lsn();

    // Payload type (1 B)
    CALICO_EXPECT_EQ(XactPayloadType {data[0]}, XactPayloadType::FULL_IMAGE);
    data.advance();

    // Page ID (8 B)
    info.pid.value = get_u64(data);
    data.advance(sizeof(PageId));

    // Image (n B)
    info.image = data;
    return info;
}

static auto decode_commit_payload(WalPayloadOut in) -> CommitDescriptor
{
    CommitDescriptor info;
    auto data = in.data();
    info.lsn = in.lsn();

    // Payload type (1 B)
    CALICO_EXPECT_EQ(XactPayloadType {data[0]}, XactPayloadType::COMMIT);

    return info;
}

auto decode_payload(WalPayloadOut in) -> std::optional<PayloadDescriptor>
{
    switch (XactPayloadType {in.data()[0]}) {
        case XactPayloadType::DELTAS:
            return decode_deltas_payload(in);
        case XactPayloadType::FULL_IMAGE:
            return decode_full_image_payload(in);
        case XactPayloadType::COMMIT:
            return decode_commit_payload(in);
        default:
            return std::nullopt;
    }
}

auto Recovery::start_abort(SequenceId commit_lsn) -> Status
{
    static constexpr auto MSG = "could not abort";

    if (!m_wal->is_enabled()) {
        ThreePartMessage message;
        message.set_primary(MSG);
        message.set_detail("WAL is not enabled");
        message.set_hint(R"("wal_limit" was set to "DISABLE_WAL" on database creation)");
        return message.logic_error();
    }

    auto s = Status::ok();
    if (m_wal->is_working())
        (void)m_wal->stop_workers();

    // This should give us the full images of each updated page belonging to the current transaction,
    // before any changes were made to it.
    return m_wal->roll_backward(commit_lsn, [this](auto payload) {
        auto info = decode_payload(payload);

        if (!info.has_value())
            return Status::corruption("WAL is corrupted");

        if (std::holds_alternative<FullImageDescriptor>(*info)) {
            const auto image = std::get<FullImageDescriptor>(*info);
            auto page = m_pager->acquire(image.pid, true);
            if (!page.has_value()) return page.error();
            page->apply_update(image);
            return m_pager->release(std::move(*page));
        }
        return Status::ok();
    });
}

auto Recovery::finish_abort(SequenceId commit_lsn) -> Status
{
    return finish_routine(commit_lsn);
}

auto Recovery::start_recovery(SequenceId &commit_lsn) -> Status
{
    SequenceId last_lsn;

    const auto redo = [this, &last_lsn, &commit_lsn](auto payload) {
        auto info = decode_payload(payload);

        // Payload has an invalid type.
        if (!info.has_value())
            return Status::corruption("WAL is corrupted");

        last_lsn = payload.lsn();

        if (std::holds_alternative<DeltasDescriptor>(*info)) {
            const auto deltas = std::get<DeltasDescriptor>(*info);
            auto page = m_pager->acquire(deltas.pid, true);
            if (!page.has_value()) return page.error();
            page->apply_update(deltas);
            return m_pager->release(std::move(*page));
        } else if (std::holds_alternative<CommitDescriptor>(*info)) {
            commit_lsn = payload.lsn();
        }
        return Status::ok();
    };

    const auto undo = [this](auto payload) {
        auto info = decode_payload(payload);

        if (!info.has_value())
            return Status::corruption("WAL is corrupted");

        if (std::holds_alternative<FullImageDescriptor>(*info)) {
            const auto image = std::get<FullImageDescriptor>(*info);
            auto page = m_pager->acquire(image.pid, true);
            if (!page.has_value()) return page.error();
            page->apply_update(image);
            return m_pager->release(std::move(*page));
        }
        return Status::ok();
    };

    CALICO_TRY(m_wal->roll_forward(m_pager->flushed_lsn(), redo));

    // Reached the end of the WAL, but didn't find a commit record.
    if (last_lsn != commit_lsn)
        return m_wal->roll_backward(commit_lsn, undo);
    return Status::ok();
}

auto Recovery::finish_recovery(SequenceId commit_lsn) -> Status
{
    return finish_routine(commit_lsn);
}

auto Recovery::finish_routine(SequenceId commit_lsn) -> Status
{
    // Flush all dirty database pages.
    CALICO_TRY(m_pager->flush());

    // Remove obsolete segment files.
    CALICO_TRY(m_wal->remove_after(commit_lsn));

    return m_wal->start_workers();
}

} // namespace calico
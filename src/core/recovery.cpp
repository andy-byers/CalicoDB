#include "recovery.h"
#include "page/page.h"
#include "pager/pager.h"
#include "wal/wal.h"

namespace Calico {

static auto encode_payload_type(Bytes out, XactPayloadType type)
{
    CALICO_EXPECT_FALSE(out.is_empty());
    out[0] = type;
}

auto encode_deltas_payload(Id page_id, Slice image, const std::vector<PageDelta> &deltas, Bytes out) -> Size
{
    const auto original_size = out.size();

    // Payload type (1 B)
    encode_payload_type(out, XactPayloadType::DELTAS);
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
    encode_payload_type(out, XactPayloadType::COMMIT);
    out.advance();

    return MINIMUM_PAYLOAD_SIZE;
}

auto encode_full_image_payload(Id page_id, Slice image, Bytes out) -> Size
{
    const auto original_size = out.size();

    // Payload type (1 B)
    encode_payload_type(out, XactPayloadType::FULL_IMAGE);
    out.advance();

    // Page ID (8 B)
    put_u64(out, page_id.value);
    out.advance(sizeof(page_id));

    // Image (N B)
    mem_copy(out, image);
    out.advance(image.size());

    return original_size - out.size();
}

static auto decode_deltas_payload(WalPayloadOut in) -> DeltaDescriptor
{
    DeltaDescriptor info;
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
        DeltaDescriptor::Delta delta;
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
    data.advance(sizeof(Id));

    // Image (n B)
    info.image = data;
    return info;
}

static auto decode_commit_payload(WalPayloadOut in) -> CommitDescriptor
{
    CommitDescriptor info;
    info.lsn = in.lsn();
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

#define ENSURE_ENABLED(primary) \
    do { \
        if (!m_wal->is_enabled()) \
            return logic_error( \
                "{}: WAL is not enabled (wal_limit was set to DISABLE_WAL on database creation)", primary); \
    } while (0)

auto Recovery::start_abort() -> Status
{
    // m_log->trace("start_abort");
    ENSURE_ENABLED("cannot start abort");

    // This should give us the full images of each updated page belonging to the current transaction,
    // before any changes were made to it.
    return m_wal->roll_backward(m_system->commit_lsn.load(), [this](auto payload) {
        auto info = decode_payload(payload);

        if (!info.has_value())
            return corruption("WAL is corrupted");

        if (std::holds_alternative<FullImageDescriptor>(*info)) {
            const auto image = std::get<FullImageDescriptor>(*info);
            auto page = m_pager->acquire(image.pid, true);
            if (!page.has_value()) return page.error();
            page->apply_update(image);
            return m_pager->release(std::move(*page));
        }
        return ok();
    });
}

auto Recovery::finish_abort() -> Status
{
    // m_log->trace("finish_abort");
    ENSURE_ENABLED("cannot finish abort");
//    return finish_routine();

    CALICO_TRY_S(m_pager->flush({}));

    return m_wal->remove_after(m_system->commit_lsn);
}

auto Recovery::start_recovery() -> Status
{
    // m_log->trace("start_recovery");
    ENSURE_ENABLED("cannot start recovery");
    Id last_lsn;

    const auto redo = [this, &last_lsn](auto payload) {
        auto info = decode_payload(payload);

        // Payload has an invalid type.
        if (!info.has_value())
            return corruption("WAL is corrupted");

        last_lsn = payload.lsn();

        if (std::holds_alternative<CommitDescriptor>(*info)) {
            m_system->commit_lsn.store(payload.lsn());
        } else if (std::holds_alternative<DeltaDescriptor>(*info)) {
            const auto delta = std::get<DeltaDescriptor>(*info);
            auto page = m_pager->acquire(delta.pid, true);
            if (!page.has_value())
                return page.error();
            if (delta.lsn > page->lsn())
                page->apply_update(delta);
            return m_pager->release(std::move(*page));
        } else if (std::holds_alternative<FullImageDescriptor>(*info)) {
            // This is not necessary in most cases, but should help with some kinds of corruption.
            const auto image = std::get<FullImageDescriptor>(*info);
            auto page = m_pager->acquire(image.pid, true);
            if (!page.has_value())
                return page.error();
            if (image.lsn > page->lsn())
                page->apply_update(image);
            return m_pager->release(std::move(*page));
        } else {
            return corruption("unrecognized payload type");
        }
        return ok();
    };

    const auto undo = [this](auto payload) {
        auto info = decode_payload(payload);

        if (!info.has_value())
            return corruption("WAL is corrupted");

        if (std::holds_alternative<FullImageDescriptor>(*info)) {
            const auto image = std::get<FullImageDescriptor>(*info);
            auto page = m_pager->acquire(image.pid, true);
            if (!page.has_value()) return page.error();
            page->apply_update(image);
            return m_pager->release(std::move(*page));
        }
        return ok();
    };


    // Apply updates that are in the WAL but not the database.
    auto s = m_wal->roll_forward(m_pager->recovery_lsn(), redo);

    // m_log->info("{} -> {}", m_pager->recovery_lsn().value, last_lsn.value);
    CALICO_TRY_S(s);

    // Reached the end of the WAL, but didn't find a commit record. Undo updates until we reach the most-recent commit.
    const auto commit_lsn = m_system->commit_lsn.load();
    if (last_lsn != commit_lsn)
        return m_wal->roll_backward(commit_lsn, undo);
    return ok();
}

auto Recovery::finish_recovery() -> Status
{
    // m_log->trace("finish_recovery");
    ENSURE_ENABLED("cannot finish recovery");

    CALICO_TRY_S(m_pager->flush({}));
    m_wal->remove_before(m_pager->recovery_lsn());
    return ok(); // TODO
}

} // namespace Calico
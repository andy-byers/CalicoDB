#include "writer.h"
#include "utils/logging.h"
#include "utils/types.h"
#include <optional>

namespace calico {

auto LogWriter::write(SequenceId lsn, BytesView payload) -> Status
{
    CALICO_EXPECT_FALSE(lsn.is_null());
    WalRecordHeader lhs {};
    lhs.type = WalRecordHeader::Type::FULL;
    lhs.size = static_cast<std::uint16_t>(payload.size());
    lhs.crc = crc_32(payload);

    while (!payload.is_empty()) {
        auto rest = m_tail;
        // Note that this modifies rest to point to [<m_offset>, <end>) in the tail buffer.
        const auto space_remaining = rest.advance(m_offset).size();
        const auto can_fit_some = space_remaining > sizeof(WalRecordHeader);
        const auto can_fit_all = space_remaining >= sizeof(WalRecordHeader) + payload.size();

        if (can_fit_some) {
            WalRecordHeader rhs;

            if (!can_fit_all)
                rhs = split_record(lhs, payload, space_remaining);

            // We must have room for the whole header and at least 1 payload byte.
            write_wal_record_header(rest, lhs);
            rest.advance(sizeof(lhs));
            mem_copy(rest, payload.range(0, lhs.size));

            m_offset += sizeof(lhs) + lhs.size;
            payload.advance(lhs.size);
            rest.advance(lhs.size);

            if (!can_fit_all)
                lhs = rhs;

            // The new value of m_offset must be less than or equal to the start of the next block. If it is exactly
            // at the start of the next block, we should fall through and read it into the tail buffer.
            if (m_offset != m_tail.size()) continue;
        }
        CALICO_EXPECT_LE(m_tail.size() - m_offset, sizeof(lhs));
        auto s = flush();
        if (!s.is_ok()) return s;
    }
    // Record is fully in the tail buffer and maybe partially on disk. Next time we flush, this record is guaranteed
    // to be all the way on disk.
    m_last_lsn = lsn;
    return Status::ok();
}

auto LogWriter::flush() -> Status
{
    fmt::print(stderr, "flush block {}\n", block_count());


    // Already flushed, just return OK.
    if (m_offset == 0)
        return Status::ok();

    // Clear unused bytes at the end of the tail buffer.
    mem_clear(m_tail.range(m_offset));

    auto s = m_file->write(m_tail);
    if (s.is_ok()) {
        m_flushed_lsn->store(m_last_lsn);
        m_offset = 0;
        m_number++;
    }
    return s;
}

auto WalWriter::open() -> Status
{
    return open_segment(++m_segments->last());
}

auto WalWriter::write(SequenceId lsn, NamedScratch payload) -> void
{
    m_worker.dispatch(Event {lsn, payload}, false);
}

auto WalWriter::advance() -> void
{
    m_worker.dispatch(std::nullopt, true);
}

auto WalWriter::destroy() && -> Status
{
    auto s = std::move(m_worker).destroy();
    close_segment();
    return s;
}

auto WalWriter::on_event(const EventWrapper &event) -> Status
{
    // std::nullopt means we need to advance to the next segment.
    if (!event) return advance_segment();

    auto [lsn, buffer] = *event;
    auto s = m_writer->write(lsn, *buffer);

    m_scratch->put(buffer);
    if (s.is_ok() && m_writer->block_count() >= m_wal_limit)
        return advance_segment();

    // If we failed to write the record, we need to close the segment file. We
    // should do it in this thread to avoid race conditions.
    if (!s.is_ok()) close_segment();
    return s;
}

auto WalWriter::open_segment(SegmentId id) -> Status
{
    CALICO_EXPECT_EQ(m_writer, std::nullopt);
    AppendWriter *file {};
    auto s = m_store->open_append_writer(m_prefix + id.to_name(), &file);
    if (s.is_ok()) {
        m_file.reset(file);
        m_writer = LogWriter {*m_file, m_tail, *m_flushed_lsn};
    }
    return s;
}

auto WalWriter::close_segment() -> Status
{
    // We must have failed while opening the segment file.
    if (!m_writer) return status();

    const auto is_empty = m_writer->block_count() == 0;

    // This is a NOOP if the tail buffer was empty.
    auto s = m_writer->flush();
    m_writer.reset();
    m_file.reset();

    // We want to do this part, even if the flush failed. If the segment is empty, and we fail to remove
    // it, we will end up overwriting it next time we open the writer.
    if (auto id = ++m_segments->last(); is_empty) {
        auto t = m_store->remove_file(m_prefix + id.to_name());
        s = s.is_ok() ? t : s;
    } else {
        m_segments->add_segment(id);
    }
    return s;
}

auto WalWriter::advance_segment() -> Status
{
    auto s = close_segment();
    if (s.is_ok()) {
        auto id = ++m_segments->last();
        return open_segment(id);
    }
    return s;
}

} // namespace calico
#include "writer.h"
#include "utils/types.h"
#include <optional>

namespace Calico {

auto LogWriter::write(WalPayloadIn payload) -> Status
{
    const auto lsn = payload.lsn();
    auto data = payload.raw();

    CALICO_EXPECT_FALSE(lsn.is_null());
    WalRecordHeader lhs {};
    lhs.type = WalRecordHeader::Type::FULL;
    lhs.size = static_cast<std::uint16_t>(data.size());
    lhs.crc = crc_32(data);

    while (!data.is_empty()) {
        auto rest = m_tail;
        // Note that this modifies rest to point to [<m_offset>, <end>) in the tail buffer.
        const auto space_remaining = rest.advance(m_offset).size();
        const auto can_fit_some = space_remaining > WalRecordHeader::SIZE;
        const auto can_fit_all = space_remaining >= WalRecordHeader::SIZE + data.size();

        if (can_fit_some) {
            WalRecordHeader rhs;

            if (!can_fit_all)
                rhs = split_record(lhs, data, space_remaining);

            // We must have room for the whole header and at least 1 payload byte.
            write_wal_record_header(rest, lhs);
            rest.advance(WalRecordHeader::SIZE);
            mem_copy(rest, data.range(0, lhs.size));

            m_offset += WalRecordHeader::SIZE + lhs.size;
            data.advance(lhs.size);
            rest.advance(lhs.size);

            if (!can_fit_all)
                lhs = rhs;

            // The new value of m_offset must be less than or equal to the start of the next block. If it is exactly
            // at the start of the next block, we should fall through and read it into the tail buffer.
            if (m_offset != m_tail.size()) continue;
        }
        CALICO_EXPECT_LE(m_tail.size() - m_offset, WalRecordHeader::SIZE);
        auto s = flush();
        if (!s.is_ok()) return s;
    }
    // Record is fully in the tail buffer and maybe partially on disk. Next time we flush, this record is guaranteed
    // to be all the way on disk.
    m_last_lsn = lsn;
    return ok();
}

auto LogWriter::flush() -> Status
{
    // Already flushed.
    if (m_offset == 0)
        return logic_error("could not flush: nothing to flush");

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

WalWriter::WalWriter(const Parameters &param)
    : m_prefix {param.prefix.to_string()},
      m_flushed_lsn {param.flushed_lsn},
      m_storage {param.storage},
      m_system {param.system},
      m_set {param.set},
      m_tail {param.tail},
      m_wal_limit {param.wal_limit}
{
    CALICO_EXPECT_FALSE(m_prefix.empty());
    CALICO_EXPECT_NE(m_flushed_lsn, nullptr);
    CALICO_EXPECT_NE(m_storage, nullptr);
    CALICO_EXPECT_NE(m_system, nullptr);
    CALICO_EXPECT_NE(m_set, nullptr);

    // First segment file gets created now, but is not registered in the WAL set until the writer
    // is finished with it.
    CALICO_ERROR_IF(open_segment(++m_set->last()));
}

auto WalWriter::write(WalPayloadIn payload) -> void
{
    if (m_system->has_error())
        return;

    CALICO_ERROR_IF(m_writer->write(payload));
    if (m_writer->block_count() >= m_wal_limit)
        CALICO_ERROR_IF(advance_segment());
}

auto WalWriter::flush() -> void
{
    auto s = m_writer->flush();
    // Throw away logic errors due to the tail buffer being empty.
    CALICO_ERROR_IF(s.is_logic_error() ? ok() : s);
}

auto WalWriter::advance() -> void
{
    // NOTE: advance() is a NOOP if the current WAL segment hasn't been written to.
    CALICO_ERROR_IF(advance_segment());
}

auto WalWriter::destroy() && -> Status
{
    return close_segment();
}

auto WalWriter::open_segment(SegmentId id) -> Status
{
    CALICO_EXPECT_EQ(m_writer, std::nullopt);
    AppendWriter *file;
    auto s = m_storage->open_append_writer(m_prefix + id.to_name(), &file);
    if (s.is_ok()) {
        m_file.reset(file);
        m_writer = LogWriter {*m_file, m_tail, *m_flushed_lsn};
        m_current = id;
    }
    return s;
}

auto WalWriter::close_segment() -> Status
{
    // We must have failed while opening the segment file.
    if (!m_writer)
        return logic_error("segment file is already closed");

    auto s = m_writer->flush();
    bool is_empty {};

    // We get a logic error if the tail buffer was empty. In this case, it is possible
    // that the whole segment is empty.
    if (!s.is_ok()) {
        is_empty = m_writer->block_count() == 0;
        if (s.is_logic_error())
            s = ok();
    }
    m_writer.reset();
    m_file.reset();

    auto id = std::exchange(m_current, SegmentId::null());

    // We want to do this part, even if the flush failed.
    if (is_empty) {
        auto t = m_storage->remove_file(m_prefix + id.to_name());
        s = s.is_ok() ? t : s;
    } else {
        m_set->add_segment(id);
    }
    return s;
}

auto WalWriter::advance_segment() -> Status
{
    auto s = close_segment();
    if (s.is_ok()) {
        auto id = ++m_set->last();
        return open_segment(id);
    }
    return s;
}

} // namespace Calico
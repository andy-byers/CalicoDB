#include "writer.h"
#include "utils/types.h"
#include <optional>

namespace Calico {

auto LogWriter::write(WalPayloadIn payload) -> Status
{
    const auto lsn = payload.lsn();
    CALICO_EXPECT_FALSE(lsn.is_null());
    auto data = payload.m_buffer;

    WalRecordHeader lhs;
    lhs.type = WalRecordHeader::Type::FULL;
    lhs.size = static_cast<std::uint16_t>(data.size());
    lhs.crc = crc32c::Value(data.data(), data.size());

    while (!data.is_empty()) {
        auto rest = m_tail;
        // Note that this modifies rest to point to [<m_offset>, <end>) in the tail buffer.
        const auto space_remaining = rest.advance(m_offset).size();
        const auto can_fit_some = space_remaining > WalRecordHeader::SIZE;
        const auto can_fit_all = space_remaining >= WalRecordHeader::SIZE + data.size();

        if (can_fit_some) {
            WalRecordHeader rhs;

            if (!can_fit_all) {
                rhs = split_record(lhs, data, space_remaining);
            }

            // We must have room for the whole header and at least 1 payload byte.
            write_wal_record_header(rest, lhs);
            rest.advance(WalRecordHeader::SIZE);
            mem_copy(rest, data.range(0, lhs.size));

            m_offset += WalRecordHeader::SIZE + lhs.size;
            data.advance(lhs.size);
            rest.advance(lhs.size);

            if (!can_fit_all) {
                lhs = rhs;
            }

            // The new value of m_offset must be less than or equal to the start of the next block. If it is exactly
            // at the start of the next block, we should fall through and read it into the tail buffer.
            if (m_offset != m_tail.size()) {
                continue;
            }
        }
        CALICO_EXPECT_LE(m_tail.size() - m_offset, WalRecordHeader::SIZE);
        if (auto s = flush(); !s.is_ok()) {
            return s;
        }
    }
    // Record is fully in the tail buffer and maybe partially on disk. Next time we flush, this record is guaranteed
    // to be all the way on disk.
    m_last_lsn = lsn;
    return ok();
}

auto LogWriter::flush() -> Status
{
    // Already flushed.
    if (m_offset == 0) {
        return ok();
    }

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
      system {param.system},
      m_set {param.set},
      m_tail {param.tail},
      m_wal_limit {param.wal_limit}
{
    CALICO_EXPECT_FALSE(m_prefix.empty());
    CALICO_EXPECT_NE(m_flushed_lsn, nullptr);
    CALICO_EXPECT_NE(m_storage, nullptr);
    CALICO_EXPECT_NE(system, nullptr);
    CALICO_EXPECT_NE(m_set, nullptr);

    // First segment file gets created now, but is not registered in the WAL set until the writer
    // is finished with it.
    CALICO_ERROR_IF(open_segment({m_set->last().value + 1}));
}

auto WalWriter::write(WalPayloadIn payload) -> void
{
    if (m_writer.has_value()) {
        CALICO_ERROR_IF(m_writer->write(payload));
        if (m_writer->block_count() >= m_wal_limit) {
            CALICO_ERROR_IF(advance_segment());
        }
    }
}

auto WalWriter::flush() -> void
{
    if (m_writer.has_value()) {
        CALICO_ERROR_IF(m_writer->flush());
    }
}

auto WalWriter::advance() -> void
{
    if (m_writer.has_value()) {
        // NOTE: advance() is a NOOP if the current WAL segment hasn't been written to.
        CALICO_ERROR_IF(advance_segment());
    }
}

auto WalWriter::destroy() && -> Status
{
    return close_segment();
}

auto WalWriter::open_segment(Id id) -> Status
{
    CALICO_EXPECT_EQ(m_writer, std::nullopt);
    AppendWriter *file;
    auto s = m_storage->open_append_writer(m_prefix + encode_segment_name(id), &file);
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
    if (!m_writer) {
        return logic_error("segment file is already closed");
    }

    flush();
    const auto written = m_writer->block_count() != 0;

    m_writer.reset();
    m_file.reset();

    if (const auto id = std::exchange(m_current, Id::null()); written) {
        m_set->add_segment(id);
    } else {
        CALICO_TRY_S(m_storage->remove_file(m_prefix + encode_segment_name(id)));
    }
    return ok();
}

auto WalWriter::advance_segment() -> Status
{
    auto s = close_segment();
    if (s.is_ok()) {
        return open_segment({m_set->last().value + 1});
    }
    return s;
}

} // namespace Calico

#include "wal_writer.h"
#include "calico/exception.h"
#include "file/interface.h"
#include "utils/identifier.h"
#include "utils/logging.h"
#include "wal_reader.h"
#include <optional>

namespace calico {

WALWriter::WALWriter(Parameters param)
    : m_file {std::move(param.wal_file)},
      m_logger {logging::create_logger(param.log_sink, "WALWriter")},
      m_block(param.block_size, '\x00')
{
    m_logger->trace("starting WALWriter");

    const auto is_block_size_valid = is_power_of_two(param.block_size) &&
                                     param.block_size >= MIN_BLOCK_SIZE &&
                                     param.block_size <= MAX_BLOCK_SIZE;

    if (!is_block_size_valid) {
        logging::MessageGroup group;
        group.set_primary("cannot open WAL writer");
        group.set_detail("WAL block size {} is invalid", param.block_size);
        group.set_hint("must be a power of 2 in [{}, {}]", MIN_BLOCK_SIZE, MAX_BLOCK_SIZE);
        throw std::invalid_argument {group.err(*m_logger)};
    }
}

auto WALWriter::has_committed() const -> bool
{
    return m_file->size() > 0;
}

auto WALWriter::append(WALRecord record) -> LSN
{
    std::optional<WALRecord> temp {std::move(record)};
    const auto lsn = temp->lsn();
    auto flushed = false;

    while (temp) {
        const auto remaining = m_block.size() - m_cursor;

        // Each record must contain at least 1 payload byte.
        const auto can_fit_some = remaining > WALRecord::HEADER_SIZE;
        const auto can_fit_all = remaining >= temp->size();

        if (can_fit_some) {
            WALRecord rest;

            if (!can_fit_all)
                rest = temp->split(remaining - WALRecord::HEADER_SIZE);

            auto destination = stob(m_block).range(m_cursor, temp->size());
            temp->write(destination);

            m_cursor += temp->size();

            if (can_fit_all) {
                temp.reset();
            } else {
                temp = rest;
            }
            continue;
        }
        flush();
        flushed = true;
    }
    // If we flushed, the last record to be put to the tail buffer is guaranteed to be on disk. Some
    // or all of the current record will be in the tail buffer.
    const auto last_lsn = std::exchange(m_last_lsn, lsn);
    return flushed ? last_lsn : LSN::null();
}

auto WALWriter::truncate() -> void
{
    m_file->resize(0);
    m_file->sync();
}

auto WALWriter::flush() -> LSN
{
    if (m_cursor) {
        // The unused part of the block should be zero-filled.
        auto block = stob(m_block);
        mem_clear(block.range(m_cursor));

        m_file->write(block);
        m_file->sync();

        m_cursor = 0;
        return m_last_lsn;
    }
    return LSN::null();
}

} // namespace calico
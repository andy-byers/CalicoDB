
#include "wal_writer.h"
#include "calico/exception.h"
#include "storage/interface.h"
#include "utils/identifier.h"
#include "utils/logging.h"
#include "wal_reader.h"
#include <optional>

namespace calico {

WALWriter::WALWriter(Parameters param)
    : m_file {param.directory.open_file(WAL_NAME, Mode::CREATE | Mode::WRITE_ONLY, 0666)},
      m_writer {m_file->open_writer()},
      m_logger {logging::create_logger(param.log_sink, "WALWriter")},
      m_block(param.block_size, '\x00')
{
    static constexpr auto ERROR_PRIMARY = "cannot open WAL writer";
    m_logger->trace("starting WALWriter");

    if (param.block_size < MINIMUM_PAGE_SIZE) {
        logging::MessageGroup group;
        group.set_primary(ERROR_PRIMARY);
        group.set_detail("WAL block size {} is too small", param.block_size);
        group.set_hint("must be greater than or equal to {}", MINIMUM_BLOCK_SIZE);
        throw std::invalid_argument {group.error(*m_logger)};
    }
    if (param.block_size > MAXIMUM_BLOCK_SIZE) {
        logging::MessageGroup group;
        group.set_primary(ERROR_PRIMARY);
        group.set_detail("WAL block size {} is too large", param.block_size);
        group.set_hint("must be less than or equal to {}", MAXIMUM_BLOCK_SIZE);
        throw std::invalid_argument {group.error(*m_logger)};
    }
    if (!is_power_of_two(param.block_size)) {
        logging::MessageGroup group;
        group.set_primary(ERROR_PRIMARY);
        group.set_detail("WAL block size {} is invalid", param.block_size);
        group.set_hint("must be a power of 2");
        throw std::invalid_argument {group.error(*m_logger)};
    }
}

auto WALWriter::has_committed() const -> bool
{
    return m_file->size() > 0;
}

auto WALWriter::append(WALRecord record) -> LSN
{
    m_logger->trace("appending {} B record with LSN {}", record.size(), record.lsn().value);
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
    m_logger->trace("truncating WAL");
    m_writer->resize(0);
    m_writer->sync();
}

auto WALWriter::flush() -> LSN
{
    m_logger->trace("trying to flush WAL");

    if (m_cursor) {
        // The unused part of the block should be zero-filled.
        auto block = stob(m_block);
        mem_clear(block.range(m_cursor));

        m_writer->write(block);
        m_writer->sync();
        m_logger->trace("WAL has been flushed up to LSN {}", m_last_lsn.value);

        m_cursor = 0;
        return m_last_lsn;
    }
    m_logger->trace("nothing to flush");
    return LSN::null();
}

} // namespace calico
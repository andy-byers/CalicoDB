
#include "exception.h"
#include "wal_reader.h"
#include "wal_writer.h"
#include "file/interface.h"

namespace cub {

WALWriter::WALWriter(std::unique_ptr<ILogFile> file, Size block_size)
    : m_file{std::move(file)}
    , m_block(block_size, '\x00')
{
    if (!is_power_of_two(block_size))
        throw InvalidArgumentError{"WAL block size must be a power of 2"};

    if (block_size < MIN_BLOCK_SIZE)
        throw InvalidArgumentError{"WAL block size is too small"};

    if (block_size > MAX_BLOCK_SIZE)
        throw InvalidArgumentError{"WAL block size is too large"};
}

auto WALWriter::block_size() const -> Size 
{
    return m_block.size();
}

auto WALWriter::has_pending() const -> bool
{
    return m_cursor > 0;
}

auto WALWriter::has_committed() const -> bool
{
    return m_file->size() > 0;
}

auto WALWriter::write(WALRecord record)  -> LSN
{
    std::optional<WALRecord> temp {record};
    const auto lsn = record.lsn();
    auto flushed = false;

    while (temp) {
        const auto remaining = m_block.size() - m_cursor;

        // Each record must contain at least 1 payload byte.
        const auto can_fit_some = remaining > WALRecord::HEADER_SIZE;
        const auto can_fit_all = remaining >= temp->size();

        if (can_fit_some) {
            std::optional<WALRecord> rest;

            if (!can_fit_all)
                rest = temp->split(remaining - WALRecord::HEADER_SIZE);

            auto destination = _b(m_block).range(m_cursor, temp->size());
            temp->write(destination);

            m_cursor += temp->size();
            temp = rest;
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
        auto block = _b(m_block);
        mem_clear(block.range(m_cursor));

        m_file->write(block);
        m_file->sync();

        m_cursor = 0;
        return m_last_lsn;
    }
    return LSN::null();
}

} // Cub
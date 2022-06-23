#include "calico/exception.h"
#include "wal_reader.h"
#include "wal_record.h"
#include "file/interface.h"
#include "page/page.h"
#include "utils/logging.h"

namespace calico {

WALReader::WALReader(Parameters param)
    : m_block(param.block_size, '\x00'),
      m_file {std::move(param.wal_file)},
      m_logger {logging::create_logger(std::move(param.log_sink), "WALReader")}
{
    m_logger->trace("constructing WAL reader");
    // Start out on the first record, if it exists.
    m_record = read_next();
}

/**
 * Move the cursor to the beginning of the WAL file.
 */
auto WALReader::reset() -> void
{
    m_logger->trace("moving cursor to the beginning of the WAL file");

    m_file->seek(0, Seek::BEGIN);
    m_has_block = false;
    m_cursor = 0;
    m_block_id = 0;
    m_positions.clear();
    m_record.reset();
    increment();
}

/**
 * Get the WAL record that the cursor is currently over.
 *
 * @return The record if the WAL file is not empty, std::nullopt otherwise
 */
auto WALReader::record() const -> std::optional<WALRecord>
{
    return m_record;
}

/**
 * Move the cursor toward the end of the WAL.
 *
 * @return True if the cursor was successfully moved, false otherwise
 */
auto WALReader::increment() -> bool
{
    m_incremented = true;
    if (auto record = read_next()) {
        m_record = std::move(record);
        return m_record && m_incremented;
    }
    return false;
}

/**
 * Move the cursor toward the beginning of the WAL.
 *
 * @return True if the cursor was successfully moved, false otherwise
 */
auto WALReader::decrement() -> bool
{
    if (!m_record)
        return false;
    if (auto record = read_previous()) {
        m_record = std::move(record);
        return true;
    }
    return false;
}

/**
 * Read the next WAL record, advancing the cursor.
 *
 * @return The next WAL record if we are not already at the end, std::nullopt otherwise
 */
auto WALReader::read_next() -> std::optional<WALRecord>
{
    WALRecord record;

    if (!m_has_block)
        read_block();
    push_position();

    try {
        while (record.type() != WALRecord::Type::FULL) {
            // Merge partial values until we have a full record.
            if (auto maybe_partial = read_record()) {
                record.merge(*maybe_partial);
                // We just hit EOF. Note that we discard `record`, which may contain a non-FULL record.
            } else {
                pop_position_and_seek();
                return std::nullopt;
            }
        }
        if (!record.is_consistent())
            throw CorruptionError {"cannot read record: record is corrupted"};

        return record;

    } catch (const CorruptionError&) {
        m_incremented = false;
        if (auto previous = read_previous())
            return previous;
        if (!m_positions.empty())
            pop_position_and_seek();
        return std::nullopt;
    }
}

/**
 * Read the previous WAL record, moving the cursor backward.
 *
 * @return The previous WAL record if we are not already at the beginning, std::nullopt otherwise
 */
auto WALReader::read_previous() -> std::optional<WALRecord>
{
    if (m_positions.size() >= 2) {
        pop_position_and_seek();
        pop_position_and_seek();
        return read_next();
    }
    return std::nullopt;
}

/**
 * Read the WAL record at the current cursor position.
 *
 * This method causes the next block of the WAL file to be read, once the cursor reaches the end of the current one.
 *
 * @return The WAL record at the specified offset if it exists, std::nullopt otherwise
 */
auto WALReader::read_record() -> std::optional<WALRecord>
{
    const auto out_of_space = m_block.size() - m_cursor <= WALRecord::HEADER_SIZE;
    const auto needs_new_block = out_of_space || !m_has_block;

    if (needs_new_block) {
        if (out_of_space) {
            m_block_id++;
            m_cursor = 0;
        }
        if (!read_block())
            return std::nullopt;
    }
    if (auto record = read_record_aux(m_cursor)) {
        m_cursor += record->size();
        CALICO_EXPECT_LE(m_cursor, m_block.size());
        return std::move(*record);
    }
    m_cursor = m_block.size();
    return read_record();
}

/**
 * Helper for reading WAL records out of the tail buffer.
 *
 * @param offset Offset at which to read the record
 * @return WAL record at the specified offset
 */
auto WALReader::read_record_aux(Index offset) -> std::optional<WALRecord>
{
    // There should be enough space for a minimally-sized record in the tail buffer.
    CALICO_EXPECT_TRUE(m_has_block);
    CALICO_EXPECT_GT(m_block.size() - offset, WALRecord::HEADER_SIZE);

    WALRecord record;
    auto buffer = stob(m_block);
    buffer.advance(offset);
    record.read(buffer);

    switch (record.type()) {
        case WALRecord::Type::FIRST:
        case WALRecord::Type::MIDDLE:
        case WALRecord::Type::LAST:
        case WALRecord::Type::FULL:
            return record;
        case WALRecord::Type::EMPTY:
            return std::nullopt;
        default:
            logging::MessageGroup group;
            group.set_primary("cannot read record");
            group.set_detail("WAL record type {} is not recognized", static_cast<unsigned>(record.type()));
            throw CorruptionError {group.err(*m_logger)};
    }
}

auto WALReader::push_position() -> void
{
    const auto absolute = m_block.size()*m_block_id + m_cursor;
    m_positions.push_back(absolute);
}

auto WALReader::pop_position_and_seek() -> void
{
    const auto absolute = m_positions.back();
    const auto block_id = absolute / m_block.size();
    const auto needs_new_block = m_block_id != block_id;

    m_block_id = block_id;
    m_cursor = absolute % m_block.size();
    m_positions.pop_back();

    if (needs_new_block)
        read_block(); // TODO: Take the block ID as a parameter and hoist above the above assignments.
                      //       We don't want to change anything until the read has succeeded.
}

auto WALReader::read_block() -> bool
{
    try {
        const auto block_start = m_block_id * m_block.size();
        if (const auto bytes_read = m_file->read_at(stob(m_block), block_start)) {
            if (bytes_read != m_block.size()) {
                logging::MessageGroup group;
                group.set_primary("cannot read block");
                group.set_detail("WAL contains an incomplete block of size {} B", bytes_read);
                group.set_hint("block size is {} B", m_block.size());
                throw CorruptionError {group.err(*m_logger)};
            }
            m_has_block = true;
            return true;
        }
        return false;

    } catch (...) {
        m_has_block = false;
        m_positions.clear();
        m_record.reset();
        m_cursor = 0;
        throw;
    }
}

} // calico
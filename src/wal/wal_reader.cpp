#include "exception.h"
#include "wal_reader.h"
#include "wal_record.h"
#include "file/interface.h"
#include "page/page.h"
#include "utils/crc.h"

namespace cub {

WALReader::WALReader(std::unique_ptr<IReadOnlyFile> file, Size block_size)
    : m_block(block_size, '\x00')
    , m_file {std::move(file)} {}

auto WALReader::reset() -> void
{
    m_file->seek(0, Seek::BEGIN);
    m_has_block = false;
    m_cursor = 0;
    m_positions.clear();
    m_record.reset();
    increment();
}

auto WALReader::record() const -> std::optional<WALRecord>
{
    return m_record;
}

auto WALReader::increment() -> bool
{
    if (auto record = read_next()) {
        m_record = std::move(record);
        return true;
    }
    return false;
}

auto WALReader::decrement() -> bool
{
    if (auto record = read_previous()) {
        m_record = std::move(record);
        return true;
    }
    return false;
}

// WALReader::increment() Routine:
//   (1) If we have a record already, push the cursor to the traversal stack and increment it by the record size.
//   (2) Read the next record. If it is a FULL record, stop. Otherwise, keep reading until we reach a LAST record.
//       If we reach EOF before a LAST record, we must roll back the cursor back to the start of the last complete record.
//
// WALReader Behavior:
//
//
// Incomplete WAL Records
// A WAL record is incomplete if it is not a FULL record and does not end with a LAST record. If the record in question is
// not the last record in the WAL file, then the WAL is considered corrupted. Otherwise, this indicates that there was a
// system failure before the LAST record could be flushed.

auto WALReader::read_next() -> std::optional<WALRecord>
{
    WALRecord record;

    if (!m_has_block)
        read_block();
    push_position();

    while (record.type() != WALRecord::Type::FULL) {
        // Merge partial values until we have a full record.
        if (auto maybe_partial = read_record()) {
            record.merge(*maybe_partial);
        // We just hit EOF. Note that we discard_handlers `record`, which may contain a non-FULL record.
        } else {
            pop_position_and_seek();
            return std::nullopt;
        }
    }
    if (!record.is_consistent())
        throw CorruptionError{"Record has an invalid CRC"};
    return record;
}

auto WALReader::read_record() -> std::optional<WALRecord>
{
    const auto out_of_space = m_block.size() - m_cursor <= WALRecord::HEADER_SIZE;;
    const auto needs_new_block = out_of_space || !m_has_block;;

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
        CUB_EXPECT_LE(m_cursor, m_block.size());
        return std::move(*record);
    }
    // Read an empty record. Try again in the next block, if it exists.
    m_cursor = m_block.size();
    return read_record();
}

/**
 *
 * @param offset
 * @return
 */
auto WALReader::read_record_aux(Index offset) -> std::optional<WALRecord>
{
    // There should be enough space for a minimally-sized record in the tail buffer.
    CUB_EXPECT_TRUE(m_has_block);
    CUB_EXPECT_GT(m_block.size() - offset, WALRecord::HEADER_SIZE);

    WALRecord record;
    auto buffer = _b(m_block);
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
            throw CorruptionError{"WAL record type is invalid"};
    }
}

auto WALReader::read_previous() -> std::optional<WALRecord>
{
    if (m_positions.size() >= 2) {
        pop_position_and_seek();
        pop_position_and_seek();
        return read_next();
    }
    return std::nullopt;
}

auto WALReader::push_position() -> void
{
    const auto absolute = m_block.size()*m_block_id + m_cursor;;
    m_positions.push_back(absolute);
}

auto WALReader::pop_position_and_seek() -> void
{
    const auto absolute = m_positions.back();;
    const auto block_id = absolute / m_block.size();;
    const auto needs_new_block = m_block_id != block_id;;

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
        const auto block_start = m_block_id * m_block.size();;
        if (const auto bytes_read = m_file->read_at(_b(m_block), block_start)) {
            if (bytes_read != m_block.size())
                throw IOError::partial_read();
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

} // db
#include "wal_reader.h"
#include "calico/exception.h"
#include "page/page.h"
#include "storage/file.h"
#include "storage/interface.h"
#include "utils/logging.h"
#include "wal_record.h"

namespace calico {

WALReader::WALReader(Parameters param)
    : m_block(param.block_size, '\x00'),
      m_file {param.directory.open_file(WAL_NAME, Mode::CREATE | Mode::READ_ONLY, 0666)},
      m_reader {m_file->open_reader()},
      m_logger {logging::create_logger(std::move(param.log_sink), "WALReader")}
{
    m_logger->trace("constructing WAL reader");
    // Start out on the first record, if it exists.
    m_record = read_next();
}

/**
 * Move the cursor to the beginning of the WAL storage.
 */
auto WALReader::reset() -> void
{
    m_logger->trace("resetting WAL reader");

    m_reader->seek(0, Seek::BEGIN);
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
 * @return The record if the WAL storage is not empty, std::nullopt otherwise
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
        m_logger->trace("incremented to record with LSN {}", m_record->lsn().value);
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
        m_logger->trace("decremented to record with LSN {}", m_record->lsn().value);
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
        if (!record.is_consistent()) {
            logging::MessageGroup group;
            group.set_primary("cannot read WAL record");
            group.set_detail("record with LSN {} is corrupted", record.lsn().value);
            group.set_hint("block ID is {} and block offset is {}", m_block_id, m_cursor);
            // This exception gets ignored, but we still get the log output.
            throw CorruptionError {group.warn(*m_logger)};
        }
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
 * This method causes the next block of the WAL storage to be read, once the cursor reaches the end of the current one.
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
            group.set_detail("WAL record type {} is not recognized", static_cast<int>(record.type()));
            throw CorruptionError {group.warn(*m_logger)};
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
        if (const auto bytes_read = m_reader->read_at(stob(m_block), block_start)) {
            if (bytes_read != m_block.size()) {
                logging::MessageGroup group;
                group.set_primary("cannot read block");
                group.set_detail("WAL contains an incomplete block of size {} B", bytes_read);
                group.set_hint("block size is {} B", m_block.size());
                throw CorruptionError {group.warn(*m_logger)};
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








auto WALReader::noex_read_next() -> Result<std::optional<WALRecord>>
{
    WALRecord record;

    if (!m_has_block) {
        if (const auto success = noex_read_block()) {
            if (!success.value())
                return success;
        } else {
            return success;
        }
    }
    push_position();

    try {
        while (record.type() != WALRecord::Type::FULL) {
            // Merge partial values until we have a full record.
            if (auto maybe_partial = noex_read_record()) {
                record.merge(*maybe_partial);
                // We just hit EOF. Note that we discard `record`, which may contain a non-FULL record.
            } else {
                pop_position_and_seek();
                return std::nullopt;
            }
        }
        if (!record.is_consistent()) {
            logging::MessageGroup group;
            group.set_primary("cannot read WAL record");
            group.set_detail("record with LSN {} is corrupted", record.lsn().value);
            group.set_hint("block ID is {} and block offset is {}", m_block_id, m_cursor);
            // This exception gets ignored, but we still get the log output.
            throw CorruptionError {group.warn(*m_logger)};
        }
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

auto WALReader::noex_read_previous() -> Result<std::optional<WALRecord>>
{
    CALICO_EXPECT_GE(m_positions.size(), 2);
    if (m_positions.size() >= 2) {
        return noex_pop_position_and_seek()
            .and_then(noex_pop_position_and_seek)
            .and_then(read_next);
    }
    return std::nullopt;
}

auto WALReader::noex_read_record() -> Result<std::optional<WALRecord>>
{
    const auto out_of_space = m_block.size() - m_cursor <= WALRecord::HEADER_SIZE;
    const auto needs_new_block = out_of_space || !m_has_block;

    if (needs_new_block) {
        if (out_of_space) {
            m_block_id++;
            m_cursor = 0;
        }
        if (const auto success = noex_read_block()) {
            if (!success.value())
                return std::nullopt;
        } else {
            return success;
        }
    }
    if (auto record = noex_read_record_aux(m_cursor)) {
        if (record.value()) {
            m_cursor += record.value()->size();
            CALICO_EXPECT_LE(m_cursor, m_block.size());
            return record;
        }
    } else {
        return record;
    }
    m_cursor = m_block.size();
    return noex_read_record();
}

auto WALReader::noex_read_record_aux(Index offset) -> Result<std::optional<WALRecord>>
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
            group.set_detail("WAL record type {} is not recognized", static_cast<int>(record.type()));
            return ErrorResult {group.corruption(*m_logger)};
    }
}

auto WALReader::noex_pop_position_and_seek() -> Result<void>
{
    const auto absolute = m_positions.back();
    const auto block_id = absolute / m_block.size();
    const auto needs_new_block = m_block_id != block_id;

    m_block_id = block_id;
    m_cursor = absolute % m_block.size();
    m_positions.pop_back();

    if (needs_new_block)
        return noex_read_block().map([] {return Result<void> {};});
    return {};
}

auto WALReader::noex_try_pop_position_and_seek() -> Result<void>
{
    const auto absolute = m_positions.back();
    const auto block_id = absolute / m_block.size();
    const auto needs_new_block = m_block_id != block_id;

    if (needs_new_block) {
        if (const auto result = noex_read_block(); !result) {
            return result;
        } else if (!result.value()) {
            logging::MessageGroup group;
            group.set_primary("unable to seek backward");
            group.set_detail("unexpected EOF");
            return ErrorResult {group.corruption(*m_logger)};
        }
    }

    m_block_id = block_id;
    m_cursor = absolute % m_block.size();
    m_positions.pop_back();
}

auto WALReader::noex_read_block() -> Result<bool>
{

    return m_reader->noex_read_at(stob(m_block), m_block_id * m_block.size())
        .and_then([this](Size read_size) -> Result<bool> {
            if (read_size == 0)
                return false;
            m_has_block = true;
            return true;
        })
        .or_else([this](const Error &error) -> Result<bool> {
            m_has_block = false;
            m_positions.clear();
            m_record.reset();
            m_cursor = 0;
            m_logger->error(btos(error.what()));
            return ErrorResult {error};
        });
}

} // calico
#include "wal_reader.h"
#include "wal_record.h"
#include "page/page.h"
#include "storage/interface.h"
#include "utils/logging.h"

namespace cco {

using namespace utils;

auto WALReader::open(const WALParameters &param) -> Result<std::unique_ptr<IWALReader>>
{
    CCO_EXPECT_GE(param.page_size, MINIMUM_PAGE_SIZE);
    CCO_EXPECT_LE(param.page_size, MAXIMUM_PAGE_SIZE);
    CCO_EXPECT_TRUE(is_power_of_two(param.page_size));

    CCO_TRY_CREATE(file, param.directory.open_file(WAL_NAME, Mode::CREATE | Mode::READ_ONLY, 0666));
    auto reader = std::unique_ptr<IWALReader> {new(std::nothrow) WALReader {std::move(file), param}};
    if (!reader) {
        ThreePartMessage message;
        message.set_primary("cannot open WAL reader");
        message.set_detail("out of memory");
        return Err {message.system_error()};
    }
    return reader;
}

WALReader::WALReader(std::unique_ptr<IFile> file, WALParameters param):
      m_block(param.page_size, '\x00'),
      m_file {std::move(file)} {}

auto WALReader::close() -> Result<void>
{
    return m_file->close();
}

/**
 * Move the cursor to the beginning of the WAL storage.
 */
auto WALReader::reset() -> Result<void>
{
    CCO_TRY(m_file->seek(0, Seek::BEGIN));
    m_has_block = false;
    m_cursor = 0;
    m_block_id = 0;
    m_positions.clear();
    m_record.reset();
    CCO_TRY(increment());
    return {};
}

auto WALReader::record() const -> std::optional<WALRecord>
{
    return m_record;
}

/**
 * Move the cursor toward the end of the WAL.
 *
 * @return True if the cursor was successfully moved, false otherwise
 */
auto WALReader::increment() -> Result<bool>
{
    CCO_TRY_CREATE(record, read_next());
    m_incremented = true; // TODO: Why???
    if (record) {
        m_record = std::move(record);
        return true;
    }
    return false;
}

/**
 * Move the cursor toward the beginning of the WAL.
 *
 * @return True if the cursor was successfully moved, false otherwise
 */
auto WALReader::decrement() -> Result<bool>
{
    if (!m_record)
        return false;
    CCO_TRY_CREATE(record, read_previous());
    if (record) {
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
auto WALReader::read_next() -> Result<std::optional<WALRecord>>
{
    WALRecord record;

    if (!m_has_block)
        CCO_TRY(read_block());
    push_position();

    const auto do_read_next = [this, &record]() -> Result<std::optional<WALRecord>> {
        while (record.type() != WALRecord::Type::FULL) {
            // Merge partial values until we have a full record.
            CCO_TRY_CREATE(maybe_partial, read_record());
            if (maybe_partial) {
                CCO_TRY(record.merge(*maybe_partial));
                // We just hit EOF. Note that we discard `record`, which may contain a non-FULL record.
            } else {
                CCO_TRY(pop_position_and_seek());
                return std::nullopt;
            }
        }
        if (!record.is_consistent())
            return Err {Status::corruption("")};
        return record;
    };

    return do_read_next()
        .or_else([this](const Status&) -> Result<std::optional<WALRecord>> {
            m_incremented = false;
            CCO_TRY_CREATE(previous, read_previous());
            if (previous)
                return previous;
            if (!m_positions.empty())
                CCO_TRY(pop_position_and_seek());
            return std::nullopt;
        });
}

auto WALReader::read_previous() -> Result<std::optional<WALRecord>>
{
    if (m_positions.size() >= 2) {
        pop_position_and_seek();
        pop_position_and_seek();
        return read_next();
    }
    return std::nullopt;
}

auto WALReader::read_record() -> Result<std::optional<WALRecord>>
{
    const auto out_of_space = m_block.size() - m_cursor <= WALRecord::HEADER_SIZE;
    const auto needs_new_block = out_of_space || !m_has_block;

    if (needs_new_block) {
        if (out_of_space) {
            m_block_id++;
            m_cursor = 0;
        }
        CCO_TRY_CREATE(was_read, read_block());
        if (!was_read)
            return std::nullopt;
    }
    CCO_TRY_CREATE(record, read_record_aux(m_cursor));
    if (record) {
        m_cursor += record->size();
        CCO_EXPECT_LE(m_cursor, m_block.size());
        return std::move(*record);
    }
    m_cursor = m_block.size();
    return read_record();
}

auto WALReader::read_record_aux(Index offset) -> Result<std::optional<WALRecord>>
{
    static constexpr auto ERROR_PRIMARY = "cannot read record";

    // There should be enough space for a minimally-sized record in the tail buffer.
    CCO_EXPECT_TRUE(m_has_block);
    CCO_EXPECT_GT(m_block.size() - offset, WALRecord::HEADER_SIZE);

    WALRecord record;
    auto buffer = stob(m_block);
    buffer.advance(offset);
    CCO_TRY(record.read(buffer));

    switch (record.type()) {
        case WALRecord::Type::FIRST:
        case WALRecord::Type::MIDDLE:
        case WALRecord::Type::LAST:
        case WALRecord::Type::FULL:
            return record;
        case WALRecord::Type::EMPTY:
            return std::nullopt;
        default:
            ThreePartMessage message;
            message.set_primary(ERROR_PRIMARY);
            message.set_detail("type {} is not recognized", static_cast<int>(record.type()));
            return Err {message.corruption()};
    }
}

auto WALReader::push_position() -> void
{
    const auto absolute = m_block.size()*m_block_id + m_cursor;
    m_positions.push_back(absolute);
}

auto WALReader::pop_position_and_seek() -> Result<void>
{
    const auto absolute = m_positions.back();
    const auto block_id = absolute / m_block.size();
    const auto needs_new_block = m_block_id != block_id;

    m_block_id = block_id;
    m_cursor = absolute % m_block.size();
    m_positions.pop_back();

    if (needs_new_block)
        CCO_TRY(read_block());
    return {};
}

auto WALReader::read_block() -> Result<bool>
{
    const auto block_start = m_block_id * m_block.size();
    return m_file->read(stob(m_block), block_start)
        .and_then([this](Size read_size) -> Result<bool> {
            if (read_size != m_block.size()) {
                if (read_size == 0)
                    return false;
                ThreePartMessage message;
                message.set_primary("cannot read block");
                message.set_detail("block is incomplete");
                message.set_hint("read {}/{} B", read_size, m_block.size());
                return Err {message.corruption()};
            }
            m_has_block = true;
            return true;
        })
        .or_else([this](const Status &error) -> Result<bool> {
            m_has_block = false;
            m_positions.clear();
            m_record.reset();
            m_cursor = 0;
            return Err {error};
        });
}

} // cco
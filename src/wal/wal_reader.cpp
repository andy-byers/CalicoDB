#include "wal_reader.h"
#include "page/page.h"
#include "storage/interface.h"
#include "utils/logging.h"

namespace cco {

auto WALReader::create(const WALParameters &param) -> Result<std::unique_ptr<IWALReader>>
{
    CCO_EXPECT_GE(param.page_size, MINIMUM_PAGE_SIZE);
    CCO_EXPECT_LE(param.page_size, MAXIMUM_PAGE_SIZE);
    CCO_EXPECT_TRUE(is_power_of_two(param.page_size));

    auto reader = std::unique_ptr<IWALReader> {new (std::nothrow) WALReader {param}};
    if (!reader) {
        ThreePartMessage message;
        message.set_primary("cannot open WAL reader");
        message.set_detail("out of memory");
        return Err {message.system_error()};
    }
    return reader;
}

WALReader::WALReader(const WALParameters &param)
    : m_block(param.page_size, '\x00')
{
    m_scratch[0] = std::string(param.page_size * 4, '\x00');
    m_scratch[1] = std::string(param.page_size * 4, '\x00');
}

auto WALReader::read(Position &position) -> Result<WALRecord>
{
    WALRecord record {stob(m_scratch[0])};
    while (record.type() != WALRecord::Type::FULL) {
        // Make sure we are buffering the correct block.
        if (!m_has_block || m_block_id != position.block_id) {
            CCO_TRY_CREATE(not_eof, read_block(position.block_id));
            if (!not_eof) return Err {Status::not_found()};
        }
        auto maybe_partial = read_record(position.offset);
        if (maybe_partial.has_value()) {
            position.offset += maybe_partial->size();
            CCO_TRY(record.merge(*maybe_partial));
        } else if (maybe_partial.error().is_not_found() && position.offset > 0) {
            position.block_id++;
            position.offset = 0;
        } else {
            return Err {maybe_partial.error()};
        }
    }
    return record;
}

auto WALReader::is_open() -> bool
{
    return m_file && m_file->is_open();
}


auto WALReader::is_empty() -> Result<bool>
{
    CCO_EXPECT_TRUE(is_open());
    return m_file->size().map([](Size size) {return size == 0;});
}

auto WALReader::open(std::unique_ptr<IFile> file) -> Result<void>
{
    CCO_EXPECT_FALSE(is_open());
    m_file = std::move(file);
    m_has_block = false;
    m_block_id = 0;
    return {};
}

auto WALReader::close() -> Result<void>
{
    return m_file->close();
}

auto WALReader::read_block(Index block_id) -> Result<bool>
{
    const auto block_start = block_id * m_block.size();
    return m_file->read(stob(m_block), block_start)
        .and_then([block_id, this](Size read_size) -> Result<bool> {
            if (read_size != m_block.size()) {
                // If we hit EOF, we didn't read anything into the buffer. Just leave the block ID alone, since
                // the last block we read is still valid.
                if (read_size == 0)
                    return false;
                ThreePartMessage message;
                message.set_primary("cannot read block");
                message.set_detail("block is incomplete");
                message.set_hint("read {}/{} B", read_size, m_block.size());
                m_has_block = false; // We corrupted the buffer. Force a reread.
                return Err {message.corruption()};
            }
            m_block_id = block_id;
            m_has_block = true;
            return true;
        })
        .or_else([this](const Status &error) -> Result<bool> {
            // Buffer could be corrupted, not sure if we have any guarantees about its contents after a failed
            // call to read().
            m_has_block = false;
            return Err {error};
        });
}

auto WALReader::read_record(Index offset) -> Result<WALRecord>
{
    CCO_EXPECT_TRUE(m_has_block);
    if (offset + WALRecord::MINIMUM_SIZE > m_block.size())
        return Err {Status::not_found()};

    WALRecord record {stob(m_scratch[1])};
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
            return Err {Status::not_found()};
        default:
            ThreePartMessage message;
            message.set_primary("cannot read record");
            message.set_detail("type {} is not recognized", static_cast<int>(record.type()));
            return Err {message.corruption()};
    }
}

auto WALReader::reset() -> void
{
    m_has_block = false;
}

WALExplorer::WALExplorer(IWALReader &reader)
    : m_reader {&reader}
{}

auto WALExplorer::reset() -> void
{
    m_position = {};
}

auto WALExplorer::read_next() -> Result<Discovery>
{
    auto position = m_position;
    auto record = m_reader->read(position);
    if (record.has_value()) {
        CCO_EXPECT_GE(record->size(), WALRecord::MINIMUM_SIZE);
        return Discovery {*record, std::exchange(m_position, position)};
    }
    return Err {record.error()};
}

} // namespace cco
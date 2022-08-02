#include "wal_writer.h"
#include "storage/interface.h"
#include "utils/identifier.h"
#include "utils/logging.h"
#include <optional>

namespace cco {

auto WALWriter::create(const WALParameters &param) -> Result<std::unique_ptr<IWALWriter>>
{
    CCO_EXPECT_GE(param.page_size, MINIMUM_PAGE_SIZE);
    CCO_EXPECT_LE(param.page_size, MAXIMUM_PAGE_SIZE);
    CCO_EXPECT_TRUE(is_power_of_two(param.page_size));

    auto writer = std::unique_ptr<WALWriter> {new (std::nothrow) WALWriter {param}};
    if (!writer) {
        ThreePartMessage message;
        message.set_primary("cannot open WAL writer");
        message.set_detail("out of memory");
        return Err {message.system_error()};
    }
    return writer;
}

WALWriter::WALWriter(const WALParameters &param)
    : m_tail(param.page_size, '\x00')
{}

auto WALWriter::is_open() -> bool
{
    return m_file && m_file->is_open();
}

auto WALWriter::needs_segmentation() -> bool
{
    return m_position.block_id > 32; // TODO: Should be an option.
}

auto WALWriter::open(std::unique_ptr<IFile> file) -> Result<void>
{
    CCO_EXPECT_FALSE(is_open());
    m_file = std::move(file);
    m_position = Position {};
    return {};
}

auto WALWriter::close() -> Result<void>
{
    return m_file->close();
}

auto WALWriter::append(WALRecord record) -> Result<Position>
{
//printf("appending LSN %u to %s\n", record.lsn().value, m_file->name().c_str());

    const auto next_lsn {record.lsn()};
    CCO_EXPECT_EQ(next_lsn.value, m_last_lsn.value + 1);

    std::optional<WALRecord> temp {std::move(record)};
    std::optional<Position> first;

    while (temp) {
        const auto remaining = m_tail.size() - m_position.offset;
        const auto can_fit_some = remaining >= WALRecord::MINIMUM_SIZE;
        const auto can_fit_all = remaining >= temp->size();

        if (can_fit_some) {
            WALRecord rest;

            if (!can_fit_all)
                rest = temp->split(remaining - WALRecord::HEADER_SIZE);

            if (!first.has_value())
                first = m_position;

            auto destination = stob(m_tail)
                                   .range(m_position.offset, temp->size());
            temp->write(destination);

            m_position.offset += temp->size();

            if (can_fit_all) {
                temp.reset();
            } else {
                temp = rest;
            }
            continue;
        }
        CCO_TRY(flush());
    }
    CCO_EXPECT_TRUE(first.has_value());
    m_last_lsn = next_lsn;
    return *first;
}

auto WALWriter::truncate() -> Result<void>
{
    return m_file->resize(0)
        .and_then([this]() -> Result<void> {
            return m_file->sync();
        })
        .and_then([this]() -> Result<void> {
            m_position = {};
            mem_clear(stob(m_tail));
            return {};
        });
}

auto WALWriter::flush() -> Result<void>
{
    if (m_position.offset) {
        // The unused part of the block should be zero-filled.
        auto block = stob(m_tail);
        mem_clear(block.range(m_position.offset));

        CCO_TRY(m_file->write(block));
        CCO_TRY(m_file->sync());

        m_position.block_id++;
        m_position.offset = 0;
        m_flushed_lsn = m_last_lsn;
    }
    return {};
}

} // namespace cco
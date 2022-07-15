#include "wal_writer.h"
#include "storage/interface.h"
#include "utils/identifier.h"
#include "utils/logging.h"
#include "wal_record.h"
#include <optional>

namespace cco {

using namespace page;
using namespace utils;

auto WALWriter::open(const Parameters &param) -> Result<std::unique_ptr<IWALWriter>>
{
    static constexpr auto ERROR_PRIMARY = "cannot open WAL writer";
    auto logger = utils::create_logger(param.log_sink, "WALWriter");
    logger->trace("opening");

    auto file = param.directory.open_file(WAL_NAME, Mode::CREATE | Mode::WRITE_ONLY, 0666);
    if (!file.has_value()) {
        logger->error(btos(file.error().what()));
        logger->error(ERROR_PRIMARY);
        return Err {file.error()};
    }
    CCO_EXPECT_GE(param.page_size, MINIMUM_PAGE_SIZE);
    CCO_EXPECT_LE(param.page_size, MAXIMUM_PAGE_SIZE);
    CCO_EXPECT_TRUE(is_power_of_two(param.page_size));
    CCO_TRY_CREATE(file_size, (*file)->size());

    auto writer = std::unique_ptr<WALWriter> {new(std::nothrow) WALWriter {std::move(*file), param}};
    if (!writer) {
        LogMessage group {*logger};
        group.set_primary(ERROR_PRIMARY);
        group.set_detail("out of memory");
        return Err {group.system_error()};
    }
    writer->m_has_committed = file_size > 0;
    return writer;
}

WALWriter::WALWriter(std::unique_ptr<IFile> file, const Parameters &param):
      m_file {std::move(file)},
      m_writer {m_file->open_writer()},
      m_logger {utils::create_logger(param.log_sink, "WALWriter")},
      m_scratch {param.page_size},
      m_block(param.page_size, '\x00') {}

auto WALWriter::post(Page &page) -> void
{
    auto [itr, was_posted] = m_registry.emplace(page.id(), UpdateManager {page, m_scratch.get().data()});
    CCO_EXPECT_TRUE(was_posted);
    page.set_manager(itr->second);
}

auto WALWriter::append(Page &page) -> Result<void>
{
    m_logger->trace("appending WAL record for page {}", page.id().value);

    auto itr = m_registry.find(page.id());
    CCO_EXPECT_NE(itr, end(m_registry));
    PageUpdate update;
    update.page_id = page.id();
    update.previous_lsn = page.lsn();
    page.set_lsn(++m_previous_lsn);
    update.lsn = m_previous_lsn;
    update.changes = itr->second.collect();
    m_registry.erase(itr);

    std::optional<WALRecord> temp {update};

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
        CCO_TRY(flush());
    }
    return {};
}

auto WALWriter::truncate() -> Result<void>
{
    m_logger->trace("truncating");

    return m_writer->resize(0)
        .and_then([this]() -> Result<void> {
            return m_writer->sync();
        })
        .and_then([this]() -> Result<void> {
            m_cursor = 0;
            m_has_committed = false;
            mem_clear(stob(m_block));
            return {};
        });
}

auto WALWriter::flush() -> Result<void>
{
    m_logger->trace("trying to flush WAL");

    if (m_cursor) {
        // The unused part of the block should be zero-filled.
        auto block = stob(m_block);
        mem_clear(block.range(m_cursor));

        CCO_TRY(m_writer->write(block));
        CCO_TRY(m_writer->sync());
        m_logger->trace("WAL has been flushed up to LSN {}", m_previous_lsn.value);

        m_cursor = 0;
        m_flushed_lsn = m_previous_lsn;
        m_has_committed = true;
        return {};
    }
    m_logger->trace("nothing to flush");
    return {};
}

auto WALWriter::discard(PID id) -> void
{
    m_registry.erase(id);
}

} // cco
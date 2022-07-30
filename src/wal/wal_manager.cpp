#include "wal_manager.h"
#include "page/file_header.h"
#include "page/node.h"
#include "pool/interface.h"
#include "storage/interface.h"
#include "utils/logging.h"
#include "wal_reader.h"
#include "wal_record.h"
#include "wal_writer.h"
#include <optional>

namespace cco {

auto WALManager::open(const WALParameters &param) -> Result<std::unique_ptr<IWALManager>>
{
    CCO_EXPECT_GE(param.page_size, MINIMUM_PAGE_SIZE);
    CCO_EXPECT_LE(param.page_size, MAXIMUM_PAGE_SIZE);
    CCO_EXPECT_TRUE(is_power_of_two(param.page_size));

    CCO_TRY_CREATE(writer, WALWriter::open(param));
    CCO_TRY_CREATE(reader, WALReader::open(param));
    auto manager = std::unique_ptr<WALManager> {new (std::nothrow) WALManager {param}};
    if (!manager) {
        ThreePartMessage message;
        message.set_primary("cannot open WAL manager");
        message.set_detail("out of memory");
        return Err {message.system_error()};
    }
    manager->m_writer = std::move(writer);
    manager->m_reader = std::move(reader);
    return manager;
}

WALManager::WALManager(const WALParameters &param)
    : m_tracker {param.page_size},
      m_logger {create_logger(param.log_sink, "wal")},
      m_pool {param.pool}
{}

auto WALManager::close() -> Result<void>
{
    const auto rr = m_reader->close();
    if (!rr.has_value()) {
        m_logger->error("cannot close WAL reader");
        m_logger->error("(reason) {}", rr.error().what());
    }
    const auto wr = m_writer->close();
    if (!wr.has_value()) {
        m_logger->error("cannot close WAL writer");
        m_logger->error("(reason) {}", wr.error().what());
    }
    // If both close() calls produced an error, we'll lose one of them. It'll end up in the
    // log though.
    return !rr.has_value() ? rr : wr;
}

auto WALManager::has_records() const -> bool
{
    return m_writer->has_committed() || m_writer->has_pending();
}

auto WALManager::flushed_lsn() const -> LSN
{
    return m_writer->flushed_lsn();
}

auto WALManager::track(Page &page) -> void
{
    m_tracker.track(page);
}

auto WALManager::discard(Page &page) -> void
{
    m_tracker.discard(page);
}

auto WALManager::append(Page &page) -> Result<void>
{
    auto update = m_tracker.collect(page, ++m_writer->last_lsn());
    if (!update.changes.empty()) {
        CCO_TRY_CREATE(position, m_writer->append(WALRecord {update}));
        m_positions.emplace_back(position);
    }
    return {};
}

auto WALManager::truncate() -> Result<void>
{
    m_tracker.reset();
    CCO_TRY(m_writer->truncate());
    m_positions.clear();
    return {};
}

auto WALManager::flush() -> Result<void>
{
    m_tracker.reset();
    return m_writer->flush();
}

auto WALManager::recover() -> Result<void>
{
    if (m_writer->has_committed()) {
        CCO_EXPECT_TRUE(m_pool->status().is_ok()); // TODO
        CCO_EXPECT_FALSE(m_writer->has_pending());
        CCO_TRY_CREATE(found_commit, roll_forward());
        if (!found_commit)
            CCO_TRY(roll_backward());
        CCO_TRY(m_pool->flush());
        CCO_TRY(truncate());
    }
    return {};
}

auto WALManager::abort() -> Result<void>
{
    CCO_TRY(flush());
    return roll_backward();
}

auto WALManager::commit() -> Result<void>
{
    // Skip the LSN that will be used for the file header updates.
    const LSN commit_lsn {m_writer->last_lsn().value + 2};
    CCO_TRY_CREATE(root, m_pool->acquire(PID::root(), true));
    auto header = get_file_header_writer(root);
    header.set_flushed_lsn(commit_lsn);
    header.update_header_crc();
    CCO_TRY(m_pool->release(std::move(root)));
    CCO_TRY(m_writer->append(WALRecord::commit(commit_lsn)));
    CCO_TRY(flush());
    return {};
}

auto WALManager::roll_forward() -> Result<bool>
{
    // This method should only be called as part of recovery. If we need to abort, we already have the WAL record
    // positions, so we just roll backward from the end.
    CCO_EXPECT_TRUE(m_positions.empty());
    WALExplorer explorer {*m_reader};
    m_reader->reset();

    for (;;) {
        auto record = read_next(explorer);
        if (!record.has_value()) {
            // We hit EOF but didn't find a commit record.
            if (record.error().is_not_found())
                break;
            return Err {record.error()};
        }

        if (m_writer->flushed_lsn() < record->lsn())
            m_writer->set_flushed_lsn(record->lsn());

        // Stop at the commit record.
        if (record->is_commit())
            return true;

        const auto update = record->decode();
        CCO_TRY_CREATE(page, m_pool->fetch(update.page_id, true));
        CCO_EXPECT_FALSE(page.has_manager());

        if (page.lsn() < record->lsn())
            page.redo(record->lsn(), update.changes);

        CCO_TRY(m_pool->release(std::move(page)));
    }
    return false;
}

auto WALManager::roll_backward() -> Result<void>
{
    auto itr = crbegin(m_positions);
    CCO_EXPECT_NE(itr, crend(m_positions));
    m_reader->reset();

    for (; itr != crend(m_positions); ++itr) {
        auto position = *itr;
        CCO_TRY_CREATE(record, m_reader->read(position));

        if (record.is_commit()) {
            if (itr != crbegin(m_positions)) {
                LogMessage message {*m_logger};
                message.set_primary("cannot roll backward");
                message.set_detail("encountered a misplaced commit record");
                message.set_hint("LSN is {}", record.lsn().value);
                return Err {message.corruption()};
            }
            continue;
        }

        const auto update = record.decode();
        CCO_TRY_CREATE(page, m_pool->fetch(update.page_id, true));
        CCO_EXPECT_FALSE(page.has_manager());
        CCO_EXPECT_EQ(record.lsn(), update.lsn);

        if (page.lsn() >= record.lsn())
            page.undo(update.previous_lsn, update.changes);

        CCO_TRY(m_pool->release(std::move(page)));
    }
    return {};
}

auto WALManager::save_header(FileHeaderWriter &header) -> void
{
    header.set_flushed_lsn(m_writer->flushed_lsn());
}

auto WALManager::load_header(const FileHeaderReader &header) -> void
{
    if (header.flushed_lsn() > m_writer->flushed_lsn())
        m_writer->set_flushed_lsn(header.flushed_lsn());
}

auto WALManager::read_next(WALExplorer &explorer) -> Result<WALRecord>
{
    static constexpr auto ERROR_PRIMARY = "cannot read record";

    auto discovery = explorer.read_next();
    if (discovery.has_value()) {
        m_positions.emplace_back(discovery->position);
        return discovery->record;
    }
    auto status = discovery.error();
    CCO_EXPECT_FALSE(status.is_ok());
    if (!status.is_not_found()) {
        m_logger->error(ERROR_PRIMARY);
        m_logger->error("(reason) {}", status.what());
    }
    return Err {status};
}

} // namespace cco
#include "wal_manager.h"
#include "wal_reader.h"
#include "wal_record.h"
#include "wal_writer.h"
#include "page/file_header.h"
#include "pool/interface.h"
#include "storage/interface.h"
#include "utils/logging.h"
#include <optional>

namespace cco {

using namespace page;
using namespace utils;

auto WALManager::open(const WALParameters &param) -> Result<std::unique_ptr<IWALManager>>
{
    CCO_EXPECT_GE(param.page_size, MINIMUM_PAGE_SIZE);
    CCO_EXPECT_LE(param.page_size, MAXIMUM_PAGE_SIZE);
    CCO_EXPECT_TRUE(is_power_of_two(param.page_size));

    CCO_TRY_CREATE(reader, WALReader::open(param));
    CCO_TRY_CREATE(writer, WALWriter::open(param));
    auto manager = std::unique_ptr<WALManager> {new(std::nothrow) WALManager {std::move(reader), std::move(writer), param}};
    if (!manager) {
        ThreePartMessage message;
        message.set_primary("cannot open WAL manager");
        message.set_detail("out of memory");
        return Err {message.system_error()};
    }
    return manager;
}

WALManager::WALManager(std::unique_ptr<IWALReader> reader, std::unique_ptr<IWALWriter> writer, const WALParameters &param):
      m_scratch {param.page_size},
      m_reader {std::move(reader)},
      m_writer {std::move(writer)},
      m_pool {param.pool} {}

auto WALManager::has_records() const -> bool
{
    return m_writer->has_committed() || m_writer->has_pending();
}

auto WALManager::flushed_lsn() const -> LSN
{
    return m_writer->flushed_lsn();
}

auto WALManager::post(Page &page) -> void
{
    CCO_EXPECT_EQ(m_registry.find(page.id()), end(m_registry));
    auto [itr, was_posted] = m_registry.emplace(page.id(), UpdateManager {page.view(0), m_scratch.get().data()});
    CCO_EXPECT_TRUE(was_posted);
    page.set_manager(itr->second);
}

auto WALManager::append(Page &page) -> Result<void>
{
    auto itr = m_registry.find(page.id());
    CCO_EXPECT_NE(itr, end(m_registry));
    PageUpdate update;
    update.page_id = page.id();
    update.previous_lsn = page.lsn();
    const auto next_lsn = ++m_writer->last_lsn();
    page.set_lsn(next_lsn);
    update.lsn = next_lsn;
    update.changes = itr->second.collect();
    m_registry.erase(itr);
    return m_writer->append(WALRecord {update});
}

auto WALManager::truncate() -> Result<void>
{
    return m_writer->truncate();
}

auto WALManager::flush() -> Result<void>
{
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
    CCO_TRY(roll_forward());
    CCO_TRY(roll_backward());
    return flush();
}

auto WALManager::commit() -> Result<void>
{
    auto next_lsn = ++m_writer->last_lsn();
    CCO_TRY_CREATE(root, m_pool->acquire(PID::root(), true));
    FileHeader header {root};
    header.set_flushed_lsn(next_lsn++);
    CCO_TRY(m_pool->release(std::move(root)));
    CCO_TRY(m_writer->append(WALRecord::commit(next_lsn)));
    CCO_TRY(flush());
    return {};
}

auto WALManager::roll_forward() -> Result<bool>
{
    CCO_TRY(m_reader->reset());

    if (!m_reader->record()) {
        if (m_writer->has_committed()) {
            ThreePartMessage message;
            message.set_primary("cannot roll forward");
            message.set_detail("WAL contains unreadable records");
            return Err {message.corruption()};
        }
        return false;
    }

    for (auto should_continue = true; should_continue; ) {
        CCO_EXPECT_NE(m_reader->record(), std::nullopt);
        const auto record = *m_reader->record();

        if (m_writer->flushed_lsn() < record.lsn())
            m_writer->set_flushed_lsn(record.lsn());

        // Stop at the commit record.
        if (record.is_commit())
            return true;

        const auto update = record.decode();
        CCO_TRY_CREATE(page, m_pool->fetch(update.page_id, true));
        CCO_EXPECT_FALSE(page.has_manager());

        if (page.lsn() < record.lsn())
            page.redo(record.lsn(), update.changes);

        CCO_TRY(m_pool->release(std::move(page)));
        CCO_TRY_STORE(should_continue, m_reader->increment());
    }
    return false;
}

auto WALManager::roll_backward() -> Result<void>
{
    if (!m_reader->record())
        return {};

    if (m_reader->record()->is_commit()) {
        CCO_TRY_CREATE(decremented, m_reader->decrement());
        if (!decremented) {
            ThreePartMessage message;
            message.set_primary("cannot roll back");
            message.set_detail("transaction is empty");
            return Err {message.corruption()};
        }
        CCO_EXPECT_NE(m_reader->record(), std::nullopt);
        CCO_EXPECT_FALSE(m_reader->record()->is_commit());
    }

    for (auto should_continue = true; should_continue; ) {
        CCO_EXPECT_NE(m_reader->record(), std::nullopt);
        const auto record = *m_reader->record();
        CCO_EXPECT_FALSE(record.is_commit());

        const auto update = record.decode();
        CCO_TRY_CREATE(page, m_pool->fetch(update.page_id, true));
        CCO_EXPECT_FALSE(page.has_manager());

        if (page.lsn() >= record.lsn())
            page.undo(update.previous_lsn, update.changes);

        CCO_TRY(m_pool->release(std::move(page)));
        CCO_TRY_STORE(should_continue, m_reader->decrement());
    }
    return {};
}

} // cco
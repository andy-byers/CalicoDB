#include "buffer_pool.h"

#include "cub/common.h"
#include "cub/exception.h"
#include "page/file_header.h"
#include "page/page.h"
#include "file/interface.h"
#include "wal/interface.h"
#include "wal/wal_record.h"

namespace cub {

BufferPool::BufferPool(Parameters param)
    : m_wal_reader {std::move(param.wal_reader)}
    , m_wal_writer {std::move(param.wal_writer)}
    , m_scratch {param.page_size}
    , m_pager {{std::move(param.pool_file), param.page_size, param.frame_count}}
    , m_flushed_lsn {param.flushed_lsn}
    , m_next_lsn {param.flushed_lsn + LSN {1}}
    , m_page_count {param.page_count} {}

/**
 * Determine if the current transaction can be committed.
 *
 * @return True if the current transaction can be committed, i.e. there have been updates since the last commit,
 *         otherwise false
 */
auto BufferPool::can_commit() const -> bool
{
    return m_wal_writer->has_committed() || m_wal_writer->has_pending();
}

auto BufferPool::block_size() const -> Size
{
    return m_wal_writer->block_size();
}

auto BufferPool::allocate(PageType type) -> Page
{
    CUB_EXPECT_TRUE(is_page_type_valid(type));
    auto page = acquire(PID {ROOT_ID_VALUE + m_page_count}, true);
    page.set_type(type);
    m_page_count++;
    return page;
}

auto BufferPool::acquire(PID id, bool is_writable) -> Page
{
    CUB_EXPECT_FALSE(id.is_null());
    auto page = fetch_page(id, is_writable);
    if (is_writable)
        page.enable_tracking(m_scratch.get());
    return page;
}

auto BufferPool::fetch_page(PID id, bool is_writable) -> Page
{
    CUB_EXPECT_FALSE(id.is_null());
    std::lock_guard lock {m_mutex};

    // Propagate exceptions from the Page destructor (likely I/O errors from the WALWriter).
    propagate_page_error();

    const auto try_fetch = [id, is_writable, this]() -> std::optional<Page> {
        if (auto itr = m_pinned.find(id); itr != m_pinned.end()) {
            auto page = itr->second.borrow(this, is_writable);
            m_ref_sum++;
            return page;
        }
        return std::nullopt;
    };
    
    // Frame is already pinned.
    if (auto page = try_fetch())
        return std::move(*page);

    // Get page from cache or disk.
    m_pinned.emplace(id, fetch_frame(id));
    auto page = try_fetch();
    CUB_EXPECT_NE(page, std::nullopt);
    return std::move(*page);
}

auto BufferPool::propagate_page_error() -> void
{
    if (m_error)
        std::rethrow_exception(std::exchange(m_error, {}));
}

auto BufferPool::log_update(Page &page) -> void
{
    page.set_lsn(m_next_lsn);

    const WALRecord::Parameters param {
        page.collect_changes(),
        page.id(),
        page.lsn(),
        m_next_lsn,
    };

    if (const auto lsn = m_wal_writer->append(WALRecord{param}); !lsn.is_null())
        m_flushed_lsn = lsn;

    m_next_lsn++;
}

auto BufferPool::fetch_frame(PID id) -> Frame
{
    const auto try_evict_and_pin = [id, this]() -> std::optional<Frame> {
        if (auto frame = m_cache.evict(m_flushed_lsn)) {
            m_pager.unpin(std::move(*frame));
            frame = m_pager.pin(id);
            return frame;
        }
        return std::nullopt;
    };
    if (auto frame = m_cache.extract(id))
        return std::move(*frame);

    if (auto frame = m_pager.pin(id))
        return std::move(*frame);

    if (auto frame = try_evict_and_pin())
        return std::move(*frame);

    if (!try_flush_wal()) {
        m_pager.unpin(Frame {m_pager.page_size()});
        return *m_pager.pin(id);
    }
    auto frame = try_evict_and_pin();
    CUB_EXPECT_NE(frame, std::nullopt);
    return std::move(*frame);
}

auto BufferPool::on_page_release(Page &page) -> void
{
    // If we have changes, we must be a writer. The shared mutex in Database::Impl should protect us.
    if (page.has_changes())
        log_update(page);

    std::lock_guard lock {m_mutex};
    CUB_EXPECT_GT(m_ref_sum, 0);

    auto itr = m_pinned.find(page.id());
    CUB_EXPECT_NE(itr, m_pinned.end());

    auto &frame = itr->second;
    frame.synchronize(page);

    if (!frame.ref_count()) {
        m_cache.put(std::move(frame));
        m_pinned.erase(itr);
    }
    m_ref_sum--;
}

auto BufferPool::on_page_error() -> void
{
    // This method should only be called from Page::~Page().
    m_error = std::current_exception();
}

auto BufferPool::roll_forward() -> bool
{
    m_wal_reader->reset();

    if (!m_wal_reader->record())
        return false;

    do {
        const auto record = *m_wal_reader->record();

        if (m_next_lsn < record.lsn()) {
            m_flushed_lsn = record.lsn();
            m_next_lsn = m_flushed_lsn;
            m_next_lsn++;
        }
        // Stop at the first commit record.
        if (record.is_commit())
            return true;

        const auto update = record.decode();
        auto page = fetch_page(update.page_id, true);

        if (page.lsn() < record.lsn())
            page.redo_changes(record.lsn(), update.changes);

    } while (m_wal_reader->increment());

    return false;
}

auto BufferPool::roll_backward() -> void
{
    // Make sure the WAL reader is at the end of the log. TODO: Maybe not necessary. If the WAL has junk at the end, this will throw.
    try {
        while (m_wal_reader->increment()) {}
    } catch (const CorruptionError&) {

    }
    CUB_EXPECT_NE(m_wal_reader->record(), std::nullopt);

    // Skip commit record(s) at the end. These can arise from exceptions during commit(), i.e. we write the commit record,
    // but get an I/O error while flushing dirty database pages.
    while (m_wal_reader->record()->is_commit()) {
        if (!m_wal_reader->decrement())
            throw CorruptionError {"WAL contains an empty commit"};
    }

    do {
        const auto record = *m_wal_reader->record();
        CUB_EXPECT_FALSE(record.is_commit());
        const auto update = record.decode();
        auto page = fetch_page(update.page_id, true);

        if (page.lsn() >= record.lsn())
            page.undo_changes(update.previous_lsn, update.changes);

    } while (m_wal_reader->decrement());
}

auto BufferPool::commit() -> void
{
    CUB_EXPECT_TRUE(m_pinned.empty());
    m_wal_writer->append(WALRecord::commit(m_next_lsn++));
    try_flush_wal();
    try_flush();
    m_wal_writer->truncate();
}

auto BufferPool::abort() -> void
{
    CUB_EXPECT_TRUE(m_pinned.empty());
    try {
        try_flush_wal();
    } catch (const IOError &error) {
        // TODO: Ignored for now.
    }
    if (!m_wal_writer->has_committed())
        return;

    // Throw away in-memory updates.
    purge();
    m_wal_reader->reset();
    roll_backward();
    try_flush();
    m_wal_writer->truncate();
    return;
}

auto BufferPool::recover() -> bool
{
    if (!m_wal_writer->has_committed())
        return false;

    bool found_commit {};
    try {
        found_commit = roll_forward();
    } catch (const CorruptionError&) {

    }
    if (!found_commit)
        roll_backward();
    try_flush();
    m_wal_writer->truncate();
    return true;
}

auto BufferPool::try_flush() -> bool
{
    CUB_EXPECT_TRUE(m_pinned.empty());

    if (m_cache.is_empty())
        return false;

    for (; ; ) {
        if (auto frame = m_cache.evict(m_flushed_lsn)) {
            m_pager.unpin(std::move(*frame));
        } else {
            break;
        }
    }
    return m_cache.is_empty();
}

auto BufferPool::try_flush_wal() -> bool
{
    if (const auto lsn = m_wal_writer->flush(); !lsn.is_null()) {
        CUB_EXPECT_EQ(m_next_lsn, lsn + LSN {1});
        m_flushed_lsn = lsn;
        return true;
    }
    return false;
}


auto BufferPool::purge() -> void
{
    const LSN max_lsn {std::numeric_limits<uint32_t>::max()};
    CUB_EXPECT_EQ(m_ref_sum, 0);

    while (!m_cache.is_empty()) {
        auto frame = m_cache.evict(max_lsn);
        CUB_EXPECT_NE(frame, std::nullopt);
        frame->clean();
        m_pager.unpin(std::move(*frame));
    }
}

auto BufferPool::save_header(FileHeader &header) -> void
{
    header.set_flushed_lsn(m_flushed_lsn);
    header.set_page_count(m_page_count);
}

auto BufferPool::load_header(const FileHeader &header) -> void
{
    m_flushed_lsn = header.flushed_lsn();
    m_page_count = header.page_count();
}

} // cub
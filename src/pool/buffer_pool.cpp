
#include "buffer_pool.h"
#include "common.h"
#include "exception.h"
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
    , m_pager {{std::move(param.database_storage), param.page_size, param.frame_count}}
    , m_flushed_lsn {param.flushed_lsn}
    , m_next_lsn {param.flushed_lsn + LSN {1}}
    , m_page_count {param.page_count} {}

auto BufferPool::can_commit() const -> bool
{
    return m_wal_writer->has_committed() || m_wal_writer->has_pending();
}

auto BufferPool::page_count() const -> Size
{
    return m_page_count;
}
    
auto BufferPool::flushed_lsn() const -> LSN
{
    return m_flushed_lsn;
}

auto BufferPool::hit_ratio() const -> double
{
    return m_cache.hit_ratio();
}

auto BufferPool::page_size() const -> Size
{
    return m_pager.page_size();
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
    return *try_fetch();
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

    if (const auto lsn = m_wal_writer->write(WALRecord {param}); !lsn.is_null())
        m_flushed_lsn = lsn;

    m_next_lsn.value++;
}

auto BufferPool::fetch_frame(PID id) -> Frame
{
    if (auto frame = m_cache.extract(id))
        return std::move(*frame);

    if (auto frame = m_pager.pin(id))
        return std::move(*frame);

    if (auto frame = m_cache.evict(m_flushed_lsn)) {
        m_pager.unpin(std::move(*frame));
        frame = m_pager.pin(id);
        return std::move(*frame);
    }

    CUB_EXPECT_TRUE(m_wal_writer->has_pending());
    m_flushed_lsn = m_wal_writer->flush();
    CUB_EXPECT_FALSE(m_flushed_lsn.is_null());
    return fetch_frame(id);
}

auto BufferPool::on_page_release(Page &page) -> void
{
    // TODO: We can definitely reduce the size of most of these critical sections. Work on that!
    std::lock_guard lock {m_mutex};
    CUB_EXPECT_GT(m_ref_sum, 0);

    auto itr = m_pinned.find(page.id());
    CUB_EXPECT_NE(itr, m_pinned.end());

    if (page.has_changes())
        log_update(page);

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
    auto found_commit = false;
    m_wal_reader->reset();

    if (!m_wal_reader->record())
        return false;

    do {
        const auto record = *m_wal_reader->record();

        if (record.payload().is_commit()) {
            CUB_EXPECT_LT(m_flushed_lsn, record.lsn());
            m_flushed_lsn = record.lsn();
            return true;
        }
        const auto update = record.payload().decode();
        auto page = fetch_page(update.page_id, true);

        if (page.lsn() < record.lsn())
            page.redo_changes(record.lsn(), update.changes);

    } while (m_wal_reader->increment());

    return false;
}

auto BufferPool::roll_backward() -> void
{
    while (m_wal_reader->increment());

    if (!m_wal_reader->record())
        return;

    CUB_EXPECT_FALSE(m_wal_reader->record()->is_commit());

    do {
        const auto record = *m_wal_reader->record();
        const auto update = record.payload().decode();
        auto page = fetch_page(update.page_id, true);

        if (page.lsn() >= record.lsn())
            page.undo_changes(update.previous_lsn, update.changes);

    } while (m_wal_reader->decrement());
}

auto BufferPool::commit() -> void
{
    m_wal_writer->write(WALRecord::commit(m_next_lsn++));
    m_flushed_lsn = m_wal_writer->flush();
    CUB_EXPECT_EQ(m_next_lsn, m_flushed_lsn + LSN {1});
    flush();
    m_wal_writer->truncate();
}

auto BufferPool::abort() -> void
{
    try {
        m_wal_writer->flush();
    } catch (const IOError &error) {
        // TODO: Ignored for now.
    }
    if (!m_wal_writer->has_committed())
        return;

    // Throw away in-memory updates.
    purge();
    roll_backward();
    flush();
    m_wal_writer->truncate();
}

auto BufferPool::recover() -> void
{
    if (!m_wal_writer->has_committed())
        return;

    if (!roll_forward())
        roll_backward();
    flush();
    m_wal_writer->truncate();
}

auto BufferPool::flush() -> void
{
    while (!m_cache.is_empty()) {
        if (auto frame = m_cache.evict(m_flushed_lsn)) {
            m_pager.unpin(std::move(*frame));
        } else {
            break;
        }
    }
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

} // cub
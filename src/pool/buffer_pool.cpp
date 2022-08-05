#include "buffer_pool.h"
#include "calico/options.h"
#include "page/file_header.h"
#include "page/page.h"
#include "pager.h"
#include "storage/directory.h"
#include "utils/identifier.h"
#include "utils/logging.h"
#include "wal/wal_manager.h"

namespace cco {

#define POOL_TRY(expr)                          \
    do {                                        \
        auto pool_try_result = (expr);          \
        if (!pool_try_result.has_value()) {     \
            m_status = pool_try_result.error(); \
            return Err {m_status};              \
        }                                       \
    } while (0)

auto BufferPool::open(const Parameters &param) -> Result<std::unique_ptr<IBufferPool>>
{
    auto pool = std::unique_ptr<BufferPool> {new (std::nothrow) BufferPool {param}};
    pool->m_logger->trace("opening");

    if (!pool) {
        ThreePartMessage message;
        message.set_primary("cannot open buffer pool");
        message.set_detail("out of memory");
        return Err {message.system_error()};
    }

    // Open the "data" file.
    const auto mode = Mode::CREATE | Mode::READ_WRITE;
    CCO_TRY_CREATE(file, param.directory.open_file(DATA_NAME, mode, param.permissions));

    CCO_TRY_STORE(pool->m_pager, Pager::open({
        std::move(file),
        param.flushed_lsn,
        param.page_size,
        param.frame_count,
    }));
    if (!param.use_xact)
        return pool;

    CCO_TRY_STORE(pool->m_wal, WALManager::open({
        pool.get(),
        param.directory,
        create_sink(),
        param.page_size,
        param.flushed_lsn,
    }));
    return pool;
}

BufferPool::BufferPool(const Parameters &param)
    : m_logger {create_logger(param.log_sink, "pool")},
//      m_ref_counts(param.frame_count),
      m_scratch {param.page_size},
      m_page_count {param.page_count},
      m_uses_xact {param.use_xact}
{}

BufferPool::~BufferPool()
{
    m_wal->teardown();
}

auto BufferPool::can_commit() const -> bool
{
    if (m_uses_xact)
        return m_wal->has_pending();
    return m_dirty_count > 0;
}

auto BufferPool::page_size() const -> Size
{
    return m_pager->page_size();
}

auto BufferPool::pin_frame(PageId id) -> Result<void>
{
    if (m_cache.contains(id))
        return {};

    if (!m_pager->available())
        CCO_TRY(try_evict_frame());

    // Pager will allocate a new temporary frame if there aren't any more available.
    return m_pager->pin(id)
        .and_then([id, this](Frame frame) -> Result<void> {
            m_cache.put(id, std::move(frame));
            return {};
        });
}

auto BufferPool::flush() -> Result<void>
{
    const auto flush_frames = [this](auto begin, auto end) -> Result<void> {
        for (auto itr = begin; itr != end; ++itr) {
            if (itr->second.is_dirty()) {
                CCO_TRY(m_pager->clean(itr->second));
                m_dirty_count--;
            }
        }
        return {};
    };
    CCO_EXPECT_EQ(m_ref_sum, 0);
    m_logger->trace("flushing");

    if (m_dirty_count > 0) {
        m_logger->info("trying to flush {} dirty frames", m_dirty_count);
        CCO_TRY(flush_frames(m_cache.cold_begin(), m_cache.cold_end()));
        CCO_TRY(flush_frames(m_cache.hot_begin(), m_cache.hot_end()));

        if (m_dirty_count > 0) {
            LogMessage message {*m_logger};
            message.set_primary("cannot flush cache");
            message.set_detail("{} dirty frame(s) left", m_dirty_count);
            return Err {message.system_error()};
        }
        CCO_TRY(m_pager->sync());
        m_logger->info("cache was flushed");
    }
    return {};
}

auto BufferPool::commit() -> Result<void>
{
    CCO_EXPECT_TRUE(can_commit());
    if (!m_status.is_ok())
        return Err {m_status};

    // All we need to do is write a commit record and start a new WAL segment.
    if (m_uses_xact)
        POOL_TRY(m_wal->commit());

    return {};
}

auto BufferPool::abort() -> Result<void>
{
    CCO_EXPECT_TRUE(can_commit());

    if (!m_uses_xact) {
        ThreePartMessage message;
        message.set_primary("cannot abort");
        message.set_detail("not supported");
        message.set_hint("transactions are disabled");
        return Err {message.logic_error()};
    }

    CCO_TRY(m_wal->abort());
    CCO_TRY(flush());
    clear_error();
    return {};
}

auto BufferPool::recover() -> Result<void>
{
    return m_wal->recover();
}

auto BufferPool::flushed_lsn() const -> SequenceNumber
{
    return m_pager->flushed_lsn();
}

auto BufferPool::try_evict_frame() -> Result<bool>
{
    const auto find_for_eviction = [this](auto begin, auto end) {
        for (auto itr = begin; itr != end; ++itr) {
            const auto limit = m_uses_xact && m_wal ? m_wal->flushed_lsn() : SequenceNumber::null();
            const auto is_unpinned = itr->second.ref_count() == 0;
            const auto is_protected = itr->second.page_lsn() <= limit;
            if (is_unpinned && is_protected)
                return itr->second.page_id();
        }
        return PageId::null();
    };

    const auto perform_eviction = [this](PageId id) -> Result<bool> {
        auto frame = *m_cache.extract(id);

        // XXX: We alter the dirty count before we unpin the frame. We transfer ownership, so we lose the opportunity
        //      to retry anyway.
        m_dirty_count -= frame.is_dirty();
        CCO_TRY(m_pager->unpin(std::move(frame)));

























        return true;
    };

    auto id = find_for_eviction(m_cache.cold_begin(), m_cache.cold_end());
    if (!id.is_null()) {
        CCO_TRY(perform_eviction(id));
        return true;
    }
    id = find_for_eviction(m_cache.hot_begin(), m_cache.hot_end());
    if (!id.is_null()) {
        CCO_TRY(perform_eviction(id));
        return true;
    }
    return false;
}

auto BufferPool::on_release(Page &page) -> void
{
    std::lock_guard lock {m_mutex};
    if (auto r = do_release(page); !r.has_value()) {
        m_logger->error(r.error().what());
        m_status = r.error();
    }
}

auto BufferPool::release(Page page) -> Result<void>
{
    std::lock_guard lock {m_mutex};
    if (auto r = do_release(page); !r.has_value()) {
        m_logger->error(r.error().what());
        m_status = r.error();
        return Err {m_status};
    }
    return {};
}

auto BufferPool::do_release(Page &page) -> Result<void>
{
    // This function needs external synchronization!
    CCO_EXPECT_GT(m_ref_sum, 0);

//    const auto index = page.id().as_index();
//    CCO_EXPECT_GT(m_ref_counts[index], 0);
//    m_ref_counts[index]--;

    auto reference = m_cache.get(page.id());
    CCO_EXPECT_NE(reference, std::nullopt);
    auto &frame = reference->get();
    const auto became_dirty = !frame.is_dirty() && page.is_dirty();

    m_dirty_count += became_dirty;
    frame.synchronize(page);
    m_ref_sum--;

    // Appending a record to the WAL is the only thing that can fail in this method. If we already have an error,
    // we'll skip this step, so we cannot encounter another error.
    if (page.has_manager()) {
        CCO_EXPECT_TRUE(m_uses_xact);
        if (m_status.is_ok())
            return m_wal->append(page);
        m_wal->discard(page);
    }
    return {};
}

auto BufferPool::save_header(FileHeaderWriter &header) -> void
{
    m_logger->trace("saving header fields");
    if (m_uses_xact)
        m_wal->save_header(header);
    m_pager->save_header(header);
    header.set_page_count(m_page_count);
}

auto BufferPool::load_header(const FileHeaderReader &header) -> void
{
    m_logger->trace("loading header fields");
    if (m_uses_xact)
        m_wal->load_header(header);
    m_pager->load_header(header);
    m_page_count = header.page_count();
}

auto BufferPool::close() -> Result<void>
{
    m_logger->trace("closing");

    Result<void> wr;
    if (m_uses_xact) {
        wr = m_wal->close();
        if (!wr.has_value()) {
            m_logger->error("cannot close WAL");
            m_logger->error("(reason) {}", wr.error().what());
            m_status = wr.error();
        }
    }
    auto pr = m_pager->close();
    if (!pr.has_value()) {
        m_logger->error("cannot close pager");
        m_logger->error("(reason) {}", pr.error().what());
        m_status = pr.error();
    }
    return !wr ? wr : pr;
}

auto BufferPool::allocate() -> Result<Page>
{
    return acquire(PageId {ROOT_ID_VALUE + m_page_count}, true)
        .and_then([this](Page page) -> Result<Page> {
            m_page_count++;
            return page;
        });
}

auto BufferPool::acquire(PageId id, bool is_writable) -> Result<Page>
{
    CCO_EXPECT_FALSE(id.is_null());
    std::lock_guard lock {m_mutex};

    if (!m_status.is_ok())
        return Err {m_status};

    return fetch(id, is_writable)
        .and_then([is_writable, this](Page page) -> Result<Page> {
            if (is_writable && m_uses_xact)
                m_wal->track(page);
            return page;
        })
        .or_else([this](const Status &status) -> Result<Page> {
            m_logger->error(status.what());
            // We should only enter the error state if data has been altered during this transaction. Otherwise, we
            // can just return from whatever operation we are in immediately (releasing resources as needed).
            if (has_updates())
                m_status = status;
            return Err {status};
        });
}

auto BufferPool::has_updates() const -> bool
{
    if (m_uses_xact)
        return m_wal->has_pending();
    return m_dirty_count > 0;
}

auto BufferPool::fetch(PageId id, bool is_writable) -> Result<Page>
{
    const auto do_acquire = [this, is_writable](PageCache::Reference frame) {
        auto page = frame.get().borrow(this, is_writable);
        m_ref_sum++;
        return page;
    };
    CCO_EXPECT_FALSE(id.is_null());

    if (auto reference = m_cache.get(id))
        return do_acquire(*reference);

    return pin_frame(id)
        .and_then([id, this, do_acquire]() -> Result<Page> {
            auto frame = m_cache.get(id);
            CCO_EXPECT_NE(frame, std::nullopt);
            return do_acquire(*frame);
        });
}

#undef POOL_TRY

} // namespace cco

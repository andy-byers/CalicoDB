#include "buffer_pool.h"
#include "pager.h"
#include "calico/options.h"
#include "page/file_header.h"
#include "page/page.h"
#include "storage/directory.h"
#include "utils/identifier.h"
#include "utils/logging.h"
#include "wal/wal_manager.h"

namespace cco {

using namespace page;
using namespace utils;

#define POOL_TRY(expr) \
    do {                  \
        auto pool_try_result = (expr); \
        if (!pool_try_result.has_value()) { \
            m_status = pool_try_result.error(); \
            return Err {m_status}; \
        } \
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

BufferPool::BufferPool(const Parameters &param):
      m_logger {utils::create_logger(param.log_sink, "pool")},
      m_scratch {param.page_size},
      m_page_count {param.page_count},
      m_use_xact {param.use_xact} {}

auto BufferPool::can_commit() const -> bool
{
    return m_dirty_count > 0;
}

auto BufferPool::page_size() const -> Size
{
    return m_pager->page_size();
}

auto BufferPool::pin_frame(PID id) -> Result<void>
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
    CCO_EXPECT_EQ(m_ref_sum, 0);
    m_logger->trace("flushing");

    if (!m_cache.is_empty()) {
        m_logger->info("trying to flush {} frames", m_cache.size());

        while (!m_cache.is_empty()) {
            CCO_TRY_CREATE(was_evicted, try_evict_frame());
            if (!was_evicted)
                break;
        }

        if (!m_cache.is_empty()) {
            LogMessage message {*m_logger};
            message.set_primary("cannot flush cache");
            message.set_detail("{} frames are left", m_cache.size());
            message.log(spdlog::level::info);
            return Err {message.system_error()};
        }
        m_logger->info("cache was flushed");
    }
    return {};
}

auto BufferPool::commit() -> Result<void>
{
    if (!m_status.is_ok())
        return Err {m_status};

    if (!m_use_xact)
        return flush();

    // Flush the WAL tail buffer, followed by the data pages.
    POOL_TRY(m_wal->commit());
    POOL_TRY(flush());
    POOL_TRY(m_wal->truncate());
    return {};
}

auto BufferPool::abort() -> Result<void>
{
    if (!m_use_xact) {
        ThreePartMessage message;
        message.set_primary("cannot abort");
        message.set_detail("not supported");
        message.set_hint("transactions are disabled");
        return Err {message.logic_error()};
    }

    purge();
    CCO_TRY(m_wal->abort());
    CCO_TRY(flush());
    CCO_TRY(m_wal->truncate());
    clear_error();
    return {};
}

auto BufferPool::recover() -> Result<void>
{
    return m_wal->recover();
}

auto BufferPool::try_evict_frame() -> Result<bool>
{
    for (Index i {}; i < m_cache.size(); ++i) {
        auto frame = m_cache.evict();
        CCO_EXPECT_NE(frame, std::nullopt);
        const auto limit = m_use_xact ? m_wal->flushed_lsn() : LSN::null();
        const auto is_unpinned = frame->ref_count() == 0;
        const auto is_writable = frame->page_lsn() <= limit;
        if (is_unpinned && is_writable) {
            m_dirty_count -= frame->is_dirty();
            CCO_TRY(m_pager->unpin(std::move(*frame)));
            return true;
        }
        const auto id = frame->page_id();
        m_cache.put(id, std::move(*frame));
    }
    return false;
}

auto BufferPool::on_release(page::Page &page) -> void
{
    std::lock_guard lock {m_mutex};
    if (auto result = do_release(page); !result.has_value()) {
        m_logger->error(result.error().what());
        m_status = result.error();
    }
}

auto BufferPool::release(Page page) -> Result<void>
{
    std::lock_guard lock {m_mutex};
    if (auto result = do_release(page); !result.has_value()) {
        m_logger->error(result.error().what());
        m_status = result.error();
        return Err {m_status};
    }
    return {};
}

auto BufferPool::do_release(page::Page &page) -> Result<void>
{
    // This function needs external synchronization!
    CCO_EXPECT_GT(m_ref_sum, 0);

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
        if (m_status.is_ok())
            return m_wal->append(page);
        m_wal->discard(page);
    }
    return {};
}

auto BufferPool::purge() -> void
{
    CCO_EXPECT_EQ(m_ref_sum, 0);
    m_logger->trace("purging page cache");

    while (!m_cache.is_empty()) {
        auto frame = m_cache.evict();
        CCO_EXPECT_NE(frame, std::nullopt);
        m_dirty_count -= frame->is_dirty();
        m_pager->discard(std::move(*frame));
    }
}

auto BufferPool::save_header(FileHeaderWriter &header) -> void
{
    m_logger->trace("saving header fields");
    m_wal->save_header(header);
    header.set_page_count(m_page_count);
}

auto BufferPool::load_header(const FileHeaderReader &header) -> void
{
    m_logger->trace("loading header fields");
    m_wal->load_header(header);
    m_page_count = header.page_count();
}

auto BufferPool::close() -> Result<void>
{
    m_logger->trace("closing");
    const auto pr = m_pager->close();
    if (!pr.has_value()) {
        m_logger->error("cannot close pager");
        m_logger->error("(reason) {}", pr.error().what());
    }
    const auto wr = m_wal->close();
    if (!wr.has_value()) {
        m_logger->error("cannot close WAL");
        m_logger->error("(reason) {}", wr.error().what());
    }
    return !pr.has_value() ? pr : wr;
}

auto BufferPool::allocate() -> Result<Page>
{
    return acquire(PID {ROOT_ID_VALUE + m_page_count}, true)
        .and_then([this](Page page) -> Result<Page> {
            m_page_count++;
            return page;
        });
}

auto BufferPool::acquire(PID id, bool is_writable) -> Result<Page>
{
    CCO_EXPECT_FALSE(id.is_null());
    std::lock_guard lock {m_mutex};

    if (!m_status.is_ok())
        return Err {m_status};

    return fetch(id, is_writable)
        .and_then([is_writable, this](Page page) -> Result<Page> {
            if (is_writable && m_use_xact)
                m_wal->track(page);
            return page;
        })
        .or_else([is_writable, this](const Status &status) -> Result<Page> {
            m_logger->error(status.what());
            // We should only enter the error state if data has been altered during this transaction. Otherwise, we
            // can just return from whatever operation we are in immediately (releasing resources as needed).
            if (is_writable && has_updates())
                m_status = status;
            return Err {status};
        });
}

auto BufferPool::has_updates() const -> bool
{
    if (m_use_xact)
        return m_wal->has_records();
    return m_dirty_count > 0;
}

auto BufferPool::fetch(PID id, bool is_writable) -> Result<Page>
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

} // cco

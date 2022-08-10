#include "buffer_pool.h"
#include "calico/options.h"
#include "page/file_header.h"
#include "page/page.h"
#include "pager.h"
#include "storage/disk.h"
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

auto BufferPool::open(const Parameters &param) -> Result<std::unique_ptr<BufferPool>>
{
    auto pool = std::unique_ptr<BufferPool> {new (std::nothrow) BufferPool {param}};
    pool->m_logger->trace("opening");

    if (!pool) {
        ThreePartMessage message;
        message.set_primary("cannot open buffer pool");
        message.set_detail("out of memory");
        return Err {message.system_error()};
    }

    // Open the database file.
    RandomAccessEditor *file {};
    auto s = param.storage.open_random_access_editor(DATA_FILENAME, &file);

    CCO_TRY_STORE(pool->m_pager, Pager::open(
        std::unique_ptr<RandomAccessEditor> {file},
        param.page_size,
        param.frame_count
    ));
    return pool;
}

BufferPool::BufferPool(const Parameters &param)
    : m_logger {create_logger(param.log_sink, "pool")},
      m_scratch {param.page_size},
      m_wal {param.wal}
{}

BufferPool::~BufferPool()
{
//    flush();
}

auto BufferPool::page_count() const -> Size
{
    return m_pager->page_count();
}

auto BufferPool::pin_frame(PageId id) -> Result<void>
{
    if (m_cache.contains(id))
        return {};

    if (!m_pager->available())
        CCO_TRY(try_evict_frame());

    // Pager will allocate a new temporary frame if there aren't any more available. TODO: Not true anymore!
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

auto BufferPool::flushed_lsn() const -> SequenceNumber
{
    return m_pager->flushed_lsn();
}

auto BufferPool::try_evict_frame() -> Result<bool>
{
    const auto find_for_eviction = [this](auto begin, auto end) {
        for (auto itr = begin; itr != end; ++itr) {
            const auto limit = SequenceNumber::null();(void)this; // TODO
//            const auto limit = m_uses_xact && m_wal ? m_wal->flushed_lsn() : SequenceNumber::null();
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

    auto itr = m_cache.get(page.id());
    CCO_EXPECT_NE(reference, std::nullopt);
    auto &frame = reference->get();
    const auto became_dirty = !frame.is_dirty() && page.is_dirty();

    m_dirty_count += became_dirty;
    frame.synchronize(page);
    m_ref_sum--;
    return {};
}

auto BufferPool::update_page(Page &page) -> Result<void>
{
    // This function needs external synchronization!
    CCO_EXPECT_GT(m_ref_sum, 0);

    auto itr = m_cache.get(page.id());
    CCO_EXPECT_NE(reference, std::nullopt);
    auto &frame = reference->get();
    const auto became_dirty = !frame.is_dirty() && page.is_dirty();

    m_dirty_count += became_dirty;
    frame.synchronize(page);
    m_ref_sum--;
    return {};
}

auto BufferPool::save_header(FileHeaderWriter &header) -> void
{
    m_logger->trace("saving header fields");
    m_pager->save_header(header);
    header.set_page_count(m_pager->page_count());
}

auto BufferPool::load_header(const FileHeaderReader &header) -> void
{
    m_logger->trace("loading header fields");
    m_pager->load_header(header);
//    m_pager->set_page_count(header.page_count());
}

auto BufferPool::close() -> Result<void>
{
    m_logger->trace("closing");
    auto pr = m_pager->close();
    if (!pr.has_value()) {
        m_logger->error("cannot close pager");
        m_logger->error("(reason) {}", pr.error().what());
        m_status = pr.error();
    }
    return pr;
}

auto BufferPool::allocate() -> Result<Page>
{
    return acquire(PageId {ROOT_ID_VALUE + m_pager->page_count()}, true);
}

auto BufferPool::acquire(PageId id, bool is_writable) -> Result<Page>
{
    CCO_EXPECT_FALSE(id.is_null());
    std::lock_guard lock {m_mutex};

    if (!m_status.is_ok())
        return Err {m_status};

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
        })
        .or_else([this](const Status &status) -> Result<Page> {
            m_logger->error(status.what());
            m_status = status;
            return Err {status};
        });
}

#undef POOL_TRY

} // namespace cco

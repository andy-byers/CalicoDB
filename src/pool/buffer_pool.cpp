#include "buffer_pool.h"
#include "calico/options.h"
#include "page/file_header.h"
#include "page/page.h"
#include "storage/directory.h"
#include "utils/logging.h"

namespace calico {

using namespace page;
using namespace utils;

auto BufferPool::open(const Parameters &param) -> Result<std::unique_ptr<IBufferPool>>
{
    std::unique_ptr<IFile> file;
    const auto mode = Mode::CREATE | Mode::READ_WRITE;
    CCO_TRY_ASSIGN(file, param.directory.open_file(DATA_NAME, mode, param.permissions));
    return std::unique_ptr<IBufferPool> {new BufferPool {std::move(file), param}};
}

BufferPool::BufferPool(std::unique_ptr<IFile> file, const Parameters &param):
      m_file {std::move(file)},
      m_logger {utils::create_logger(param.log_sink, "BufferPool")},
      m_scratch {param.page_size},
      m_pager {{m_file->open_reader(), m_file->open_writer(), param.log_sink, param.page_size, param.frame_count}},
      m_page_count {param.page_count}
{
    m_logger->trace("constructing BufferPool object");
}

auto BufferPool::pin_frame(PID id) -> Result<void>
{
    if (m_cache.contains(id))
        return {};

    if (!m_pager.available())
        try_evict_frame();

    return m_pager.pin(id)
        .and_then([id, this](Frame frame) -> Result<void> {
            m_cache.put(id, std::move(frame));
            return {};
        });
}

auto BufferPool::flush() -> Result<void>
{
    CALICO_EXPECT_EQ(m_ref_sum, 0);

    if (!m_cache.is_empty()) {
        m_logger->trace("trying to flush {} frames", m_cache.size());
        for (Index i {}, n {m_cache.size()}; i < n; ++i) {
            auto was_evicted = try_evict_frame();
            if (!was_evicted)
                return was_evicted;
        }
        if (!m_cache.is_empty()) {
            // TODO: More logging here?
            m_logger->trace("{} frames are left", m_cache.size());
            return ErrorResult {Error::not_found()};
        }
    }
    m_logger->trace("buffer pool is empty");
    return {};
}

auto BufferPool::try_evict_frame() -> Result<void>
{
    for (Index i {}; i < m_cache.size(); ++i) {
        auto frame = m_cache.evict();
        CALICO_EXPECT_NE(frame, std::nullopt);
        if (frame->ref_count() == 0) {
            m_dirty_count -= frame->is_dirty();
            return m_pager.unpin(std::move(*frame));
        }
        const auto id = frame->page_id();
        m_cache.put(id, std::move(*frame));
    }
    return ErrorResult {Error::not_found()};
}

auto BufferPool::on_release(page::Page &page) -> void
{
    std::lock_guard lock {m_mutex};
    if (auto was_released = do_release(page); !was_released.has_value())
        m_errors.emplace_back(was_released.error());
}

auto BufferPool::release(Page page) -> Result<void>
{
    std::lock_guard lock {m_mutex};
    return do_release(page);
}

auto BufferPool::do_release(page::Page &page) -> Result<void>
{
    // This function needs external synchronization!
    CALICO_EXPECT_GT(m_ref_sum, 0);

    // TODO: Push updates to the WAL here, which can fail. If it fails, we push the error object to the error stack.

    auto reference = m_cache.get(page.id());
    CALICO_EXPECT_NE(reference, std::nullopt);
    auto &frame = reference->get();

    m_dirty_count += !frame.is_dirty() && page.is_dirty();
    frame.synchronize(page);
    m_ref_sum--;
    return {}; // Result from WAL.
}

auto BufferPool::purge() -> Result<void>
{
    CALICO_EXPECT_EQ(m_ref_sum, 0);
    m_logger->trace("purging page cache");

    while (!m_cache.is_empty()) {
        auto frame = m_cache.evict();
        CALICO_EXPECT_NE(frame, std::nullopt);
        m_dirty_count -= frame->is_dirty();
        if (frame->ref_count())
            frame->purge();

        if (auto unpinned = m_pager.discard(std::move(*frame)); !unpinned)
            return unpinned;
    }
    return {};
}

auto BufferPool::save_header(FileHeader &header) -> void
{
    m_logger->trace("saving header fields");
    header.set_page_count(m_page_count);
}

auto BufferPool::load_header(const FileHeader &header) -> void
{
    m_logger->trace("loading header fields");
    m_page_count = header.page_count();
}

auto BufferPool::close() -> Result<void>
{
    return {};
}

auto BufferPool::allocate() -> Result<Page>
{
    return acquire(PID {ROOT_ID_VALUE + m_page_count}, true)
        .and_then([this](Page page) -> Result<Page> {
            m_page_count++;
            return page;
        });
}

auto BufferPool::acquire(PID id, bool is_writable) -> Result<page::Page>
{
    CALICO_EXPECT_FALSE(id.is_null());
    std::lock_guard lock {m_mutex};

    if (auto reference = m_cache.get(id)) {
        auto page = reference->get().borrow(this, is_writable);
        m_ref_sum++;
        return page;
    }

    return pin_frame(id)
        .map([id, is_writable, this] {
            auto reference = m_cache.get(id);
            CALICO_EXPECT_NE(reference, std::nullopt);
            auto page = reference->get().borrow(this, is_writable);
            m_ref_sum++;
            return page;
        });
}

} // calico

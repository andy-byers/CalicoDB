#include "buffer_pool.h"
#include "pager.h"
#include "calico/options.h"
#include "page/file_header.h"
#include "page/page.h"
#include "storage/directory.h"
#include "utils/identifier.h"
#include "utils/logging.h"
#include "wal/wal_writer.h"

namespace cco {

using namespace page;
using namespace utils;

auto BufferPool::open(const Parameters &param) -> Result<std::unique_ptr<IBufferPool>>
{
    const auto mode = Mode::CREATE | Mode::READ_WRITE;
    CCO_TRY_CREATE(file, param.directory.open_file(DATA_NAME, mode, param.permissions));
    CCO_TRY_CREATE(pager, Pager::open({file->open_reader(), file->open_writer(), param.log_sink, param.page_size, param.frame_count}));

    std::unique_ptr<IWALReader> wal_reader;
    std::unique_ptr<IWALWriter> wal_writer;
    if (param.use_xact) {
//        CCO_TRY_STORE(wal_reader, WALReader::open({param.directory, param.log_sink, param.page_size}));
        CCO_TRY_STORE(wal_writer, WALWriter::open({param.directory, param.log_sink, param.page_size}));
    }
    return std::unique_ptr<IBufferPool> {new BufferPool {{std::move(file), std::move(pager), /*std::move(wal_reader),*/ std::move(wal_writer)}, param}};
}

BufferPool::BufferPool(State state, const Parameters &param):
      m_file {std::move(state.file)},
      m_pager {std::move(state.pager)},
//      m_wal_reader {std::move(state.wal_reader)},
      m_wal_writer {std::move(state.wal_writer)},
      m_logger {utils::create_logger(param.log_sink, "BufferPool")},
      m_scratch {param.page_size},
      m_page_count {param.page_count},
      m_use_xact {param.use_xact}
{
    m_logger->trace("opening");
}

BufferPool::~BufferPool()
{
    m_logger->trace("closing");
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

    if (m_use_xact)
        CCO_TRY(m_wal_writer->flush());

    if (!m_cache.is_empty()) {
        m_logger->info("trying to flush {} frames", m_cache.size());

        for (Index i {}, n {m_cache.size()}; i < n; ++i) {
            CCO_TRY_CREATE(was_evicted, try_evict_frame());
            if (!was_evicted)
                break;
        }

        if (!m_cache.is_empty()) {
            LogMessage message {*m_logger};
            message.set_primary("cannot flush cache");
            message.set_detail("{} frames are left", m_cache.size());
            message.set_hint("flush the WAL and try again");
            return Err {message.not_found(spdlog::level::info)};
        }
        m_logger->info("cache was flushed");
    }
    return {};
}

auto BufferPool::try_evict_frame() -> Result<bool>
{
    for (Index i {}; i < m_cache.size(); ++i) {
        auto frame = m_cache.evict();
        CCO_EXPECT_NE(frame, std::nullopt);
        const auto limit = m_use_xact ? m_wal_writer->flushed_lsn() : LSN::null();
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
    CCO_EXPECT_GT(m_ref_sum, 0);

    auto reference = m_cache.get(page.id());
    CCO_EXPECT_NE(reference, std::nullopt);
    auto &frame = reference->get();

    m_dirty_count += !frame.is_dirty() && page.is_dirty();
    frame.synchronize(page);
    m_ref_sum--;

    if (m_use_xact) {
        page.set_lsn(m_next_lsn++);
        return m_wal_writer->append(page);
    }
    return {};
}

auto BufferPool::purge() -> Result<void>
{
    CCO_EXPECT_EQ(m_ref_sum, 0);
    m_logger->trace("purging page cache");

    while (!m_cache.is_empty()) {
        auto frame = m_cache.evict();
        CCO_EXPECT_NE(frame, std::nullopt);
        m_dirty_count -= frame->is_dirty();
        if (frame->ref_count())
            frame->purge();

        if (auto unpinned = m_pager->discard(std::move(*frame)); !unpinned)
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
    CCO_EXPECT_FALSE(id.is_null());
    std::lock_guard lock {m_mutex};

    if (auto reference = m_cache.get(id)) {
        auto page = reference->get().borrow(this, is_writable);
        m_ref_sum++;
        return page;
    }

    return pin_frame(id)
        .map([id, is_writable, this] {
            auto reference = m_cache.get(id);
            CCO_EXPECT_NE(reference, std::nullopt);
            auto page = reference->get().borrow(this, is_writable);
            m_ref_sum++;
            return page;
        });
}

} // calico

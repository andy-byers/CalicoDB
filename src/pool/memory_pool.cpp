#include "memory_pool.h"
#include "frame.h"
#include "page/file_header.h"
#include "page/page.h"
#include "utils/logging.h"

namespace cco {

using namespace page;
using namespace utils;

MemoryPool::MemoryPool(Size page_size, bool /* use_transactions */, spdlog::sink_ptr log_sink):
      m_scratch {page_size},
      m_logger {utils::create_logger(std::move(log_sink), "MemoryPool")},
      m_page_size {page_size}
{
    m_logger->trace("opening");
}

auto MemoryPool::page_count() const -> Size
{
    return m_frames.size();
}

auto MemoryPool::save_header(page::FileHeader &header) -> void
{
    header.set_page_count(page_count());
}

auto MemoryPool::load_header(const page::FileHeader &header) -> void
{
    while (page_count() > header.page_count())
        m_frames.pop_back();

    while (page_count() < header.page_count())
        m_frames.emplace_back(m_page_size);
}

auto MemoryPool::close() -> Result<void>
{
    m_logger->trace("closing");
    return {};
}

auto MemoryPool::allocate() -> Result<Page>
{
    return acquire_aux(PID {ROOT_ID_VALUE + page_count()}, true);
}

auto MemoryPool::acquire(PID id, bool is_writable) -> Result<Page>
{
    return acquire_aux(id, is_writable);
}

auto MemoryPool::fetch(PID id, bool is_writable) -> Result<page::Page>
{
    // TODO
    return acquire(id, is_writable);
}

auto MemoryPool::acquire_aux(PID id, bool is_writable) -> Page
{
    CCO_EXPECT_FALSE(id.is_null());
    while (page_count() <= id.as_index()) {
        m_frames.emplace_back(m_page_size);
        m_frames.back().reset(PID::from_index(m_frames.size() - 1));
    }
    return m_frames[id.as_index()].borrow(this, is_writable);
}

auto MemoryPool::release(Page page) -> Result<void>
{
    std::lock_guard lock {m_mutex};
    do_release(page);
    return {};
}

auto MemoryPool::on_release(page::Page &page) -> void
{
    std::lock_guard lock {m_mutex};
    do_release(page);
}

auto MemoryPool::do_release(page::Page &page) -> void
{
    const auto index = page.id().as_index();
    CCO_EXPECT_LT(index, m_frames.size());
    m_frames[index].synchronize(page);
}

auto MemoryPool::purge() -> void
{
    m_logger->trace("purging cache");
    for (auto &frame: m_frames) {
        if (frame.ref_count())
            frame.purge();
    }
}

} // calico
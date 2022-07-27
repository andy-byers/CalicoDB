#include "memory_pool.h"
#include "frame.h"
#include "page/file_header.h"
#include "page/page.h"
#include "utils/logging.h"

namespace cco {


MemoryPool::MemoryPool(Size page_size, bool use_xact):
      m_tracker {page_size},
      m_scratch {page_size},
      m_page_size {page_size},
      m_uses_xact {use_xact} {}

auto MemoryPool::page_count() const -> Size
{
    return m_frames.size();
}

auto MemoryPool::save_header(FileHeaderWriter &header) -> void
{
    header.set_page_count(page_count());
}

auto MemoryPool::load_header(const FileHeaderReader &header) -> void
{
    while (page_count() > header.page_count())
        m_frames.pop_back();

    while (page_count() < header.page_count())
        m_frames.emplace_back(m_page_size);
}

auto MemoryPool::close() -> Result<void>
{
    return {};
}

auto MemoryPool::allocate() -> Result<Page>
{
    return acquire(PID {ROOT_ID_VALUE + page_count()}, true);
}

auto MemoryPool::acquire(PID id, bool is_writable) -> Result<Page>
{
    CCO_TRY_CREATE(page, fetch(id, is_writable));
    if (m_uses_xact && is_writable)
        m_tracker.track(page);
    return page;
}

auto MemoryPool::fetch(PID id, bool is_writable) -> Result<Page>
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

auto MemoryPool::on_release(Page &page) -> void
{
    std::lock_guard lock {m_mutex};
    do_release(page);
}

auto MemoryPool::do_release(Page &page) -> void
{
    const auto index = page.id().as_index();
    CCO_EXPECT_LT(index, m_frames.size());
    if (page.has_manager()) {
        for (const auto &change: m_tracker.collect(page, LSN::null()).changes)
            m_stack.emplace_back(UndoInfo {btos(change.before), page.id(), change.offset});
    }
    m_frames[index].synchronize(page);
}

auto MemoryPool::commit() -> Result<void>
{
    if (can_commit()) {
        m_stack.clear();
    } else if (m_uses_xact) {
        ThreePartMessage message;
        message.set_primary("cannot commit");
        message.set_detail("transaction is empty");
        return Err {message.logic_error()};
    }
    return {};
}

auto MemoryPool::abort() -> Result<void>
{
    if (!m_uses_xact) {
        ThreePartMessage message;
        message.set_primary("cannot abort");
        message.set_detail("not supported");
        message.set_hint("transactions are disabled");
        return Err {message.logic_error()};
    }

    if (!can_commit()) {
        ThreePartMessage message;
        message.set_primary("cannot abort");
        message.set_detail("transaction is empty");
        return Err {message.logic_error()};
    }

    while (can_commit()) {
        const auto [before, id, offset] = std::move(m_stack.back());
        m_stack.pop_back();
        CCO_TRY_CREATE(page, fetch(id, true));
        mem_copy(page.bytes(offset), stob(before));
    }
    return {};
}

} // cco
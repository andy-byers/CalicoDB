#include "in_memory.h"

#include "frame.h"
#include "page/file_header.h"
#include "page/page.h"

namespace cub {

auto InMemory::allocate(PageType type) -> Page
{
    CUB_EXPECT_TRUE(is_page_type_valid(type));
    auto page = acquire(PID {ROOT_ID_VALUE + page_count()}, true);
    page.set_type(type);
    return page;
}

auto InMemory::acquire(PID id, bool is_writable) -> Page
{
    CUB_EXPECT_FALSE(id.is_null());
    propagate_page_error();

    while (page_count() <= id.as_index()) {
        m_frames.emplace_back(m_page_size);
        m_frames.back().reset(PID::from_index(m_frames.size() - 1));
    }
    auto &frame = m_frames[id.as_index()];
    auto page = frame.borrow(this, is_writable);
    if (m_uses_transactions && is_writable)
        page.enable_tracking(m_scratch.get());
    return page;
}

auto InMemory::commit() -> void
{
    if (can_commit())
        m_stack.clear();
}

auto InMemory::abort() -> void
{
    while (can_commit()) {
        const auto [before, id, offset] = std::move(m_stack.back());
        m_stack.pop_back();
        auto page = acquire(id, true);
        mem_copy(page.raw_data().range(offset), stob(before));
    }
}

auto InMemory::on_page_release(Page &page) -> void
{
    std::lock_guard lock {m_mutex};
    const auto index = page.id().as_index();
    CUB_EXPECT_LT(index, m_frames.size());
    m_frames[index].synchronize(page);
    for (const auto &change: page.collect_changes())
        m_stack.emplace_back(UndoInfo {btos(change.before), page.id(), change.offset});
}

auto InMemory::on_page_error() -> void
{
    // This method should only be called from Page::~Page().
    m_error = std::current_exception();
}

auto InMemory::propagate_page_error() -> void
{
    if (m_error)
        std::rethrow_exception(std::exchange(m_error, {}));
}

auto InMemory::save_header(FileHeader &header) -> void
{
    header.set_page_count(page_count());
}

auto InMemory::load_header(const FileHeader &header) -> void
{
    while (page_count() > header.page_count())
        m_frames.pop_back();

    while (page_count() < header.page_count())
        m_frames.emplace_back(m_page_size);
}

} // cub
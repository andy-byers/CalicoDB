
#include "in_memory.h"
#include "page/page.h"
#include "utils/types.h"

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
    const auto n = page_count();
    if (n < id.value)
        m_data.resize(m_data.size() + m_page_size*(id.value-n));
    Page page {{
        id,
        _b(m_data).range(id.as_index() * m_page_size, m_page_size),
        this,
        is_writable,
        false,
    }};
    if (is_writable)
        page.enable_tracking(m_scratch.get());
    return page;
}

auto InMemory::on_page_release(Page &page) -> void
{
    std::lock_guard lock {m_mutex};
    for (const auto &change: page.collect_changes())
        m_stack.push({_s(change.before), page.id(), change.offset});
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

} // cub
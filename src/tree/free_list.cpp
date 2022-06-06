#include "free_list.h"
#include "page/file_header.h"
#include "page/link.h"
#include "page/page.h"
#include "pool/interface.h"

namespace cub {

FreeList::FreeList(const Parameters &param)
    : m_pool {param.buffer_pool}
    , m_free_start {param.free_start}
    , m_free_count {param.free_count} {}

auto FreeList::save_header(FileHeader &header) const-> void
{
    header.set_free_start(m_free_start);
    header.set_free_count(m_free_count);
}

auto FreeList::load_header(const FileHeader &header) -> void
{
    m_free_start = header.free_start();
    m_free_count = header.free_count();
}

auto FreeList::push(Page page) -> void
{
    CUB_EXPECT_FALSE(page.id().is_root());
    page.set_type(PageType::FREELIST_LINK);
    Link link {std::move(page)};
    link.set_next_id(m_free_start);
    m_free_start = link.page().id();
    m_free_count++;
}

auto FreeList::pop() -> std::optional<Page>
{
    if (m_free_count) {
        Link link {m_pool->acquire(m_free_start, true)};
        m_free_start = link.next_id();
        m_free_count--;
        return link.take();
    }
    return std::nullopt;
}

} // cub
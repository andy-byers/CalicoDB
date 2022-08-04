#include "free_list.h"
#include "page/file_header.h"
#include "page/link.h"
#include "page/page.h"
#include "pool/interface.h"

namespace cco {

FreeList::FreeList(const Parameters &param)
    : m_pool {param.buffer_pool},
      m_head {param.free_head}
{}

auto FreeList::save_header(FileHeaderWriter &header) const -> void
{
    header.set_free_start(m_head);
}

auto FreeList::load_header(const FileHeaderReader &header) -> void
{
    m_head = header.free_start();
}

auto FreeList::push(Page page) -> Result<void>
{
    CCO_EXPECT_FALSE(page.id().is_base());
    page.set_type(PageType::FREELIST_LINK);
    Link link {std::move(page)};
    link.set_next_id(m_head);
    m_head = link.page().id();
    return m_pool->release(link.take());
}

auto FreeList::pop() -> Result<Page>
{
    if (!m_head.is_null()) {
        return m_pool->acquire(m_head, true)
            .and_then([&](Page page) -> Result<Page> {
                Link link {std::move(page)};
                m_head = link.next_id();
                return link.take();
            });
    }
    CCO_EXPECT_TRUE(m_head.is_null());
    return Err {Status::logic_error("cannot pop page: free list is empty")};
}

} // namespace cco
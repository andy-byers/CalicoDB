#include "free_list.h"
#include "page/file_header.h"
#include "page/link.h"
#include "page/page.h"
#include "pool/interface.h"

namespace cco {

FreeList::FreeList(const Parameters &param)
    : m_pool {param.buffer_pool},
      m_free_start {param.free_start},
      m_free_count {param.free_count}
{}

auto FreeList::save_header(FileHeaderWriter &header) const -> void
{
    header.set_free_start(m_free_start);
    header.set_free_count(m_free_count);
}

auto FreeList::load_header(const FileHeaderReader &header) -> void
{
    m_free_start = header.free_start();
    m_free_count = header.free_count();
}

auto FreeList::push(Page page) -> Result<void>
{
    CCO_EXPECT_FALSE(page.id().is_base());
    CCO_EXPECT_EQ(m_free_count == 0, m_free_start.is_null());
    page.set_type(PageType::FREELIST_LINK);
    Link link {std::move(page)};
    link.set_next_id(m_free_start);
    m_free_start = link.page().id();
    m_free_count++;
    return m_pool->release(link.take());
}

auto FreeList::pop() -> Result<Page>
{
    if (m_free_count) {
        CCO_EXPECT_FALSE(m_free_start.is_null());
        return m_pool->acquire(m_free_start, true)
            .and_then([&](Page page) -> Result<Page> {
                Link link {std::move(page)};
                m_free_start = link.next_id();
                m_free_count--;
                return link.take();
            });
    }
    CCO_EXPECT_TRUE(m_free_start.is_null());
    return Err {Status::logic_error("cannot pop page: free list is empty")};
}

} // namespace cco
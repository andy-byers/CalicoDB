#include "free_list.h"
#include "calico/options.h"
#include "page/link.h"
#include "page/page.h"
#include "pager/pager.h"

namespace calico {

auto FreeList::save_state(FileHeader &header) const -> void
{
    header.freelist_head = m_head.value;
}

auto FreeList::load_state(const FileHeader &header) -> void
{
    m_head.value = header.freelist_head;
}

auto FreeList::push(Page page) -> Result<void>
{
    CALICO_EXPECT_FALSE(page.id().is_root());
    page.set_type(PageType::FREELIST_LINK);
    Link link {std::move(page)};
    link.set_next_id(m_head);
    m_head = link.page().id();
    const auto s = m_pager->release(link.take());
    if (!s.is_ok()) return Err {s}; // TODO
    return {};
}

auto FreeList::pop() -> Result<Page>
{
    if (!m_head.is_null()) {
        return m_pager->acquire(m_head, true)
            .and_then([&](Page page) -> Result<Page> {
                Link link {std::move(page)};
                m_head = link.next_id();
                return link.take();
            });
    }
    CALICO_EXPECT_TRUE(m_head.is_null());
    return Err {Status::logic_error("cannot pop page: free list is empty")};
}

} // namespace cco
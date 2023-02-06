#include "free_list.h"
#include "node.h"
#include "pager/pager.h"

namespace Calico {

auto FreeList::push(Page page) -> void
{
    CALICO_EXPECT_FALSE(page.id().is_root());
    put_u64(page.span(sizeof(Id), sizeof(Id)), m_head.value);
    m_head = page.id();
    m_pager->release(std::move(page));
}

auto FreeList::pop() -> tl::expected<Page, Status>
{
    if (!m_head.is_null()) {
        Calico_New_R(page, m_pager->acquire(m_head));
        m_head.value = get_u64(page.data() + sizeof(Id));
        return page;
    }
    CALICO_EXPECT_TRUE(m_head.is_null());
    return tl::make_unexpected(logic_error("cannot pop page: free list is empty"));
}

} // namespace Calico
#include "free_list.h"
#include "page/link.h"
#include "page/page.h"
#include "pager/pager.h"
#include "utils/header.h"

namespace Calico {

auto FreeList::save_state(FileHeader &header) const -> void
{
    header.freelist_head = m_head.value;
}

auto FreeList::load_state(const FileHeader &header) -> void
{
    m_head.value = header.freelist_head;
}

auto FreeList::push(Page page) -> tl::expected<void, Status>
{
    CALICO_EXPECT_FALSE(page.id().is_root());
    page.set_type(PageType::FREELIST_LINK);
    Link link {std::move(page)};
    link.set_next_id(m_head);
    m_head = link.page().id();
    const auto s = m_pager->release(link.take());
    if (!s.is_ok()) return tl::make_unexpected(s); // TODO
    return {};
}

auto FreeList::pop() -> tl::expected<Page, Status>
{
    if (!m_head.is_null()) {
        return m_pager->acquire(m_head, true)
            .and_then([this](Page page) -> tl::expected<Page, Status> {
                Link link {std::move(page)};
                m_head = link.next_id();
                return link.take();
            });
    }
    CALICO_EXPECT_TRUE(m_head.is_null());
    return tl::make_unexpected(logic_error("cannot pop page: free list is empty"));
}

} // namespace Calico
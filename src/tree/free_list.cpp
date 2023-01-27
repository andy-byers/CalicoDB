#include "free_list.h"
#include "page/link.h"
#include "page/page.h"
#include "pager/pager.h"
#include "utils/header.h"

namespace Calico {

auto FreeList__::save_state(FileHeader__ &header) const -> void
{
    header.freelist_head = m_head.value;
}

auto FreeList__::load_state(const FileHeader__ &header) -> void
{
    m_head.value = header.freelist_head;
}

auto FreeList__::push(Page_ page) -> tl::expected<void, Status>
{
    CALICO_EXPECT_FALSE(page.id().is_root());
    page.set_type(PageType::FREELIST_LINK);
    Link link {std::move(page)};
    link.set_next_id(m_head);
    m_head = link.page().id();
    const auto s = m_pager->release(link.take());
    if (!s.is_ok()) return tl::make_unexpected(s);
    return {};
}

auto FreeList__::pop() -> tl::expected<Page_, Status>
{
    if (!m_head.is_null()) {
        return m_pager->acquire(m_head, true)
            .map([this](auto page){
                Link link {std::move(page)};
                m_head = link.next_id();
                return link.take();
            });
    }
    CALICO_EXPECT_TRUE(m_head.is_null());
    return tl::make_unexpected(logic_error("cannot pop page: free list is empty"));
}

} // namespace Calico
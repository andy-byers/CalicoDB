#ifndef CALICO_PAGE_LINK_H
#define CALICO_PAGE_LINK_H

#include "page.h"

namespace Calico {

class Link {
public:
    explicit Link(Page_);

    [[nodiscard]]
    auto id() const -> Id
    {
        return m_page.id();
    }

    [[nodiscard]]
    auto size() const -> Size
    {
        return m_page.size();
    }

    [[nodiscard]]
    auto page() const -> const Page_ &
    {
        return m_page;
    }

    auto page() -> Page_ &
    {
        return m_page;
    }

    auto take() -> Page_
    {
        return std::move(m_page);
    }

    [[nodiscard]] auto next_id() const -> Id;
    [[nodiscard]] auto content_size() const -> Size;
    [[nodiscard]] auto content_view() const -> Slice;
    auto set_next_id(Id) -> void;
    auto content_bytes(Size) -> Span;

private:
    Page_ m_page;
};

} // namespace Calico

#endif // CALICO_PAGE_LINK_H

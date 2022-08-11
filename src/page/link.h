#ifndef CCO_PAGE_LINK_H
#define CCO_PAGE_LINK_H

#include "page.h"

namespace cco {

class Link {
public:
    explicit Link(Page);

    [[nodiscard]]
    auto id() const -> PageId
    {
        return m_page.id();
    }

    [[nodiscard]]
    auto size() const -> Size
    {
        return m_page.size();
    }

    [[nodiscard]]
    auto page() const -> const Page &
    {
        return m_page;
    }

    auto page() -> Page &
    {
        return m_page;
    }

    auto take() -> Page
    {
        return std::move(m_page);
    }

    [[nodiscard]] auto next_id() const -> PageId;
    [[nodiscard]] auto content_size() const -> Size;
    [[nodiscard]] auto content_view() const -> BytesView;
    auto set_next_id(PageId) -> void;
    auto content_bytes(Size) -> Bytes;

private:
    Page m_page;
};

} // namespace cco

#endif // CCO_PAGE_LINK_H

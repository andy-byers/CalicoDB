#ifndef CALICO_PAGE_LINK_H
#define CALICO_PAGE_LINK_H

#include "page.h"

namespace calico {

class Link {
public:
    explicit Link(Page);
    ~Link() = default;


    [[nodiscard]] auto id() const -> PID
    {
        return m_page.id();
    }

    [[nodiscard]] auto size() const -> Size
    {
        return m_page.size();
    }


    [[nodiscard]] auto page() const -> const Page&
    {
        return m_page;
    }

    auto page() -> Page&
    {
        return m_page;
    }

    auto take() -> Page
    {
        return std::move(m_page);
    }

    [[nodiscard]] auto next_id() const -> PID;
    auto set_next_id(PID) -> void;

    [[nodiscard]] auto content_size() const -> Size;
    [[nodiscard]] auto ref_content() const -> BytesView;
    auto mut_content(Size) -> Bytes;

    Link(Link&&) = default;
    auto operator=(Link&&) -> Link& = default;

private:
    Page m_page;
};

} // calico

#endif // CALICO_PAGE_LINK_H

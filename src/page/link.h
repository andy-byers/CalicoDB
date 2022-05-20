#ifndef CUB_PAGE_LINK_H
#define CUB_PAGE_LINK_H

#include "common.h"
#include "page.h"

namespace cub {

class Link {
public:
    explicit Link(Page);
    ~Link() = default;


    auto id() const -> PID
    {
        return m_page.id();
    }

    auto size() const -> Size
    {
        return m_page.size();
    }

    [[nodiscard]] auto next_id() const -> PID;
    auto set_next_id(PID) -> void;

    [[nodiscard]] auto ref_content() const -> RefBytes;
    auto mut_content() -> MutBytes;

    auto take() -> Page {return std::move(m_page);}
    auto page() const -> const Page& {return m_page;}
    auto page() -> Page& {return m_page;}

    Link(Link&&) = default;
    auto operator=(Link&&) -> Link& = default;

private:
    Page m_page;
};

} // cub

#endif // CUB_PAGE_LINK_H

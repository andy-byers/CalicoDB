#ifndef CUB_PAGE_LINK_H
#define CUB_PAGE_LINK_H

#include "common.h"
#include "page.h"

namespace cub {

class Link {
public:
    explicit Link(Page);
    ~Link() = default;

    [[nodiscard]] auto next_id() const -> PID;
    auto set_next_id(PID) -> void;

    [[nodiscard]] auto ref_content() const -> RefBytes;
    auto mut_content() -> MutBytes;

    auto take() -> Page {return std::move(m_page);}
    auto page() const -> const Page& {return m_page;}
    auto page() -> Page& {return m_page;}

private:
    Page m_page;
};

} // cub

#endif // CUB_PAGE_LINK_H

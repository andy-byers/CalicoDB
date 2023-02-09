#include "memory.h"
#include "node.h"
#include "pager/pager.h"

namespace Calico {

[[nodiscard]]
static auto header_offset() -> Size
{
    return sizeof(Lsn) + sizeof(Byte);
}

[[nodiscard]]
static auto read_next_id(const Page &page) -> Id
{
    return {get_u64(page.view(header_offset()))};
}

static auto write_next_id(Page &page, Id next_id) -> void
{
    put_u64(page.span(header_offset(), sizeof(Id)), next_id.value);
}

[[nodiscard]]
static auto get_readable_content(const Page &page, Size size_limit) -> Slice
{
    const auto offset = header_offset() + sizeof(Lsn);
    return page.view(offset, std::min(size_limit, page.size() - offset));
}

[[nodiscard]]
static auto get_writable_content(Page &page, Size size_limit) -> Span
{
    const auto offset = header_offset() + sizeof(Lsn);
    return page.span(offset, std::min(size_limit, page.size() - offset));
}

static auto fix_link_back_refs(Pager &pager, Page &page, Id swap_pid) -> tl::expected<void, Status>
{
    (void)pager;(void)page;(void)swap_pid;return {};
}

extern auto fix_node_back_refs(Pager &pager, Page &page, Id swap_pid) -> tl::expected<void, Status>;


auto fix_back_refs(Pager &pager, Page &page, Id swap_pid) -> tl::expected<void, Status>
{
    fix_node_back_refs(pager, page, swap_pid);
    fix_link_back_refs(pager, page, swap_pid);return {};
//    if (LinkHeader::is_link(page)) {
//
//    } else {
//
//    }
//    const auto prev_id = read_prev_id(page);
//
//    // We hit the start of an overflow chain.
//    if (prev_id.is_null()) {
//
//    }
}

auto FreeList::push(Page page) -> void
{
    CALICO_EXPECT_FALSE(page.id().is_root());
    write_next_id(page, m_head);
    m_head = page.id();
    m_pager->release(std::move(page));
}

auto FreeList::pop() -> tl::expected<Page, Status>
{
    if (!m_head.is_null()) {
        Calico_New_R(page, m_pager->acquire(m_head));
        m_head = read_next_id(page);
        return page;
    }
    CALICO_EXPECT_TRUE(m_head.is_null());
    return tl::make_unexpected(Status::logic_error("free list is empty"));
}

auto read_chain(Pager &pager, Id pid, Span out) -> tl::expected<void, Status>
{
    while (!out.is_empty()) {
        Calico_New_R(page, pager.acquire(pid));
        const auto content = get_readable_content(page, out.size());
        mem_copy(out, content);
        out.advance(content.size());
        pid = read_next_id(page);
        pager.release(std::move(page));
    }
    return {};
}

auto write_chain(Pager &pager, FreeList &free_list, Slice overflow) -> tl::expected<Id, Status>
{
    CALICO_EXPECT_FALSE(overflow.is_empty());
    std::optional<Page> prev;
    auto head = Id::null();

    while (!overflow.is_empty()) {
        Calico_New_R(page, free_list.pop()
            .or_else([&pager](const Status &error) -> tl::expected<Page, Status> {
                if (error.is_logic_error()) {
                    return pager.allocate();
                }
                return tl::make_unexpected(error);
            }));

        pager.upgrade(page);
        auto content = get_writable_content(page, overflow.size());
        mem_copy(content, overflow, content.size());
        overflow.advance(content.size());

        if (prev) {
            write_next_id(*prev, page.id());
            pager.release(std::move(*prev));
        } else {
            head = page.id();
        }
        prev.emplace(std::move(page));
    }
    if (prev) {
        pager.release(std::move(*prev));
    }
    return head;
}

auto erase_chain(Pager &pager, FreeList &free_list, Id pid, Size size) -> tl::expected<void, Status>
{
    while (size) {
        Calico_New_R(page, pager.acquire(pid));
        size -= get_readable_content(page, size).size();
        pid = read_next_id(page);
        pager.upgrade(page);
        free_list.push(std::move(page));
    }
    return {};
}

} // namespace Calico
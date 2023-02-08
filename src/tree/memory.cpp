#include "memory.h"
#include "node.h"
#include "pager/pager.h"

namespace Calico {

[[nodiscard]]
static auto header_offset() -> Size
{
    return sizeof(Lsn) + sizeof(Byte);
}

//static auto read_prev_id(const Page &page) -> Id
//{
//    return {get_u64(page.view(header_offset()))};
//}

[[nodiscard]]
static auto read_next_id(const Page &page) -> Id
{
    return {get_u64(page.view(header_offset() + sizeof(Id)))};
}

static auto write_prev_id(Page &page, Id prev_id) -> void
{
    put_u64(page.span(header_offset(), sizeof(Id)), prev_id.value);
}

static auto write_next_id(Page &page, Id next_id) -> void
{
    put_u64(page.span(header_offset() + sizeof(Id), sizeof(Id)), next_id.value);
}

[[nodiscard]]
static auto get_readable_content(const Page &page, Size size_limit) -> Slice
{
    const auto offset = header_offset() + LinkHeader::SIZE;
    return page.view(offset, std::min(size_limit, page.size() - offset));
}

[[nodiscard]]
static auto get_writable_content(Page &page, Size size_limit) -> Span
{
    const auto offset = header_offset() + LinkHeader::SIZE;
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

auto FreeList::push(Page page) -> tl::expected<void, Status>
{
    CALICO_EXPECT_FALSE(page.id().is_root());
    const auto head = m_head;
    write_prev_id(page, Id::null());
    write_next_id(page, head);
    m_head = page.id();
    m_pager->release(std::move(page));

    if (!head.is_null()) {
        Calico_Put_R(page, m_pager->acquire(head));
        m_pager->upgrade(page);
        write_prev_id(page, m_head);
        m_pager->release(std::move(page));
    }
    return {};
}

auto FreeList::pop() -> tl::expected<Page, Status>
{
    if (!m_head.is_null()) {
        Calico_New_R(page, m_pager->acquire(m_head));
        m_head = read_next_id(page);

        if (!m_head.is_null()) {
            Calico_New_R(head, m_pager->acquire(m_head));
            m_pager->upgrade(head);
            write_prev_id(head, Id::null());
            m_pager->release(std::move(head));
        }
        return page;
    }
    CALICO_EXPECT_TRUE(m_head.is_null());
    return tl::make_unexpected(logic_error("cannot pop page: free list is empty"));
}

auto FreeList::vacuum(Size target) -> tl::expected<Size, Status>
{
//    Id last_id {m_pager->page_count()};

    return target;
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
            write_prev_id(page, prev->id());
            pager.release(std::move(*prev));
        } else {
            // TODO: We need to link back to the node that holds the overflow chain start ID (for vacuum). This can't be
            //       done here because we don't know where the cell will ultimately end up. We'll need some routine to fix
            //       the start of an overflow chain when a cell is moved.
            write_prev_id(page, Id::null());
            head = page.id();
        }
        if (overflow.is_empty()) {
            write_next_id(page, Id::null());
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

        Calico_Try_R(free_list.push(std::move(page)));
    }
    return {};
}

} // namespace Calico
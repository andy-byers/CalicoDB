#include "overflow.h"
#include "free_list.h"
#include "pager/pager.h"

namespace Calico {

static constexpr auto HEADER_SIZE = 2 * sizeof(Id);

[[nodiscard]]
static auto read_next_id(const Page &page) -> Id
{
    return {get_u64(page.data() + sizeof(Lsn))};
}

static auto write_next_id(Page &page, Id next_id) -> void
{
    put_u64(page.span(sizeof(Lsn), sizeof(Id)), next_id.value);
}

[[nodiscard]]
static auto get_readable_content(const Page &page, Size size_limit) -> Slice
{
    return page.view(HEADER_SIZE, std::min(size_limit, page.size() - HEADER_SIZE));
}

static auto get_writable_content(Page &page, Size size_limit) -> Span
{
    return page.span(HEADER_SIZE, std::min(size_limit, page.size() - HEADER_SIZE));
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
                if (error.is_logic_error())
                    return pager.allocate();
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
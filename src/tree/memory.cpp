#include "memory.h"
#include "node.h"
#include "pager/pager.h"

namespace Calico {

[[nodiscard]]
static constexpr auto header_offset() -> Size
{
    return sizeof(Lsn);
}

[[nodiscard]]
static constexpr auto content_offset() -> Size
{
    return header_offset() + sizeof(Id);
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
    return page.view(content_offset(), std::min(size_limit, page.size() - content_offset()));
}

[[nodiscard]]
static auto get_writable_content(Page &page, Size size_limit) -> Span
{
    return page.span(content_offset(), std::min(size_limit, page.size() - content_offset()));
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
        m_pager->upgrade(page, content_offset());
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

static constexpr auto ENTRY_SIZE =
    sizeof(Byte) + // Type
    sizeof(Id);    // Back pointer

static auto entry_offset(Id map_id, Id pid) -> Size
{
    CALICO_EXPECT_GT(pid, map_id);

    // Account for the page LSN.
    return sizeof(Lsn) + (pid.value-map_id.value-1)*ENTRY_SIZE;
}

static auto decode_entry(const Byte *data) -> PointerMap::Entry
{
    PointerMap::Entry entry;
    entry.type = PointerMap::Type {*data++};
    entry.back_ptr.value = get_u64(data);
    return entry;
}

auto PointerMap::read_entry(const Page &map, Id pid) -> Entry
{
    CALICO_EXPECT_NE(lookup_map(pid), pid);
    CALICO_EXPECT_EQ(lookup_map(pid), map.id());
    const auto offset = entry_offset(map.id(), pid);
    CALICO_EXPECT_LE(offset + ENTRY_SIZE, m_usable_size + sizeof(Lsn));
    return decode_entry(map.data() + offset);
}

auto PointerMap::write_entry(Pager *pager, Page &map, Id pid, Entry entry) -> void
{
    CALICO_EXPECT_NE(lookup_map(pid), pid);
    CALICO_EXPECT_EQ(lookup_map(pid), map.id());
    const auto offset = entry_offset(map.id(), pid);
    CALICO_EXPECT_LE(offset + ENTRY_SIZE, m_usable_size + sizeof(Lsn));
    const auto [back_ptr, type] = decode_entry(map.data() + offset);
    if (entry.back_ptr != back_ptr || entry.type != type) {
        if (!map.is_writable()) {
            CALICO_EXPECT_NE(pager, nullptr);
            pager->upgrade(map);
        }
        auto data = map.span(offset, ENTRY_SIZE).data();
        *data++ = entry.type;
        put_u64(data, entry.back_ptr.value);
    }
}

auto PointerMap::lookup_map(Id pid) const -> Id
{
    // Root page (1) has no parents, and page 2 is the first pointer map page. If "pid" is a pointer map
    // page, "pid" will be returned.
    if (pid.value < 2) {
        return Id::null();
    }
    const auto inc = m_usable_size/ENTRY_SIZE + 1;
    const auto idx = (pid.value-2) / inc;
    return {idx*inc + 2};
}

} // namespace Calico
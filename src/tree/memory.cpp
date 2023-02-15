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
static auto get_readable_content(const Page &page, Size size_limit) -> Slice
{
    return page.view(content_offset(), std::min(size_limit, page.size() - content_offset()));
}

[[nodiscard]]
static auto get_writable_content(Page &page, Size size_limit) -> Span
{
    return page.span(content_offset(), std::min(size_limit, page.size() - content_offset()));
}

auto FreeList::push(Page page) -> tl::expected<void, Status>
{
    CALICO_EXPECT_FALSE(page.id().is_root());
    write_next_id(page, m_head);

    // Write the parent of the old head, if it exists.
    PointerMap::Entry entry {page.id(), PointerMap::FREELIST_LINK};
    if (!m_head.is_null()) {
        Calico_Try_R(m_pointers->write_entry(m_head, entry));
    }
    // Clear the parent of the new head.
    entry.back_ptr = Id::null();
    Calico_Try_R(m_pointers->write_entry(page.id(), entry));

    m_head = page.id();
    m_pager->release(std::move(page));
    return {};
}

auto FreeList::pop() -> tl::expected<Page, Status>
{
    if (!m_head.is_null()) {
        Calico_New_R(page, m_pager->acquire(m_head));
        m_pager->upgrade(page, content_offset());
        m_head = read_next_id(page);

        if (!m_head.is_null()) {
            // Only clear the back pointer for the new freelist head. Callers must make sure to update the returned
            // node's back pointer at some point.
            const PointerMap::Entry entry {Id::null(), PointerMap::FREELIST_LINK};
            Calico_Try_R(m_pointers->write_entry(m_head, entry));
        }
        return page;
    }
    CALICO_EXPECT_TRUE(m_head.is_null());
    return tl::make_unexpected(Status::logic_error("free list is empty"));
}

auto OverflowList::read_chain(Id pid, Span out) -> tl::expected<void, Status>
{
    while (!out.is_empty()) {
        Calico_New_R(page, m_pager->acquire(pid));
        const auto content = get_readable_content(page, out.size());
        mem_copy(out, content);
        out.advance(content.size());
        pid = read_next_id(page);
        m_pager->release(std::move(page));
    }
    return {};
}

auto OverflowList::write_chain(Id pid, Slice overflow) -> tl::expected<Id, Status>
{
    CALICO_EXPECT_FALSE(overflow.is_empty());
    std::optional<Page> prev;
    auto head = Id::null();

    while (!overflow.is_empty()) {
        auto r = m_freelist->pop()
            .or_else([this](const Status &error) -> tl::expected<Page, Status> {
                if (error.is_logic_error()) {
                    Calico_New_R(page, m_pager->allocate());
                    if (m_pointers->lookup(page.id()) == page.id()) {
                        m_pager->release(std::move(page));
                        Calico_Put_R(page, m_pager->allocate());
                    }
                    return page;
                }
                return tl::make_unexpected(error);
            });
        Calico_New_R(page, std::move(r));

        auto content = get_writable_content(page, overflow.size());
        mem_copy(content, overflow, content.size());
        overflow.advance(content.size());

        if (prev) {
            write_next_id(*prev, page.id());
            m_pager->release(std::move(*prev));

            // Update overflow link back pointers.
            const PointerMap::Entry entry {prev->id(), PointerMap::OVERFLOW_LINK};
            Calico_Try_R(m_pointers->write_entry(page.id(), entry));
        } else {
            head = page.id();

            const PointerMap::Entry entry {pid, PointerMap::OVERFLOW_HEAD};
            Calico_Try_R(m_pointers->write_entry(page.id(), entry));
        }
        prev.emplace(std::move(page));
    }
    if (prev) {
        // "prev" contains the last page in the chain.
        write_next_id(*prev, Id::null());
        m_pager->release(std::move(*prev));
    }
    return head;
}

auto OverflowList::erase_chain(Id pid, Size size) -> tl::expected<void, Status>
{
    while (size) {
        Calico_New_R(page, m_pager->acquire(pid));
        size -= get_readable_content(page, size).size();
        pid = read_next_id(page);
        m_pager->upgrade(page);
        Calico_Try_R(m_freelist->push(std::move(page)));
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

auto PointerMap::read_entry(Id pid) -> tl::expected<Entry, Status>
{
    const auto mid = lookup(pid);
    CALICO_EXPECT_GE(mid.value, 2);
    CALICO_EXPECT_NE(mid, pid);
    const auto offset = entry_offset(mid, pid);
    CALICO_EXPECT_LE(offset + ENTRY_SIZE, m_pager->page_size());
    Calico_New_R(map, m_pager->acquire(mid));
    const auto entry = decode_entry(map.data() + offset);
    m_pager->release(std::move(map));
    return entry;
}

auto PointerMap::write_entry(Id pid, Entry entry) -> tl::expected<void, Status>
{
    const auto mid = lookup(pid);
    CALICO_EXPECT_GE(mid.value, 2);
    CALICO_EXPECT_NE(mid, pid);
    const auto offset = entry_offset(mid, pid);
    CALICO_EXPECT_LE(offset + ENTRY_SIZE, m_pager->page_size());
    Calico_New_R(map, m_pager->acquire(mid));
    const auto [back_ptr, type] = decode_entry(map.data() + offset);
    if (entry.back_ptr != back_ptr || entry.type != type) {
        if (!map.is_writable()) {
            m_pager->upgrade(map);
        }
        auto data = map.span(offset, ENTRY_SIZE).data();
        *data++ = entry.type;
        put_u64(data, entry.back_ptr.value);
    }
    m_pager->release(std::move(map));
    return {};
}

auto PointerMap::lookup(Id pid) -> Id
{
    // Root page (1) has no parents, and page 2 is the first pointer map page. If "pid" is a pointer map
    // page, "pid" will be returned.
    if (pid.value < 2) {
        return Id::null();
    }
    const auto usable_size = m_pager->page_size() - sizeof(Lsn);
    const auto inc = usable_size/ENTRY_SIZE + 1;
    const auto idx = (pid.value-2) / inc;
    return {idx*inc + 2};
}

[[nodiscard]]
auto read_next_id(const Page &page) -> Id
{
    return {get_u64(page.view(header_offset()))};
}

auto write_next_id(Page &page, Id next_id) -> void
{
    put_u64(page.span(header_offset(), sizeof(Id)), next_id.value);
}

} // namespace Calico
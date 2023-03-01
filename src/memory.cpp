#include "memory.h"
#include "node.h"
#include "pager.h"

namespace calicodb
{

[[nodiscard]] static constexpr auto header_offset() -> std::size_t
{
    return sizeof(Lsn);
}

[[nodiscard]] static constexpr auto content_offset() -> std::size_t
{
    return header_offset() + sizeof(Id);
}

[[nodiscard]] static auto get_readable_content(const Page &page, std::size_t size_limit) -> Slice
{
    return page.view(content_offset(), std::min(size_limit, page.size() - content_offset()));
}

[[nodiscard]] static auto get_writable_content(Page &page, std::size_t size_limit) -> Span
{
    return page.span(content_offset(), std::min(size_limit, page.size() - content_offset()));
}

auto Freelist::pop(Page &page) -> Status
{
    if (!m_head.is_null()) {
        CDB_TRY(m_pager->acquire(m_head, page));
        m_pager->upgrade(page, content_offset());
        m_head = read_next_id(page);

        if (!m_head.is_null()) {
            // Only clear the back pointer for the new freelist head. Callers must make sure to update the returned
            // node's back pointer at some point.
            const PointerMap::Entry entry {Id::null(), PointerMap::FreelistLink};
            CDB_TRY(m_pointers->write_entry(m_head, entry));
        }
        return Status::ok();
    }
    CDB_EXPECT_TRUE(m_head.is_null());
    return Status::logic_error("free list is empty");
}

auto Freelist::push(Page page) -> Status
{
    CDB_EXPECT_FALSE(page.id().is_root());
    write_next_id(page, m_head);

    // Write the parent of the old head, if it exists.
    PointerMap::Entry entry {page.id(), PointerMap::FreelistLink};
    if (!m_head.is_null()) {
        CDB_TRY(m_pointers->write_entry(m_head, entry));
    }
    // Clear the parent of the new head.
    entry.back_ptr = Id::null();
    CDB_TRY(m_pointers->write_entry(page.id(), entry));

    m_head = page.id();
    m_pager->release(std::move(page));
    return Status::ok();
}

auto OverflowList::read_chain(Span out, Id pid, std::size_t offset) const -> Status
{
    while (!out.is_empty()) {
        Page page;
        CDB_TRY(m_pager->acquire(pid, page));
        auto content = get_readable_content(page, page.size());

        if (offset) {
            const auto max = std::min(offset, content.size());
            content.advance(max);
            offset -= max;
        }
        if (!content.is_empty()) {
            const auto size = std::min(out.size(), content.size());
            mem_copy(out, content, size);
            out.advance(size);
        }
        pid = read_next_id(page);
        m_pager->release(std::move(page));
    }
    return Status::ok();
}

auto OverflowList::write_chain(Id &out, Id pid, Slice first, Slice second) -> Status
{
    std::optional<Page> prev;
    auto head = Id::null();

    if (first.is_empty()) {
        first = second;
        second.clear();
    }

    while (!first.is_empty()) {
        Page page;
        auto s = m_freelist->pop(page);
        if (s.is_logic_error()) {
            s = m_pager->allocate(page);
            if (s.is_ok() && m_pointers->lookup(page.id()) == page.id()) {
                m_pager->release(std::move(page));
                s = m_pager->allocate(page);
            }
        }
        CDB_TRY(s);

        auto content = get_writable_content(page, first.size() + second.size());
        auto limit = std::min(first.size(), content.size());
        mem_copy(content, first, limit);
        first.advance(limit);

        if (first.is_empty()) {
            first = second;
            second.clear();

            if (!first.is_empty()) {
                content.advance(limit);
                limit = std::min(first.size(), content.size());
                mem_copy(content, first, limit);
                first.advance(limit);
            }
        }
        PointerMap::Entry entry {pid, PointerMap::OverflowHead};
        if (prev) {
            write_next_id(*prev, page.id());
            m_pager->release(std::move(*prev));
            entry.back_ptr = prev->id();
            entry.type = PointerMap::OverflowLink;
        } else {
            head = page.id();
        }
        CDB_TRY(m_pointers->write_entry(page.id(), entry));
        prev.emplace(std::move(page));
    }
    if (prev) {
        // "prev" contains the last page in the chain.
        write_next_id(*prev, Id::null());
        m_pager->release(std::move(*prev));
    }
    out = head;
    return Status::ok();
}

auto OverflowList::copy_chain(Id &out, Id pid, Id overflow_id, std::size_t size) -> Status
{
    if (m_scratch.size() < size) {
        m_scratch.resize(size);
    }
    Span buffer {m_scratch};
    buffer.truncate(size);

    CDB_TRY(read_chain(buffer, overflow_id));
    return write_chain(out, pid, buffer);
}

auto OverflowList::erase_chain(Id pid) -> Status
{
    while (!pid.is_null()) {
        Page page;
        CDB_TRY(m_pager->acquire(pid, page));
        pid = read_next_id(page);
        m_pager->upgrade(page);
        CDB_TRY(m_freelist->push(std::move(page)));
    }
    return Status::ok();
}

static constexpr auto ENTRY_SIZE =
    sizeof(char) + // Type
    sizeof(Id);    // Back pointer

static auto entry_offset(Id map_id, Id pid) -> std::size_t
{
    CDB_EXPECT_GT(pid, map_id);

    // Account for the page LSN.
    return sizeof(Lsn) + (pid.value - map_id.value - 1) * ENTRY_SIZE;
}

static auto decode_entry(const char *data) -> PointerMap::Entry
{
    PointerMap::Entry entry;
    entry.type = PointerMap::Type {*data++};
    entry.back_ptr.value = get_u64(data);
    return entry;
}

auto PointerMap::read_entry(Id pid, Entry &out) const -> Status
{
    const auto mid = lookup(pid);
    CDB_EXPECT_GE(mid.value, 2);
    CDB_EXPECT_NE(mid, pid);
    const auto offset = entry_offset(mid, pid);
    CDB_EXPECT_LE(offset + ENTRY_SIZE, m_pager->page_size());
    Page map;
    CDB_TRY(m_pager->acquire(mid, map));
    out = decode_entry(map.data() + offset);
    m_pager->release(std::move(map));
    return Status::ok();
}

auto PointerMap::write_entry(Id pid, Entry entry) -> Status
{
    const auto mid = lookup(pid);
    CDB_EXPECT_GE(mid.value, 2);
    CDB_EXPECT_NE(mid, pid);
    const auto offset = entry_offset(mid, pid);
    CDB_EXPECT_LE(offset + ENTRY_SIZE, m_pager->page_size());
    Page map;
    CDB_TRY(m_pager->acquire(mid, map));
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
    return Status::ok();
}

auto PointerMap::lookup(Id pid) const -> Id
{
    // Root page (1) has no parents, and page 2 is the first pointer map page. If "pid" is a pointer map
    // page, "pid" will be returned.
    if (pid.value < 2) {
        return Id::null();
    }
    const auto usable_size = m_pager->page_size() - sizeof(Lsn);
    const auto inc = usable_size / ENTRY_SIZE + 1;
    const auto idx = (pid.value - 2) / inc;
    return {idx * inc + 2};
}

[[nodiscard]] auto read_next_id(const Page &page) -> Id
{
    return {get_u64(page.view(header_offset()))};
}

auto write_next_id(Page &page, Id next_id) -> void
{
    put_u64(page.span(header_offset(), sizeof(Id)), next_id.value);
}

} // namespace calicodb
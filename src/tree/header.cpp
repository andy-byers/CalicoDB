#include "header.h"
#include "pager/page.h"
#include "utils/crc.h"
#include "utils/encoding.h"
#include "utils/expect.h"

namespace Calico {

FileHeader::FileHeader(const Page &page)
{
    CALICO_EXPECT_TRUE(page.id().is_root());
    auto data = page.data();

    magic_code = get_u32(data);
    data += sizeof(std::uint32_t);

    header_crc = get_u32(data);
    data += sizeof(std::uint32_t);

    page_count = get_u64(data);
    data += sizeof(std::uint64_t);

    record_count = get_u64(data);
    data += sizeof(std::uint64_t);

    free_list_id.value = get_u64(data);
    data += sizeof(Id);

    recovery_lsn.value = get_u64(data);
    data += sizeof(Lsn);

    page_size = get_u16(data);
}

auto FileHeader::compute_crc() const -> std::uint32_t
{
    // TODO: This is sketchy. Probably need some pragmas to ensure certain packing for this struct.
    const auto *data = reinterpret_cast<const Byte *>(this) + sizeof(Id);
    return crc32c::Value(data, sizeof(FileHeader) - sizeof(Id));
}

auto FileHeader::write(Page &page) const -> void
{
    CALICO_EXPECT_TRUE(page.id().is_root());
    auto data = page.data();

    put_u32(data, magic_code);
    data += sizeof(std::uint32_t);

    put_u32(data, header_crc);
    data += sizeof(std::uint32_t);

    put_u64(data, page_count);
    data += sizeof(std::uint64_t);

    put_u64(data, record_count);
    data += sizeof(std::uint64_t);

    put_u64(data, free_list_id.value);
    data += sizeof(Id);

    put_u64(data, recovery_lsn.value);
    data += sizeof(Lsn);

    put_u16(data, page_size);
    insert_delta(page.m_deltas, {0, SIZE});
}

NodeHeader::NodeHeader(const Page &page)
{
    auto data = page.data() + page_offset(page);

    page_lsn.value = get_u64(data);
    data += sizeof(Id);

    const auto flags = *data;
    data++;

    // Flags byte (only 1 bit is needed right now).
    is_external = flags & 1;

    parent_id.value = get_u64(data);
    data += sizeof(Id);

    next_id.value = get_u64(data);
    data += sizeof(Id);

    prev_id.value = get_u64(data);
    data += sizeof(Id);

    cell_count = get_u16(data);
    data += sizeof(PageSize);

    cell_start = get_u16(data);
    data += sizeof(PageSize);

    frag_count = get_u16(data);
    data += sizeof(PageSize);

    free_start = get_u16(data);
    data += sizeof(PageSize);

    free_total = get_u16(data);
}

auto NodeHeader::write(Page &page) const -> void
{
    auto *data = page.data() + page_offset(page);

    put_u64(data, page_lsn.value);
    data += sizeof(Id);

    *data++ = static_cast<Byte>(is_external);

    put_u64(data, parent_id.value);
    data += sizeof(Id);

    put_u64(data, next_id.value);
    data += sizeof(Id);

    put_u64(data, prev_id.value);
    data += sizeof(Id);

    put_u16(data, cell_count);
    data += sizeof(PageSize);

    put_u16(data, cell_start);
    data += sizeof(PageSize);

    put_u16(data, frag_count);
    data += sizeof(PageSize);

    put_u16(data, free_start);
    data += sizeof(PageSize);

    put_u16(data, free_total);
    insert_delta(page.m_deltas, {page_offset(page), SIZE});
}

} // namespace Calico
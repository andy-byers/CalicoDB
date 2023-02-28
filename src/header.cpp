#include "header.h"
#include "crc.h"
#include "encoding.h"
#include "page.h"

namespace calicodb
{

static auto write_file_header(char *data, const FileHeader &header) -> void
{
    put_u32(data, header.magic_code);
    data += sizeof(std::uint32_t);

    put_u32(data, header.header_crc);
    data += sizeof(std::uint32_t);

    put_u64(data, header.page_count);
    data += sizeof(std::uint64_t);

    put_u64(data, header.record_count);
    data += sizeof(std::uint64_t);

    put_u64(data, header.freelist_head.value);
    data += sizeof(Id);

    put_u64(data, header.commit_lsn.value);
    data += sizeof(Lsn);

    put_u16(data, header.page_size);
}

FileHeader::FileHeader(const Page &page)
    : FileHeader {page.data()}
{
}

FileHeader::FileHeader(const char *data)
{
    magic_code = get_u32(data);
    data += sizeof(std::uint32_t);

    header_crc = get_u32(data);
    data += sizeof(std::uint32_t);

    page_count = get_u64(data);
    data += sizeof(std::uint64_t);

    record_count = get_u64(data);
    data += sizeof(std::uint64_t);

    freelist_head.value = get_u64(data);
    data += sizeof(Id);

    commit_lsn.value = get_u64(data);
    data += sizeof(Lsn);

    page_size = get_u16(data);
}

auto FileHeader::compute_crc() const -> std::uint32_t
{
    char data[FileHeader::SIZE];
    write_file_header(data, *this);
    return crc32c::Value(data + 8, FileHeader::SIZE - 8);
}

auto FileHeader::write(Page &page) const -> void
{
    CDB_EXPECT_TRUE(page.id().is_root());
    write_file_header(page.data(), *this);
    insert_delta(page.m_deltas, {0, SIZE});
}

auto NodeHeader::read(const Page &page) -> void
{
    auto data = page.data() + page_offset(page) + sizeof(Lsn);

    // Flags byte.
    is_external = *data++;

    next_id.value = get_u64(data);
    data += sizeof(Id);

    prev_id.value = get_u64(data);
    data += sizeof(Id);

    cell_count = get_u16(data);
    data += sizeof(PageSize);

    cell_start = get_u16(data);
    data += sizeof(PageSize);

    free_start = get_u16(data);
    data += sizeof(PageSize);

    free_total = get_u16(data);
    data += sizeof(PageSize);

    frag_count = static_cast<std::uint8_t>(*data);
}

auto NodeHeader::write(Page &page) const -> void
{
    auto *data = page.data() + page_offset(page) + sizeof(Lsn);

    *data++ = static_cast<char>(is_external);

    put_u64(data, next_id.value);
    data += sizeof(Id);

    put_u64(data, prev_id.value);
    data += sizeof(Id);

    put_u16(data, cell_count);
    data += sizeof(PageSize);

    put_u16(data, cell_start);
    data += sizeof(PageSize);

    put_u16(data, free_start);
    data += sizeof(PageSize);

    put_u16(data, free_total);
    data += sizeof(PageSize);

    *data = static_cast<char>(frag_count);
    insert_delta(page.m_deltas, {page_offset(page), SIZE});
}

} // namespace calicodb
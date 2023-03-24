// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "header.h"
#include "crc.h"
#include "encoding.h"

namespace calicodb
{

static auto write_file_header(char *data, const FileHeader &header) -> void
{
    put_u32(data, header.magic_code);
    data += sizeof(std::uint32_t);

    put_u32(data, header.header_crc);
    data += sizeof(std::uint32_t);

    put_u32(data, header.page_count);
    data += sizeof(std::uint32_t);

    put_u32(data, header.freelist_head.value);
    data += Id::kSize;

    put_u64(data, header.record_count);
    data += sizeof(std::uint64_t);

    put_u64(data, header.commit_lsn.value);
    data += Lsn::kSize;

    put_u16(data, static_cast<std::uint16_t>(header.page_size));
}

auto FileHeader::read(const char *data) -> void
{
    magic_code = get_u32(data);
    data += sizeof(std::uint32_t);

    header_crc = get_u32(data);
    data += sizeof(std::uint32_t);

    page_count = get_u32(data);
    data += sizeof(std::uint32_t);

    freelist_head.value = get_u32(data);
    data += Id::kSize;

    record_count = get_u64(data);
    data += sizeof(std::uint64_t);

    commit_lsn.value = get_u64(data);
    data += Lsn::kSize;

    page_size = get_u16(data);
}

auto FileHeader::compute_crc() const -> std::uint32_t
{
    char data[FileHeader::kSize];
    write_file_header(data, *this);
    return crc32c::Value(data + 8, FileHeader::kSize - 8);
}

auto FileHeader::write(char *data) const -> void
{
    write_file_header(data, *this);
}

auto NodeHeader::read(const char *data) -> void
{
    // Flags byte.
    is_external = *data++;

    next_id.value = get_u32(data);
    data += Id::kSize;

    prev_id.value = get_u32(data);
    data += Id::kSize;

    cell_count = get_u16(data);
    data += sizeof(std::uint16_t);

    cell_start = get_u16(data);
    data += sizeof(std::uint16_t);

    free_start = get_u16(data);
    data += sizeof(std::uint16_t);

    frag_count = static_cast<std::uint8_t>(*data);
}

auto NodeHeader::write(char *data) const -> void
{
    *data++ = static_cast<char>(is_external);

    put_u32(data, next_id.value);
    data += Id::kSize;

    put_u32(data, prev_id.value);
    data += Id::kSize;

    put_u16(data, static_cast<std::uint16_t>(cell_count));
    data += sizeof(std::uint16_t);

    put_u16(data, static_cast<std::uint16_t>(cell_start));
    data += sizeof(std::uint16_t);

    put_u16(data, static_cast<std::uint16_t>(free_start));
    data += sizeof(std::uint16_t);

    *data = static_cast<char>(frag_count);
}

} // namespace calicodb
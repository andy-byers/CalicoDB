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
    data += sizeof(U32);

    put_u32(data, header.header_crc);
    data += sizeof(U32);

    put_u32(data, header.page_count);
    data += sizeof(U32);

    put_u32(data, header.freelist_head.value);
    data += Id::kSize;

    put_u64(data, header.record_count);
    data += sizeof(U64);

    put_u64(data, header.ckpt_number);
    data += sizeof(U64);

    put_u16(data, static_cast<U16>(header.page_size));
}

auto FileHeader::read(const char *data) -> void
{
    magic_code = get_u32(data);
    data += sizeof(U32);

    header_crc = get_u32(data);
    data += sizeof(U32);

    page_count = get_u32(data);
    data += sizeof(U32);

    freelist_head.value = get_u32(data);
    data += Id::kSize;

    record_count = get_u64(data);
    data += sizeof(U64);

    ckpt_number = get_u64(data);
    data += sizeof(U64);

    page_size = get_u16(data);
}

auto FileHeader::compute_crc() const -> U32
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
    data += sizeof(U16);

    cell_start = get_u16(data);
    data += sizeof(U16);

    free_start = get_u16(data);
    data += sizeof(U16);

    frag_count = static_cast<U8>(*data);
}

auto NodeHeader::write(char *data) const -> void
{
    *data++ = static_cast<char>(is_external);

    put_u32(data, next_id.value);
    data += Id::kSize;

    put_u32(data, prev_id.value);
    data += Id::kSize;

    put_u16(data, static_cast<U16>(cell_count));
    data += sizeof(U16);

    put_u16(data, static_cast<U16>(cell_start));
    data += sizeof(U16);

    put_u16(data, static_cast<U16>(free_start));
    data += sizeof(U16);

    *data = static_cast<char>(frag_count);
}

} // namespace calicodb
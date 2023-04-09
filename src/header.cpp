// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "header.h"
#include "encoding.h"

namespace calicodb
{

auto FileHeader::read(const char *data) -> bool
{
    if (std::memcmp(data, kIdentifier, sizeof(kIdentifier)) != 0) {
        return false;
    }
    data += sizeof(kIdentifier);

    page_size = get_u16(data);
    data += sizeof(U16);

    page_count = get_u32(data);
    data += sizeof(U32);

    freelist_head = get_u32(data);
    data += sizeof(U32);

    format_version = *data++;
    return true;
}

auto FileHeader::write(char *data) const -> void
{
    std::memcpy(data, kIdentifier, sizeof(kIdentifier));
    data += sizeof(kIdentifier);

    put_u16(data, page_size);
    data += sizeof(U16);

    put_u32(data, page_count);
    data += sizeof(U32);

    put_u32(data, freelist_head);
    data += sizeof(U32);

    *data++ = format_version;
}

auto NodeHeader::read(const char *data) -> void
{
    // Flags byte.
    is_external = *data++ & 1;

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
    *data++ = is_external ? 1 : 2;

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
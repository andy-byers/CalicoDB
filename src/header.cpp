// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "header.h"
#include "encoding.h"
#include "logging.h"

namespace calicodb
{

auto NodeHeader::read(const char *data) -> void
{
    // TODO: Return a bool from this method. false if there is corruption, true otherwise.
    //       These asserts should be made into guard clauses at some point. The tree layer
    //       can set an error status on the pager if there is corruption detected.
    CALICODB_EXPECT_EQ(*data & ~0b11, 0);

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
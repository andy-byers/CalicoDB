// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "header.h"
#include "encoding.h"
#include "logging.h"

namespace calicodb
{

auto FileHeader::read(const char *data) -> bool
{
    if (std::memcmp(data, kIdentifier, sizeof(kIdentifier)) != 0) {
        return false;
    }
    data += sizeof(kIdentifier);

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

    put_u32(data, page_count);
    data += sizeof(U32);

    put_u32(data, freelist_head);
    data += sizeof(U32);

    *data++ = format_version;
}

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

auto bad_identifier_error(const Slice &bad_identifier) -> Status
{
    const auto good_id = FileHeader::kIdentifier;
    const auto bad_id = bad_identifier.range(0, std::min(bad_identifier.size(), sizeof(good_id)));
    std::string message("not a CalicoDB database (expected identifier ");
    append_fmt_string(message, R"("%s\00" but read "%s"))", good_id, escape_string(bad_id).c_str());
    return Status::invalid_argument(message);
}

} // namespace calicodb
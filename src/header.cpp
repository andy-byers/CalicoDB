// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "header.h"
#include "calicodb/options.h"
#include "logging.h"

namespace calicodb
{

auto FileHdr::check_db_support(const char *root) -> Status
{
    Status s;
    const Slice fmt_string(root, sizeof(kFmtString));
    const auto bad_fmt_string = std::memcmp(
        fmt_string.data(), kFmtString, fmt_string.size());
    if (bad_fmt_string) {
        std::string message;
        append_fmt_string(
            message, R"(not a CalicoDB database (expected identifier "%s\00" but got "%s"))",
            kFmtString, escape_string(fmt_string).c_str());
        s = Status::invalid_argument(message);
    }
    const auto bad_fmt_version =
        root[kFmtVersionOffset] > kFmtVersion;
    if (s.is_ok() && bad_fmt_version) {
        std::string message;
        append_fmt_string(
            message, R"(CalicoDB version is not supported (expected format version "%d" but got "%d"))",
            kFmtVersion, root[kFmtVersionOffset]);
        s = Status::invalid_argument(message);
    }
    return s;
}

auto FileHdr::make_supported_db(char *root) -> void
{
    // Initialize the file header.
    std::memcpy(root, kFmtString, sizeof(kFmtString));
    root[kFmtVersionOffset] = kFmtVersion;
    put_u32(root + kPageCountOffset, 1);

    // Initialize the root page of the schema tree.
    NodeHdr root_hdr;
    root_hdr.is_external = true;
    root_hdr.cell_start = kPageSize;
    root_hdr.write(root + kSize);
}

enum : char {
    kExternal = '\x01',
    kInternal = '\x02',
};

auto NodeHdr::read(const char *data) -> int
{
    if (const auto t = *data++; t == kExternal) {
        is_external = true;
    } else if (t == kInternal) {
        is_external = false;
    } else {
        return -1;
    }

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
    return 0;
}

auto NodeHdr::write(char *data) const -> void
{
    *data++ = is_external ? kExternal : kInternal;

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
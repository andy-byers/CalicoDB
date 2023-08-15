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
        s = Status::invalid_argument("file is not a CalicoDB database");
    }
    const auto bad_fmt_version =
        root[kFmtVersionOffset] > kFmtVersion;
    if (s.is_ok() && bad_fmt_version) {
        s = Status::invalid_argument("CalicoDB version is not supported");
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
    NodeHdr::put_type(root + FileHdr::kSize, true);
    NodeHdr::put_cell_start(root + FileHdr::kSize, kPageSize);
}

} // namespace calicodb
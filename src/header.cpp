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
    if (0 != std::memcmp(root, kFmtString, sizeof(kFmtString))) {
        return Status::invalid_argument("file is not a CalicoDB database");
    } else if (root[kFmtVersionOffset] > kFmtVersion) {
        return Status::invalid_argument("CalicoDB version is not supported");
    }
    return Status::ok();
}

auto FileHdr::make_supported_db(char *root) -> void
{
    // Initialize the file header.
    std::memcpy(root, kFmtString, sizeof(kFmtString));
    root[kFmtVersionOffset] = kFmtVersion;
    put_page_count(root, 1);
    put_largest_root(root, Id::root());

    // Initialize the root page of the schema tree.
    root += FileHdr::kSize;
    NodeHdr::put_type(root, true);
    NodeHdr::put_cell_start(root, kPageSize);
}

} // namespace calicodb
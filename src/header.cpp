// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "header.h"
#include "calicodb/options.h"
#include "logging.h"

namespace calicodb
{

auto FileHdr::check_page_size(size_t page_size) -> Status
{
    if (page_size && (page_size & (page_size - 1))) {
        // Note that a page size of 0 in the header actually represents 1 << 16, which won't quite
        // fit in 2 bytes.
        return StatusBuilder::corruption("page size (%u) is not a power of 2");
    } else if (page_size < kMinPageSize) {
        return StatusBuilder::corruption("page size (%u) is too small (minimum page size is %u)",
                                         page_size, kMinPageSize);
    } else if (page_size > kMaxPageSize) {
        return StatusBuilder::corruption("page size (%u) is too large (maximum page size is %u)",
                                         page_size, kMaxPageSize);
    }
    return Status::ok();
}

auto FileHdr::check_db_support(const char *root) -> Status
{
    if (0 != std::memcmp(root, kFmtString, sizeof(kFmtString))) {
        return Status::invalid_argument("file is not a CalicoDB database");
    } else if (root[kFmtVersionOffset] > kFmtVersion) {
        return StatusBuilder::invalid_argument("CalicoDB file format version %d is not supported"
                                               "(maximum supported version is %d)",
                                               root[kFmtVersionOffset], kFmtVersion);
    }
    return check_page_size(FileHdr::get_page_size(root));
}

auto FileHdr::make_supported_db(char *root, size_t page_size) -> void
{
    // Initialize the file header.
    std::memcpy(root, kFmtString, sizeof(kFmtString));
    root[kFmtVersionOffset] = kFmtVersion;
    put_page_count(root, 1);
    put_largest_root(root, Id::root());
    put_page_size(root, static_cast<uint32_t>(page_size));

    // Initialize the root page of the schema tree.
    root += FileHdr::kSize;
    NodeHdr::put_type(root, true);
    NodeHdr::put_cell_start(root, static_cast<uint32_t>(page_size));
}

} // namespace calicodb
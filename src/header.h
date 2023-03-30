// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_HEADER_H
#define CALICODB_HEADER_H

#include "utils.h"

namespace calicodb
{

class Page;

// There are 4 page types in CalicoDB: nodes, freelist pages, overflow chain pages, and pointer
// map pages. Pages that store records or separator keys are called nodes, and pages that hold
// parent pointers for other pages are called pointer maps. Overflow chain pages store data, as
// well as a pointer to the next page in the chain, while freelist pages just store the "next
// pointer".
//
// The first page in the database file is called the root page. The root page contains the file
// header at offset 0, followed by a page header and a node header (the root is always a node).
// All other pages have a page header at offset 0, followed by type-specific headers.

// File Header Format:
//     Offset  Size  Name
//    ----------------------------
//     0       4     magic_code
//     4       4     header_crc
//     8       4     page_count
//     12      4     free_list_id
//     16      8     record_count
//     24      8     commit_lsn
//     32      2     page_size
//
// NOTE: The "page_size" field contains 0 if the maximum page size of 65,536 is used, since this
// value cannot be represented by a 16-bit unsigned integer.
struct FileHeader {
    static constexpr U32 kMagicCode = 0xB11924E1;
    static constexpr std::size_t kSize = 34;
    auto read(const char *data) -> void;
    auto write(char *data) const -> void;

    [[nodiscard]] auto compute_crc() const -> U32;

    U32 magic_code = kMagicCode;
    U32 header_crc = 0;
    U32 page_count = 0;
    U64 record_count = 0; // TODO: Don't store this! Needs to be updated way too frequently to be feasible.
    Id freelist_head;
    U64 ckpt_number = 0;
    unsigned page_size = 0;
};

// Node Header Format:
//     Offset  Size  Name
//    --------------------------
//     0       1     flags
//     1       4     next_id
//     5       4     prev_id
//     9       2     cell_count
//     11      2     cell_start
//     13      2     free_start
//     15      1     frag_count
struct NodeHeader {
    static constexpr std::size_t kSize = 16;
    auto read(const char *data) -> void;
    auto write(char *data) const -> void;

    Id next_id;
    Id prev_id;
    unsigned cell_count = 0;
    unsigned cell_start = 0;
    unsigned free_start = 0;
    unsigned frag_count = 0;
    bool is_external = false;
};

} // namespace calicodb

#endif // CALICODB_HEADER_H

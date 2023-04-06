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
// header at offset 0, followed by a node header (the root is always a node).

// File Header Format:
//     Offset  Size  Name
//    ----------------------------
//     0       18    Identifier string
//     18      2     Page size
//     20      4     Number of pages in the DB
//     24      4     Freelist head
//     28      1     File format version
//     29      35    Reserved
//
// NOTE: The "page_size" field contains 0 if the maximum page size of 65,536 is used, since this
// value cannot be represented by a 16-bit unsigned integer.
struct FileHeader {
    static constexpr char kIdentifier[18] = "CalicoDB format 1";
    static constexpr std::size_t kSize = 64;

    auto read(const char *data) -> bool;
    auto write(char *data) const -> void;

    U16 page_size = 0;
    U32 page_count = 0;
    U32 freelist_head = 0;
    char format_version = 0;
};

// Node Header Format:
//     Offset  Size  Name
//    --------------------------
//     0       1     Flags byte (0 = internal node, 1 = external node)
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

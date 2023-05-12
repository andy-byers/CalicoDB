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
//     18      4     Number of pages in the DB
//     22      4     Freelist head
//     26      1     File format version
//     27      37    Reserved
struct FileHeader {
    static constexpr char kFmtString[18] = "CalicoDB format 1";
    static constexpr char kFmtVersion = 1;

    enum FieldOffset {
        kFormatStringOffset = 0,
        kPageCountOffset = 18,
        kFreelistHeadOffset = 22,
        kFmtVersionOfs = 26,
        kSize = 64
    };
};

// Node Header Format:
//     Offset  Size  Name
//    --------------------------
//     0       1     Flags byte (1 = external, internal otherwise)
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

auto bad_identifier_error(const Slice &bad_identifier, bool is_corruption = false) -> Status;

} // namespace calicodb

#endif // CALICODB_HEADER_H

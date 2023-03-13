// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_HEADER_H
#define CALICODB_HEADER_H

#include "types.h"

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
//     8       8     page_count
//     16      8     record_count
//     24      8     free_list_id
//     32      8     commit_lsn
//     40      2     page_size
struct FileHeader {
    static constexpr std::uint32_t kMagicCode {0xB11924E1};
    static constexpr std::size_t kSize {42};
    auto read(const char *data) -> void;
    auto write(char *data) const -> void;

    [[nodiscard]] auto compute_crc() const -> std::uint32_t;

    std::uint32_t magic_code {kMagicCode};
    std::uint32_t header_crc {};
    std::uint64_t page_count {};
    std::uint64_t record_count {};
    Id freelist_head;
    Lsn commit_lsn;
    std::uint16_t page_size {};
};

// Page Header Format:
//     Offset  Size  Name
//    --------------------------
//     0       8     page_lsn
static constexpr auto kPageHeaderSize = sizeof(Lsn);

// Node Header Format:
//     Offset  Size  Name
//    --------------------------
//     0       1     flags
//     1       8     next_id
//     9       8     prev_id
//     17      2     cell_count
//     19      2     cell_start
//     21      2     free_start
//     23      2     free_total
//     25      1     frag_count
struct NodeHeader {
    static constexpr std::size_t kSize {26};
    auto read(const char *data) -> void;
    auto write(char *data) const -> void;

    Id next_id;
    Id prev_id;
    std::uint16_t cell_count {};
    std::uint16_t cell_start {};
    std::uint16_t free_start {};
    std::uint16_t free_total {};
    std::uint8_t frag_count {};
    bool is_external {true};
};

} // namespace calicodb

#endif // CALICODB_HEADER_H

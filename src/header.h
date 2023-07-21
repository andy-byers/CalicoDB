// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_HEADER_H
#define CALICODB_HEADER_H

#include "encoding.h"
#include "utils.h"

namespace calicodb
{

// There are 4 page types in CalicoDB: nodes, freelist pages, overflow chain pages, and pointer
// map pages. Pages that store records or separator keys are called nodes, and pages that hold
// parent pointers for other pages are called pointer maps. Overflow chain pages store data that
// wasn't able to fit on a node page, as well as the page ID of the next page in the chain.
// Freelist pages are further subdivided into freelist trunk and leaf pages. Trunk pages store
// pointers to many leaf pages. Leaf pages are unused and their content is untracked.
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
struct FileHdr {
    static constexpr char kFmtString[18] = "CalicoDB format 1";
    static constexpr char kFmtVersion = 1;

    [[nodiscard]] static auto check_db_support(const char *root) -> Status;
    static auto make_supported_db(char *root) -> void;

    enum {
        kPageCountOffset = sizeof(kFmtString),
        kFreelistHeadOffset = kPageCountOffset + sizeof(U32),
        kFmtVersionOffset = kFreelistHeadOffset + sizeof(U32),
        kReservedOffset = kFmtVersionOffset + sizeof(char),
        kSize = kReservedOffset + 37
    };

    [[nodiscard]] static auto get_page_count(const char *root) -> U32
    {
        return get_u32(root + kPageCountOffset);
    }
    static auto put_page_count(char *root, U32 value) -> void
    {
        put_u32(root + kPageCountOffset, value);
    }

    [[nodiscard]] static auto get_freelist_head(const char *root) -> Id
    {
        return Id(get_u32(root + kFreelistHeadOffset));
    }
    static auto put_freelist_head(char *root, Id value) -> void
    {
        put_u32(root + kFreelistHeadOffset, value.value);
    }
};

// Node Header Format:
//     Offset  Size  Name
//    --------------------------
//     0       1     Node type
//     1       4     Next ID
//     5       4     Previous ID
//     9       2     Cell count
//     11      2     Cell area start
//     13      2     Freelist start
//     15      2     Freelist count
//     17      1     Fragment count
struct NodeHdr {
    enum Type : char {
        kExternal = '\x01',
        kInternal = '\x02',
    };

    enum {
        kTypeOffset,
        kNextIdOffset = kTypeOffset + sizeof(Type),
        kPrevIdOffset = kNextIdOffset + sizeof(U32),
        kCellCountOffset = kPrevIdOffset + sizeof(U32),
        kCellStartOffset = kCellCountOffset + sizeof(U16),
        kFreeStartOffset = kCellStartOffset + sizeof(U16),
        kFreeCountOffset = kFreeStartOffset + sizeof(U16),
        kFragCountOffset = kFreeCountOffset + sizeof(U16),
        kSize = kFragCountOffset + sizeof(U8)
    };

    [[nodiscard]] static auto get_type(const char *root) -> Type
    {
        return Type{root[kTypeOffset]};
    }
    static auto put_type(char *root, Type value) -> void
    {
        root[kTypeOffset] = value;
    }

    [[nodiscard]] static auto get_next_id(const char *root) -> Id
    {
        return Id(get_u32(root + kNextIdOffset));
    }
    static auto put_next_id(char *root, Id value) -> void
    {
        put_u32(root + kNextIdOffset, value.value);
    }

    [[nodiscard]] static auto get_prev_id(const char *root) -> Id
    {
        return Id(get_u32(root + kPrevIdOffset));
    }
    static auto put_prev_id(char *root, Id value) -> void
    {
        put_u32(root + kPrevIdOffset, value.value);
    }

    [[nodiscard]] static auto get_cell_count(const char *root) -> U32
    {
        return get_u16(root + kCellCountOffset);
    }
    static auto put_cell_count(char *root, U32 value) -> void
    {
        put_u16(root + kCellCountOffset, static_cast<U16>(value));
    }

    [[nodiscard]] static auto get_cell_start(const char *root) -> U32
    {
        return get_u16(root + kCellStartOffset);
    }
    static auto put_cell_start(char *root, U32 value) -> void
    {
        put_u16(root + kCellStartOffset, static_cast<U16>(value));
    }

    [[nodiscard]] static auto get_free_start(const char *root) -> U32
    {
        return get_u16(root + kFreeStartOffset);
    }
    static auto put_free_start(char *root, U32 value) -> void
    {
        put_u16(root + kFreeStartOffset, static_cast<U16>(value));
    }

    [[nodiscard]] static auto get_free_total(const char *root) -> U32
    {
        return get_u16(root + kFreeCountOffset);
    }
    static auto put_free_total(char *root, U32 value) -> void
    {
        put_u16(root + kFreeCountOffset, static_cast<U16>(value));
    }

    [[nodiscard]] static auto get_frag_count(const char *root) -> U32
    {
        // This cast prevents sign extension.
        return static_cast<U8>(root[kFragCountOffset]);
    }
    static auto put_frag_count(char *root, U32 value) -> void
    {
        root[kFragCountOffset] = static_cast<char>(value);
    }
};

} // namespace calicodb

#endif // CALICODB_HEADER_H

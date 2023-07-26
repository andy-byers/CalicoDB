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
//     26      4     Freelist length
//     30      1     File format version
//     31      33    Reserved
struct FileHdr {
    static constexpr char kFmtString[18] = "CalicoDB format 1";
    static constexpr char kFmtVersion = 1;

    [[nodiscard]] static auto check_db_support(const char *root) -> Status;
    static auto make_supported_db(char *root) -> void;

    enum {
        kPageCountOffset = sizeof(kFmtString),
        kFreelistHeadOffset = kPageCountOffset + sizeof(U32),
        kFreelistLengthOffset = kFreelistHeadOffset + sizeof(U32),
        kFmtVersionOffset = kFreelistLengthOffset + sizeof(U32),
        kReservedOffset = kFmtVersionOffset + sizeof(char),
        kSize = 64
    };
    static_assert(kReservedOffset < kSize);

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

    [[nodiscard]] static auto get_freelist_length(const char *root) -> U32
    {
        return get_u32(root + kFreelistLengthOffset);
    }
    static auto put_freelist_length(char *root, U32 value) -> void
    {
        put_u32(root + kFreelistLengthOffset, value);
    }
};

// Node Header Format:
//     Offset  Size  Name
//    --------------------------
//     0       1     Node type
//     1       2     Cell count
//     3       2     Cell area start
//     5       2     Freelist start
//     7       1     Fragment count
//     8       4     Next ID
// TODO: Only internal nodes need a "Next ID".
struct NodeHdr {
    enum Type : int {
        kInvalid = 0,
        kInternal = 1,
        kExternal = 2,
    };

    enum {
        kTypeOffset,
        kCellCountOffset = kTypeOffset + sizeof(Type),
        kCellStartOffset = kCellCountOffset + sizeof(U16),
        kFreeStartOffset = kCellStartOffset + sizeof(U16),
        kFragCountOffset = kFreeStartOffset + sizeof(U16),
        kNextIdOffset = kFragCountOffset + sizeof(char),
        kSize = kNextIdOffset + sizeof(U32)
    };

    [[nodiscard]] static auto get_type(const char *root) -> Type
    {
        switch (root[kTypeOffset]) {
            case kInternal:
                return kInternal;
            case kExternal:
                return kExternal;
            default:
                return kInvalid;
        }
    }
    static auto put_type(char *root, bool is_external) -> void
    {
        root[kTypeOffset] = static_cast<char>(kInternal + is_external);
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

    [[nodiscard]] static auto get_frag_count(const char *root) -> U32
    {
        // This cast prevents sign extension.
        return static_cast<U8>(root[kFragCountOffset]);
    }
    static auto put_frag_count(char *root, U32 value) -> void
    {
        root[kFragCountOffset] = static_cast<char>(value);
    }

    [[nodiscard]] static auto get_next_id(const char *root) -> Id
    {
        return Id(get_u32(root + kNextIdOffset));
    }
    static auto put_next_id(char *root, Id value) -> void
    {
        put_u32(root + kNextIdOffset, value.value);
    }
};

} // namespace calicodb

#endif // CALICODB_HEADER_H

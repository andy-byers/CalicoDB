// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_HEADER_H
#define CALICODB_HEADER_H

#include "encoding.h"
#include "internal.h"

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
//     30      4     Largest root
//     34      2     Page size
//     36      1     File format version
//     37      27    Reserved
struct FileHdr {
    static constexpr char kFmtString[18] = "CalicoDB format 1";
    static constexpr char kFmtVersion = 1;

    FileHdr() = delete;
    [[nodiscard]] static auto check_db_support(const char *root) -> Status;
    [[nodiscard]] static auto check_page_size(size_t page_size) -> Status;
    static auto make_supported_db(char *root, size_t page_size) -> void;

    enum {
        kPageCountOffset = sizeof(kFmtString),
        kFreelistHeadOffset = kPageCountOffset + sizeof(uint32_t),
        kFreelistLengthOffset = kFreelistHeadOffset + sizeof(uint32_t),
        kLargestRootOffset = kFreelistLengthOffset + sizeof(uint32_t),
        kPageSizeOffset = kLargestRootOffset + sizeof(uint32_t),
        kFmtVersionOffset = kPageSizeOffset + sizeof(uint16_t),
        kReservedOffset = kFmtVersionOffset + sizeof(char),
        kSize = 64
    };
    static_assert(kReservedOffset < kSize);

    [[nodiscard]] static auto get_page_count(const char *root) -> uint32_t
    {
        return get_u32(root + kPageCountOffset);
    }

    static auto put_page_count(char *root, uint32_t value) -> void
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

    [[nodiscard]] static auto get_freelist_length(const char *root) -> uint32_t
    {
        return get_u32(root + kFreelistLengthOffset);
    }

    static auto put_freelist_length(char *root, uint32_t value) -> void
    {
        put_u32(root + kFreelistLengthOffset, value);
    }

    [[nodiscard]] static auto get_largest_root(const char *root) -> Id
    {
        return Id(get_u32(root + kLargestRootOffset));
    }

    static auto put_largest_root(char *root, Id value) -> void
    {
        put_u32(root + kLargestRootOffset, value.value);
    }

    [[nodiscard]] static auto get_page_size(const char *root) -> uint32_t
    {
        return get_u16(root + kPageSizeOffset);
    }

    static auto put_page_size(char *root, uint32_t value) -> void
    {
        put_u16(root + kPageSizeOffset, static_cast<uint16_t>(value));
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
//     8       4     Next ID*
//
// * Only external nodes have this field.
struct NodeHdr {
    enum Type : char {
        kInvalid = 0,
        kInternal = 1,
        kExternal = 2,
    };

    enum {
        kTypeOffset,
        kCellCountOffset = kTypeOffset + sizeof(Type),
        kCellStartOffset = kCellCountOffset + sizeof(uint16_t),
        kFreeStartOffset = kCellStartOffset + sizeof(uint16_t),
        kFragCountOffset = kFreeStartOffset + sizeof(uint16_t),
        kNextIdOffset = kFragCountOffset + sizeof(char),
        kSizeExternal = kNextIdOffset
    };

    NodeHdr() = delete;

    [[nodiscard]] static constexpr auto size(bool is_external) -> uint32_t
    {
        return kSizeExternal + !is_external * sizeof(uint32_t);
    }

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

    [[nodiscard]] static auto get_cell_count(const char *root) -> uint32_t
    {
        return get_u16(root + kCellCountOffset);
    }
    static auto put_cell_count(char *root, uint32_t value) -> void
    {
        put_u16(root + kCellCountOffset, static_cast<uint16_t>(value));
    }

    [[nodiscard]] static auto get_cell_start(const char *root) -> uint32_t
    {
        return get_u16(root + kCellStartOffset);
    }
    static auto put_cell_start(char *root, uint32_t value) -> void
    {
        put_u16(root + kCellStartOffset, static_cast<uint16_t>(value));
    }

    [[nodiscard]] static auto get_free_start(const char *root) -> uint32_t
    {
        return get_u16(root + kFreeStartOffset);
    }
    static auto put_free_start(char *root, uint32_t value) -> void
    {
        put_u16(root + kFreeStartOffset, static_cast<uint16_t>(value));
    }

    [[nodiscard]] static auto get_frag_count(const char *root) -> uint32_t
    {
        // This cast prevents sign extension.
        return static_cast<uint8_t>(root[kFragCountOffset]);
    }
    static auto put_frag_count(char *root, uint32_t value) -> void
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

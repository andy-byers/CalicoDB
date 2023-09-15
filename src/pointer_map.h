// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_POINTER_MAP_H
#define CALICODB_POINTER_MAP_H

#include "internal.h"

namespace calicodb
{

class Pager;

struct PointerMap {
    enum Type : char {
        kEmpty,
        kTreeNode,
        kTreeRoot,
        kOverflowHead,
        kOverflowLink,
        kFreelistPage,
        kTypeCount
    };

    struct Entry {
        Id back_ptr;
        Type type = kEmpty;
    };

    // Return true if page "page_id" is a pointer map page, false otherwise.
    [[nodiscard]] static auto is_map(Id page_id, size_t page_size) -> bool
    {
        return lookup(page_id, page_size) == page_id;
    }

    // Return the page ID of the pointer map page that holds the back pointer for page "page_id",
    // Id::null() otherwise.
    [[nodiscard]] static auto lookup(Id page_id, size_t page_size) -> Id;

    // Read an entry from the pointer map.
    static auto read_entry(Pager &pager, Id page_id, Entry &entry_out) -> Status;

    // Write an entry to the pointer map.
    static auto write_entry(Pager &pager, Id page_id, Entry entry, Status &s) -> void;
};

} // namespace calicodb

#endif // CALICODB_POINTER_MAP_H

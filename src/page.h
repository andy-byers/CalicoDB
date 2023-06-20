// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_PAGE_H
#define CALICODB_PAGE_H

#include "header.h"

namespace calicodb
{

struct PageRef final {
    Id page_id;

    // Pointer to the start of the buffer slot containing the page data.
    char *page = nullptr;

    // Number of live copies of this page.
    unsigned refcount = 0;

    // Dirty list fields.
    PageRef *dirty = nullptr;
    PageRef *prev_dirty = nullptr;
    PageRef *next_dirty = nullptr;

    enum Flag {
        kNormal = 0,
        kDirty = 1,
        kExtra = 2,
    } flag = kNormal;
};

[[nodiscard]] inline auto page_offset(Id page_id) -> U32
{
    return FileHdr::kSize * page_id.is_root();
}

} // namespace calicodb

#endif // CALICODB_PAGE_H

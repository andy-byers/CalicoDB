// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_FREELIST_H
#define CALICODB_FREELIST_H

#include "internal.h"

namespace calicodb
{

class Pager;
struct PageRef;

class Freelist
{
public:
    // Add a `page` to the freelist
    // The page does not need to be marked dirty prior to calling this routine. If the page
    // is converted into a freelist leaf page, it doesn't need to be logged. If it becomes
    // a freelist trunk page, it will be marked dirty in this routine.
    static auto add(Pager &pager, PageRef *&page) -> Status;

    enum RemoveType {
        kRemoveAny,
        kRemoveExact,
    };

    // Attempt to remove a page from the freelist
    // If the freelist is empty, returns a status for which Status::is_invalid_argument()
    // evaluates to true.
    static auto remove(Pager &pager, RemoveType type, Id nearby, PageRef *&page_out) -> Status;

    // Make sure the freelist is consistent
    static auto assert_state(Pager &pager) -> bool;
};

} // namespace calicodb

#endif // CALICODB_FREELIST_H

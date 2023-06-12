// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_FREELIST_H
#define CALICODB_FREELIST_H

#include "utils.h"

namespace calicodb
{

class Pager;
struct PageRef;

struct Freelist {
    // Add a `page` to the freelist
    static auto push(Pager &pager, PageRef *&page) -> Status;

    // Attempt to remove a page from the freelist
    // If the freelist is empty, returns a status for which Status::is_invalid_argument()
    static auto pop(Pager &pager, Id &id_out) -> Status;

    // Make sure the freelist is consistent
    static auto assert_state(Pager &pager) -> bool;
};

} // namespace calicodb

#endif // CALICODB_FREELIST_H

// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_FREELIST_H
#define CALICODB_FREELIST_H

#include "page.h"

namespace calicodb
{

class Pager;

struct Freelist {
    static auto is_empty(Pager &pager) -> bool;
    static auto push(Pager &pager, Page page) -> Status;
    static auto pop(Pager &pager, Id &id_out) -> Status;
    static auto assert_state(Pager &pager) -> bool;
};

} // namespace calicodb

#endif // CALICODB_FREELIST_H

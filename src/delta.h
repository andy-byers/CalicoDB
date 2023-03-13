// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_PAGE_DELTA_H
#define CALICODB_PAGE_DELTA_H

#include "utils.h"
#include <vector>

namespace calicodb
{

struct PageDelta {
    std::size_t offset {};
    std::size_t size {};
};

// Join overlapping deltas in a sorted (by offset) vector. Makes sure that delta WAL records are minimally sized.
auto compress_deltas(std::vector<PageDelta> &deltas) -> std::size_t;

// Insert a delta into a sorted vector, possibly joining it with the first overlapping delta. Only resolves
// the first overlap it encounters, so some edge cases will be missed (delta that overlaps multiple other
// deltas). Rather than trying to cover these here, just call compress_deltas() after all deltas have been
// collected.
auto insert_delta(std::vector<PageDelta> &deltas, PageDelta delta) -> void;

} // namespace calicodb

#endif // CALICODB_PAGE_DELTA_H

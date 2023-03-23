// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "delta.h"
#include <algorithm>

namespace calicodb
{

static auto can_merge_ordered_deltas(const PageDelta &lhs, const PageDelta &rhs) -> bool
{
    CALICODB_EXPECT_LE(lhs.offset, rhs.offset);
    return rhs.offset <= lhs.offset + lhs.size;
}

static auto merge_deltas(const PageDelta &lhs, const PageDelta &rhs) -> PageDelta
{
    const auto rhs_end = rhs.offset + rhs.size;
    const auto new_dx = std::max(lhs.size, rhs_end - lhs.offset);
    return PageDelta {lhs.offset, new_dx};
}

auto compress_deltas(std::vector<PageDelta> &deltas) -> void
{
    if (deltas.size() <= 1) {
        return;
    }

    auto lhs = begin(deltas);
    for (auto rhs = next(lhs); rhs != end(deltas); ++rhs) {
        if (can_merge_ordered_deltas(*lhs, *rhs)) {
            *lhs = merge_deltas(*lhs, *rhs);
        } else {
            ++lhs;
            *lhs = *rhs;
        }
    }
    deltas.erase(next(lhs), end(deltas));
}

auto insert_delta(std::vector<PageDelta> &deltas, PageDelta delta) -> void
{
    if (delta.size == 0) {
        return;
    }
    auto after = std::upper_bound(
        begin(deltas), end(deltas), delta.offset,
        [](auto offset, const auto &rhs) {
            return offset < rhs.offset;
        });

    if (after != end(deltas)) {
        if (can_merge_ordered_deltas(delta, *after)) {
            *after = merge_deltas(delta, *after);
            return;
        }
    }
    if (after != begin(deltas)) {
        auto before = prev(after);
        if (can_merge_ordered_deltas(*before, delta)) {
            *before = merge_deltas(*before, delta);
            return;
        }
    }
    deltas.insert(after, delta);
}

} // namespace calicodb

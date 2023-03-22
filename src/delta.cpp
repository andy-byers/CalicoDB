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
    if (deltas.empty()) {
        deltas.emplace_back(delta);
        return;
    }
    auto itr = std::upper_bound(begin(deltas), end(deltas), delta, [](const auto &lhs, const auto &rhs) {
        return lhs.offset <= rhs.offset;
    });
    const auto try_merge = [&itr](const auto &lhs, const auto &rhs) {
        if (can_merge_ordered_deltas(lhs, rhs)) {
            *itr = merge_deltas(lhs, rhs);
            return true;
        }
        return false;
    };
    if (itr != end(deltas) && try_merge(delta, *itr)) {
        return;
    }
    if (itr != begin(deltas)) {
        --itr;
        if (try_merge(*itr, delta)) {
            return;
        }
        ++itr;
    }
    deltas.insert(itr, delta);
}

} // namespace calicodb

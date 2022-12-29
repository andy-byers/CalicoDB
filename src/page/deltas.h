#ifndef CALICO_PAGE_DELTAS_H
#define CALICO_PAGE_DELTAS_H

#include "core/recovery.h"
#include "utils/expect.h"
#include <algorithm>

namespace calico {

namespace impl {

    inline auto can_merge_ordered_deltas(const PageDelta &lhs, const PageDelta &rhs) -> bool
    {
        CALICO_EXPECT_LE(lhs.offset, rhs.offset);
        return rhs.offset <= lhs.offset + lhs.size;
    }

    inline auto merge_deltas(const PageDelta &lhs, const PageDelta &rhs) -> PageDelta
    {
        const auto rhs_end = rhs.offset + rhs.size;
        const auto new_dx = std::max(lhs.size, rhs_end - lhs.offset);
        return PageDelta {lhs.offset, new_dx};
    }

} // namespace impl

/*
 * Join overlapping deltas in a sorted (by offset) vector. Makes sure that delta WAL records are minimally sized.
 */
inline auto compress_deltas(std::vector<PageDelta> &deltas) -> void
{
    if (deltas.size() < 2)
        return;

    auto lhs = begin(deltas);
    for (auto rhs = next(lhs); rhs != end(deltas); ++rhs) {
        if (impl::can_merge_ordered_deltas(*lhs, *rhs)) {
            *lhs = impl::merge_deltas(*lhs, *rhs);
        } else {
            lhs++;
            *lhs = *rhs;
        }
    }
    deltas.erase(next(lhs), end(deltas));
}

/*
 * Insert a delta into a sorted vector, possibly joining it with the first overlapping delta. Only resolves
 * the first overlap it encounters, so some edge cases will be missed (delta that overlaps multiple other
 * deltas). Rather than trying to cover these here, just call compress_deltas() after all deltas have been
 * collected.
 */
inline auto insert_delta(std::vector<PageDelta> &deltas, PageDelta delta) -> void
{
    CALICO_EXPECT_GT(delta.size, 0);
    if (deltas.empty()) {
        deltas.emplace_back(delta);
        return;
    }
    auto itr = std::upper_bound(begin(deltas), end(deltas), delta, [](const auto &lhs, const auto &rhs) {
        return lhs.offset <= rhs.offset;
    });
    const auto try_merge = [&itr](const auto &lhs, const auto &rhs) {
        if (impl::can_merge_ordered_deltas(lhs, rhs)) {
            *itr = impl::merge_deltas(lhs, rhs);
            return true;
        }
        return false;
    };
    if (itr != end(deltas)) {
        if (try_merge(delta, *itr))
            return;
    }
    if (itr != begin(deltas)) {
        itr--;
        if (try_merge(*itr, delta))
            return;
        itr++;
    }
    deltas.insert(itr, delta);
}

} // namespace calico

#endif // CALICO_PAGE_DELTAS_H

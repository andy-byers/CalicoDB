#ifndef CALICO_PAGE_DELTAS_H
#define CALICO_PAGE_DELTAS_H

#include "utils/expect.h"
#include "wal/wal.h"

namespace calico {

namespace impl {

    inline auto can_merge_deltas(const PageDelta &lhs, const PageDelta &rhs) -> bool
    {
        // TODO: Could eliminate the first check and perhaps rename the function to
        //       reflect the precondition `lhs.offset <= rhs.offset`. We only call this on two
        //       deltas that are already ordered by the `offset` field. Maybe we could call
        //       it `can_merge_ordered_deltas(...)` or something.
        return lhs.offset <= rhs.offset && rhs.offset <= lhs.offset + lhs.size;
    }

    inline auto merge_deltas(const PageDelta &lhs, const PageDelta &rhs) -> PageDelta
    {
        const auto rhs_end = rhs.offset + rhs.size;
        const auto new_dx = std::max(lhs.size, rhs_end - lhs.offset);
        return PageDelta {lhs.offset, new_dx};
    }

} // namespace impl

inline auto compress_deltas(std::vector<PageDelta> &deltas) -> void
{
    if (deltas.size() < 2)
        return;

    auto lhs = begin(deltas);
    for (auto rhs = next(lhs); rhs != end(deltas); ++rhs) {
        if (impl::can_merge_deltas(*lhs, *rhs)) {
            *lhs = impl::merge_deltas(*lhs, *rhs);
        } else {
            lhs++;
            *lhs = *rhs;
        }
    }
    deltas.erase(next(lhs), end(deltas));
}

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
        if (impl::can_merge_deltas(lhs, rhs)) {
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

} // calico

#endif // CALICO_PAGE_DELTAS_H

#include "update.h"

#include <algorithm>

namespace cco {


namespace impl {
    auto can_merge(const Range &lhs, const Range &rhs) -> bool
    {
        // TODO: Could eliminate the first check and perhaps rename the function to
        //       reflect the precondition `lhs.x <= rhs.x`. We only call this on two
        //       ranges that are already ordered by the `x` field. Maybe we could call
        //       it `can_merge_ordered()` or something.
        return lhs.x <= rhs.x && rhs.x <= lhs.x + lhs.dx;
    }

    auto merge(const Range &lhs, const Range &rhs) -> Range
    {
        const auto rhs_end = rhs.x + rhs.dx;
        const auto new_dx = std::max(lhs.dx, rhs_end - lhs.x);
        return Range {lhs.x, new_dx};
    }

    auto compress_ranges(std::vector<Range> &ranges) -> void
    {
        if (ranges.size() < 2)
            return;

        auto lhs = ranges.begin();
        for (auto rhs = lhs + 1; rhs != ranges.end(); ++rhs) {
            if (can_merge(*lhs, *rhs)) {
                *lhs = merge(*lhs, *rhs);
            } else {
                lhs++;
                *lhs = *rhs;
            }
        }
        ranges.erase(++lhs, ranges.end());
    }

    auto insert_range(std::vector<Range> &ranges, Range range) -> void
    {
        if (ranges.empty()) {
            ranges.emplace_back(range);
            return;
        }
        auto itr = std::upper_bound(ranges.begin(), ranges.end(), range, [](const Range &lhs, const Range &rhs) {
            return lhs.x <= rhs.x;
        });
        const auto try_merge = [&itr] (const Range &lhs, const Range &rhs) {
            if (can_merge(lhs, rhs)) {
                *itr = merge(lhs, rhs);
                return true;
            }
            return false;
        };
        if (itr != ranges.end()) {
            if (try_merge(range, *itr))
                return;
        }
        if (itr != ranges.begin()) {
            itr--;
            if (try_merge(*itr, range))
                return;
            itr++;
        }
        ranges.insert(itr, range);
    }

} // impl

UpdateManager::UpdateManager(BytesView page, Bytes scratch):
    m_snapshot {scratch},
    m_current {page}
{
    CCO_EXPECT_EQ(page.size(), m_snapshot.size());
    mem_copy(scratch, m_current);
}

auto UpdateManager::push(Range range) -> void
{
    impl::insert_range(m_ranges, range);
}

auto UpdateManager::collect() -> std::vector<ChangedRegion>
{
    impl::compress_ranges(m_ranges);

    std::vector<ChangedRegion> update(m_ranges.size());
    auto itr = m_ranges.begin();
    for (auto &[offset, before, after]: update) {
        const auto &[x, dx] = *itr;
        offset = x;
        before = m_snapshot.range(x, dx);
        after = m_current.range(x, dx);
        itr++;
    }
    m_ranges.clear();
    return update;
}

} // cco




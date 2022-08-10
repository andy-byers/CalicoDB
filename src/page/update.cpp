#include "update.h"

#include <algorithm>

namespace cco {

namespace impl {
    using Range = PageChange;

    auto can_merge(const Range &lhs, const Range &rhs) -> bool
    {
        // TODO: Could eliminate the first check and perhaps rename the function to
        //       reflect the precondition `lhs.x <= rhs.x`. We only call this on two
        //       ranges that are already ordered by the `x` field. Maybe we could call
        //       it `can_merge_ordered()` or something.
        return lhs.offset <= rhs.offset && rhs.offset <= lhs.offset + lhs.size;
    }

    auto merge(const Range &lhs, const Range &rhs) -> Range
    {
        const auto rhs_end = rhs.offset + rhs.size;
        const auto new_dx = std::max(lhs.size, rhs_end - lhs.offset);
        return Range {lhs.offset, new_dx};
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
        auto itr = std::upper_bound(ranges.begin(), ranges.end(), range, [](const auto &lhs, const auto &rhs) {
            return lhs.offset <= rhs.offset;
        });
        const auto try_merge = [&itr](const auto &lhs, const auto &rhs) {
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

} // namespace impl

ChangeManager::ChangeManager(BytesView page, ManualScratch before, ManualScratch after)
    : m_before {before},
      m_after {after},
      m_current {page}
{
    CCO_EXPECT_EQ(m_current.size(), m_before.size());
    CCO_EXPECT_EQ(m_current.size(), m_after.size());
    mem_copy(m_before.data(), m_current);
}

auto ChangeManager::push_change(PageChange change) -> void
{
    impl::insert_range(m_changes, change);
}

auto ChangeManager::release_scratches(ManualScratchManager &manager) -> void
{
    // Beware! Don't use an instance after calling this method on it!
    manager.put(m_before);
    manager.put(m_after);
}

auto ChangeManager::collect_changes() -> std::vector<ChangedRegion>
{
    impl::compress_ranges(m_changes);

    // Copy all the data we need to reference off of the page. This lets us reuse the page buffer immediately, so that we can
    // push construction of WAL records to a background thread. Otherwise, the page needs to remain live and unaltered until
    // the "after" contents are written somewhere.
    mem_copy(m_after.data(), m_current);

    std::vector<ChangedRegion> update(m_changes.size());
    auto itr = m_changes.begin();
    for (auto &[offset, before, after]: update) {
        const auto &[x, dx] = *itr;
        offset = x;
        before = m_before.data().range(x, dx);
        after = m_after.data().range(x, dx);
        itr++;
    }
    m_changes.clear();
    return update;
}

} // namespace cco

#include <algorithm>
#include "update.h"

namespace cub {

namespace {

    using Range = UpdateManager::Range;

    auto can_merge(const Range &lhs, const Range &rhs)
    {
        // TODO: Could eliminate the first check and perhaps rename the function to
        //       reflect the precondition. We only call this on two ranges that are
        //       already ordered by the `x` field. Maybe we could call it
        //       `can_merge_ordered()` or something.
        return lhs.x <= rhs.x && rhs.x <= lhs.x + lhs.dx;
    }

    auto merge(const Range &lhs, const Range &rhs)
    {
        const auto rhs_end = rhs.x + rhs.dx;
        const auto new_dx = std::max(lhs.dx, rhs_end - lhs.x);
        return Range {lhs.x, new_dx};
    }

    auto compress_ranges(std::vector<Range> &ranges)
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


    // TODO: Check if O1-O3 move strings automatically when they are expiring.

    auto insert_range(std::vector<Range> &ranges, Range range)
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

} // <anonymous>

UpdateManager::UpdateManager(Scratch scratch)
    : m_snapshot {std::move(scratch)} {}

auto UpdateManager::has_changes() const -> bool
{
    return !m_ranges.empty();
}

auto UpdateManager::indicate_change(Index x, Size dx) -> void
{
    insert_range(m_ranges, {x, dx});
}

auto UpdateManager::collect_changes(BytesView snapshot) -> std::vector<ChangedRegion>
{
    compress_ranges(m_ranges);

    std::vector<ChangedRegion> update(m_ranges.size());
    auto itr = m_ranges.begin();
    for (auto &[offset, before, after]: update) {
        const auto &[x, dx] = *itr;
        offset = x;
        before = m_snapshot.data().range(offset, dx);
        after = snapshot.range(offset, dx);
        itr++;
    }
    m_ranges.clear();
    return update;
}

//#if CUB_BUILD_TESTS

namespace test {

    auto update_basic_assertions() -> int
    {
        // 0  1  2  3  4
        // |--------|
        // |--------|
        CUB_EXPECT_TRUE(can_merge({0, 3}, {0, 3}));
        const auto res_1 = merge({0, 3}, {0, 3});
        CUB_EXPECT_EQ(res_1.x, 0);
        CUB_EXPECT_EQ(res_1.dx, 3);

        // 0  1  2  3  4
        // |--------|
        // |-----|
        CUB_EXPECT_TRUE(can_merge({0, 3}, {0, 2}));
        const auto res_2 = merge({0, 3}, {0, 2});
        CUB_EXPECT_EQ(res_2.x, 0);
        CUB_EXPECT_EQ(res_2.dx, 3);

        // 0  1  2  3  4
        // |--------|
        // |-----------|
        CUB_EXPECT_TRUE(can_merge({0, 3}, {0, 4}));
        const auto res_3 = merge({0, 3}, {0, 4});
        CUB_EXPECT_EQ(res_3.x, 0);
        CUB_EXPECT_EQ(res_3.dx, 4);

        // 0  1  2  3  4
        // |--------|
        //    |--|
        CUB_EXPECT_TRUE(can_merge({0, 3}, {1, 1}));
        const auto res_4 = merge({0, 3}, {1, 1});
        CUB_EXPECT_EQ(res_4.x, 0);
        CUB_EXPECT_EQ(res_4.dx, 3);

        // 0  1  2  3  4
        // |--------|
        //    |-----|
        CUB_EXPECT_TRUE(can_merge({0, 3}, {1, 2}));
        const auto res_5 = merge({0, 3}, {1, 2});
        CUB_EXPECT_EQ(res_5.x, 0);
        CUB_EXPECT_EQ(res_5.dx, 3);

        // 0  1  2  3  4
        // |--------|
        //    |--------|
        CUB_EXPECT_TRUE(can_merge({0, 3}, {1, 3}));
        const auto res_6 = merge({0, 3}, {1, 3});
        CUB_EXPECT_EQ(res_6.x, 0);
        CUB_EXPECT_EQ(res_6.dx, 4);

        // 0  1  2  3  4
        // |--------|
        //          |--|
        CUB_EXPECT_TRUE(can_merge({0, 3}, {3, 1}));
        const auto res_7 = merge({0, 3}, {3, 1});
        CUB_EXPECT_EQ(res_7.x, 0);
        CUB_EXPECT_EQ(res_7.dx, 4);

        std::vector<Range> v {
            {0, 2},
            {4, 2},
            {7, 1},
            {8, 3},
        };

        // 0  1  2  3  4  5  6  7  8  9
        // |--------|
        //                |-----|
        //                         |--|

        const Range r {3, 1};


        insert_range(v, r);
        compress_ranges(v);

        return 0;
    }

} // test

//#endif // CUB_BUILD_TESTS

} // cub










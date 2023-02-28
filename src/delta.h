#ifndef CALICODB_PAGE_DELTA_H
#define CALICODB_PAGE_DELTA_H

#include "utils.h"
#include <vector>

namespace calicodb
{

struct PageDelta {
    Size offset {};
    Size size {};
};

using ChangeBuffer = std::vector<PageDelta>;

/*
 * Join overlapping deltas in a sorted (by offset) vector. Makes sure that delta WAL records are minimally sized.
 */
auto compress_deltas(ChangeBuffer &deltas) -> Size;

/*
 * Insert a delta into a sorted vector, possibly joining it with the first overlapping delta. Only resolves
 * the first overlap it encounters, so some edge cases will be missed (delta that overlaps multiple other
 * deltas). Rather than trying to cover these here, just call compress_deltas() after all deltas have been
 * collected.
 */
auto insert_delta(ChangeBuffer &deltas, PageDelta delta) -> void;

} // namespace calicodb

#endif // CALICODB_PAGE_DELTA_H

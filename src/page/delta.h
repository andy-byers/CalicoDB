#ifndef CALICO_PAGE_DELTA_H
#define CALICO_PAGE_DELTA_H

#include "core/recovery.h"

namespace Calico {

/*
 * Join overlapping deltas in a sorted (by offset) vector. Makes sure that delta WAL records are minimally sized.
 */
auto compress_deltas(std::vector<PageDelta> &deltas) -> Size;

/*
 * Insert a delta into a sorted vector, possibly joining it with the first overlapping delta. Only resolves
 * the first overlap it encounters, so some edge cases will be missed (delta that overlaps multiple other
 * deltas). Rather than trying to cover these here, just call compress_deltas() after all deltas have been
 * collected.
 */
auto insert_delta(std::vector<PageDelta> &deltas, PageDelta delta) -> void;

} // namespace Calico

#endif // CALICO_PAGE_DELTA_H

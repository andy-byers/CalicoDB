#ifndef CALICO_BENCHMARKS_H
#define CALICO_BENCHMARKS_H

#include "calico/slice.h"
#include "tools.h"
#include <algorithm>
#include <climits>
#include <random>

namespace Calico {

static constexpr Size DB_INITIAL_SIZE {100'000};
static constexpr Size DB_BATCH_SIZE {1'000};
static constexpr auto DB_VALUE = /* <100 characters> */
    " .## .##. #  # .## .##.  "
    " #   #  # #  # #   #  #  "
    " #   #### #  # #   #  #  "
    " '## #  # ## # '## '##'  ";
static constexpr Size DB_KEY_SIZE {16};

} // namespace Calico

#endif // CALICO_BENCHMARKS_H

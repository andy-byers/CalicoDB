#ifndef CALICO_BENCHMARKS_H
#define CALICO_BENCHMARKS_H

#include <algorithm>
#include <calico/slice.h>
#include <climits>
#include <random>
#include "tools.h"

namespace Calico {

static constexpr Size DB_INITIAL_SIZE {10'000};
static constexpr Size DB_BATCH_SIZE {1'000};
static constexpr Size DB_PAYLOAD_SIZE {100};
static constexpr auto DB_VALUE = /* <88 characters> */
    ".## .##. #  ::::::::::"
    "#   #  # #  # .## .##."
    "'## '#'# ## # #   #  #"
    "::::::::::: # '## '##'";
static constexpr Size DB_KEY_SIZE {12};

static_assert(DB_KEY_SIZE + std::char_traits<Byte>::length(DB_VALUE) == DB_PAYLOAD_SIZE);

} // namespace Calico

#endif // CALICO_BENCHMARKS_H

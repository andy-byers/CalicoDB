#ifndef CALICO_UTILS_RESULT_H
#define CALICO_UTILS_RESULT_H

#include "tl/expected.hpp"
#include "calico/status.h"

namespace calico {

template<class T>
using Result = tl::expected<T, Status>;
using Err = tl::unexpected<Status>;

} // namespace calico

#endif // CALICO_UTILS_RESULT_H

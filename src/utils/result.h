#ifndef CCO_UTILS_RESULT_H
#define CCO_UTILS_RESULT_H

#include "tl/expected.hpp"
#include "calico/status.h"

namespace cco {

template<class T>
using Result = tl::expected<T, Status>;
using Err = tl::unexpected<Status>;

} // namespace cco

#endif // CCO_UTILS_RESULT_H

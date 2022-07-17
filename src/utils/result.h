#ifndef CCO_UTILS_RESULT_H
#define CCO_UTILS_RESULT_H

// @TartanLlama/expected v1.0.0
#include "expected.hpp"

#include "calico/status.h"

namespace cco {

template<class T>
using Result = tl::expected<T, Status>;
using Err = tl::unexpected<Status>;

} // cco

#endif // CCO_UTILS_RESULT_H

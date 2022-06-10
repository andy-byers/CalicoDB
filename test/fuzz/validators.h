#ifndef CUB_FUZZ_VALIDATORS_H
#define CUB_FUZZ_VALIDATORS_H

#include "cub/common.h"

namespace cub {

class Database;

namespace fuzz {

auto validate_ordering(Database&) -> void;

} // cub

} // fuzz

#endif // CUB_FUZZ_VALIDATORS_H

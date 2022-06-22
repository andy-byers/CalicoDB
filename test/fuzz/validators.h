#ifndef CALICO_FUZZ_VALIDATORS_H
#define CALICO_FUZZ_VALIDATORS_H

#include "calico/options.h"

namespace calico {

class Database;

namespace fuzz {

auto validate_ordering(Database&) -> void;

} // calico

} // fuzz

#endif // CALICO_FUZZ_VALIDATORS_H

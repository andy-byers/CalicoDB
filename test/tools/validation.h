#ifndef CALICO_TREE_VALIDATION_H
#define CALICO_TREE_VALIDATION_H

#include "calico/common.h"

namespace calico {

class ITree;

auto validate_siblings(ITree&) -> void;
auto validate_ordering(ITree&) -> void;
auto validate_links(ITree&) -> void;

} // calico

#endif // CALICO_TREE_VALIDATION_H

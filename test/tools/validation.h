#ifndef CCO_TREE_VALIDATION_H
#define CCO_TREE_VALIDATION_H

#include "calico/common.h"

namespace cco {

class ITree;

auto validate_siblings(ITree&) -> void;
auto validate_ordering(ITree&) -> void;
auto validate_links(ITree&) -> void;

} // cco

#endif // CCO_TREE_VALIDATION_H

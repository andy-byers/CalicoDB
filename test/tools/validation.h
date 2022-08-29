#ifndef CALICO_TREE_VALIDATION_H
#define CALICO_TREE_VALIDATION_H

#include "calico/common.h"

namespace calico {

class Tree;

auto validate_siblings(Tree &) -> void;
auto validate_ordering(Tree &) -> void;
auto validate_links(Tree &) -> void;

auto print_keys(Tree &) -> void;

} // namespace calico

#endif // CALICO_TREE_VALIDATION_H

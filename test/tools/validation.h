#ifndef CCO_TREE_VALIDATION_H
#define CCO_TREE_VALIDATION_H

#include "calico/common.h"

namespace cco {

class Tree;

auto validate_siblings(Tree &) -> void;
auto validate_ordering(Tree &) -> void;
auto validate_links(Tree &) -> void;

auto print_keys(Tree &) -> void;

} // cco

#endif // CCO_TREE_VALIDATION_H

#ifndef CCO_TREE_VALIDATION_H
#define CCO_TREE_VALIDATION_H

#include "calico/common.h"

namespace cco {

class BPlusTree;

auto validate_siblings(BPlusTree &) -> void;
auto validate_ordering(BPlusTree &) -> void;
auto validate_links(BPlusTree &) -> void;

auto print_keys(BPlusTree &) -> void;

} // cco

#endif // CCO_TREE_VALIDATION_H

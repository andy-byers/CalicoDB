#ifndef CALICO_TREE_OVERFLOW_H
#define CALICO_TREE_OVERFLOW_H

#include "pager/page.h"
#include "utils/expected.hpp"

namespace Calico {

class Pager;
class FreeList;

[[nodiscard]] auto read_chain(Pager &pager, Id pid, Span out) -> tl::expected<void, Status>;
[[nodiscard]] auto write_chain(Pager &pager, FreeList &free_list, Slice overflow) -> tl::expected<Id, Status>;
[[nodiscard]] auto erase_chain(Pager &pager, FreeList &free_list, Id pid, Size size) -> tl::expected<void, Status>;

} // namespace Calico


#endif // CALICO_TREE_OVERFLOW_H

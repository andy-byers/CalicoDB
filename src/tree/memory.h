#ifndef CALICO_TREE_MEMORY_H
#define CALICO_TREE_MEMORY_H

#include "calico/status.h"
#include "pager/page.h"
#include "utils/expected.hpp"
#include "utils/types.h"

namespace Calico {

class Pager;

class FreeList {
    Pager *m_pager {};
    Id m_head;

public:
    friend class BPlusTree;

    explicit FreeList(Pager &pager)
        : m_pager {&pager} {}

    [[nodiscard]]
    auto is_empty() const -> bool
    {
        return m_head.is_null();
    }

    [[nodiscard]] auto pop() -> tl::expected<Page, Status>;
    [[nodiscard]] auto push(Page page) -> tl::expected<void, Status>;
    [[nodiscard]] auto vacuum(Size target) -> tl::expected<Size, Status>;
};

[[nodiscard]] auto read_chain(Pager &pager, Id pid, Span out) -> tl::expected<void, Status>;
[[nodiscard]] auto write_chain(Pager &pager, FreeList &free_list, Slice overflow) -> tl::expected<Id, Status>;
[[nodiscard]] auto erase_chain(Pager &pager, FreeList &free_list, Id pid, Size size) -> tl::expected<void, Status>;

} // namespace Calico

#endif // CALICO_TREE_MEMORY_H

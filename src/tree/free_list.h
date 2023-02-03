#ifndef CALICO_TREE_FREE_LIST_H
#define CALICO_TREE_FREE_LIST_H

#include "calico/status.h"
#include "pager/page.h"
#include "utils/expected.hpp"
#include "utils/types.h"
#include <optional>

namespace Calico {

class Pager;

class FreeList {
    Pager *m_pager {};
    Id m_head;

public:
    friend class BPlusTree;

    explicit FreeList(Pager &pager)
        : m_pager {&pager} {}

    [[nodiscard]] auto is_empty() const -> bool
    {
        return m_head.is_null();
    }

    [[nodiscard]] auto pop() -> tl::expected<Page, Status>;
    auto push(Page page) -> void;
};

} // namespace Calico

#endif // CALICO_TREE_FREE_LIST_H

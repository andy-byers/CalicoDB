#ifndef CALICO_TREE_FREE_LIST_H
#define CALICO_TREE_FREE_LIST_H

#include "calico/status.h"
#include "tree.h"
#include "utils/types.h"
#include <optional>

namespace calico {

class Pager;

class FreeList {
public:
    ~FreeList() = default;

    explicit FreeList(Pager &pager)
        : m_pager {&pager} {}

    [[nodiscard]] auto is_empty() const -> bool
    {
        return m_head.is_null();
    }

    [[nodiscard]] auto push(Page page) -> Result<void>;
    [[nodiscard]] auto pop() -> Result<Page>;
    auto save_state(FileHeader &header) const -> void;
    auto load_state(const FileHeader &header) -> void;

private:
    Pager *m_pager;
    identifier m_head;
};

} // namespace calico

#endif // CALICO_TREE_FREE_LIST_H

#ifndef CALICO_TREE_FREE_LIST_H
#define CALICO_TREE_FREE_LIST_H

#include "calico/status.h"
#include "tree.h"
#include "utils/types.h"
#include <optional>

namespace Calico {

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

    [[nodiscard]] auto push(Page page) -> tl::expected<void, Status>;
    [[nodiscard]] auto pop() -> tl::expected<Page, Status>;
    auto save_state(FileHeader &header) const -> void;
    auto load_state(const FileHeader &header) -> void;

private:
    Pager *m_pager;
    Id m_head;
};

} // namespace Calico

#endif // CALICO_TREE_FREE_LIST_H

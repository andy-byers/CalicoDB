#ifndef CALICO_TREE_FREE_LIST_H__
#define CALICO_TREE_FREE_LIST_H__

#include "calico/status.h"
#include "tree.h"
#include "utils/types.h"
#include <optional>

namespace Calico {

class Pager;

class FreeList__ {
public:
    ~FreeList__() = default;

    explicit FreeList__(Pager &pager)
        : m_pager {&pager} {}

    [[nodiscard]] auto is_empty() const -> bool
    {
        return m_head.is_null();
    }

    [[nodiscard]] auto push(Page_ page) -> tl::expected<void, Status>;
    [[nodiscard]] auto pop() -> tl::expected<Page_, Status>;
    auto save_state(FileHeader__ &header) const -> void;
    auto load_state(const FileHeader__ &header) -> void;

private:
    Pager *m_pager;
    Id m_head;
};

} // namespace Calico

#endif // CALICO_TREE_FREE_LIST_H__

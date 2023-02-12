#ifndef CALICO_TREE_MEMORY_H
#define CALICO_TREE_MEMORY_H

#include "calico/status.h"
#include "pager/page.h"
#include "utils/expected.hpp"
#include "utils/types.h"

namespace Calico {

class Pager;

/*
 *
 */
class PointerMap {
    Size m_usable_size {};

public:
    enum Type: Byte {
        NODE = 1,
        OVERFLOW_LINK,
        FREELIST_LINK,
    };

    struct Entry {
        Id back_ptr;
        Type type {};
    };

    explicit PointerMap(Size page_size)
        : m_usable_size {page_size - sizeof(Lsn)}
    {}

    [[nodiscard]] auto lookup_map(Id pid) const -> Id;
    [[nodiscard]] auto read_entry(const Page &map, Id pid) -> Entry;
    auto write_entry(Pager *pager, Page &map, Id pid, Entry entry) -> void;
};

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
    auto push(Page page) -> void;
};

[[nodiscard]] auto read_chain(Pager &pager, Id pid, Span out) -> tl::expected<void, Status>;
[[nodiscard]] auto write_chain(Pager &pager, FreeList &free_list, Slice overflow) -> tl::expected<Id, Status>;
[[nodiscard]] auto erase_chain(Pager &pager, FreeList &free_list, Id pid, Size size) -> tl::expected<void, Status>;

} // namespace Calico

#endif // CALICO_TREE_MEMORY_H

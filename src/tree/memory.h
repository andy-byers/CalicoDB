#ifndef CALICO_TREE_MEMORY_H
#define CALICO_TREE_MEMORY_H

#include "calico/status.h"
#include "pager/page.h"
#include "utils/expected.hpp"
#include "utils/types.h"

namespace Calico {

class Pager;

/* Pointer map management. Most pages in the database have a parent page. For node pages, the parent is clear:
 * it is the page that contains a child reference to the current page. For non-node pages, i.e. overflow links and
 * freelist links, the parent is the link that came before it. For overflow links, the parent of the first link
 * is the node page that the chain originated in. The only 2 pages that don't have a parent are the root page and
 * the head of the freelist.
 *
 * Special care must be taken to ensure that the pointer maps stay correct. Pointer map entries must be updated in
 * the following situations:
 *     (1) A parent-child tree connection is changed
 *     (2) A cell with an overflow chain is moved between (external) nodes
 *     (3) During all freelist and some overflow chain operations
 *
 * NOTE: The purpose of this data structure is to make the vacuum operation possible. It lets us swap any 2 pages,
 *       and easily update the pages that reference them. This lets us swap freelist pages with pages from the end
 *       of the file, after which the file can be truncated.
 */
class PointerMap {
    Pager *m_pager {};

public:
    enum Type: Byte {
        NODE = 1,
        OVERFLOW_HEAD,
        OVERFLOW_LINK,
        FREELIST_LINK,
    };

    struct Entry {
        Id back_ptr;
        Type type {};
    };

    explicit PointerMap(Pager &pager)
        : m_pager {&pager}
    {}

    // Find the page ID of the pointer map that holds the back pointer for page "pid".
    [[nodiscard]] auto lookup(Id pid) -> Id;

    // Read an entry from a pointer map.
    [[nodiscard]] auto read_entry(Id pid) -> tl::expected<Entry, Status>;

    // Write an entry to a pointer map.
    [[nodiscard]] auto write_entry(Id pid, Entry entry) -> tl::expected<void, Status>;
};

/* Freelist management. The freelist is essentially a linked list that is threaded through the database. Each freelist
 * link page contains a pointer to the next freelist link page, or to Id::null() if it is the last link. Pages that are
 * no longer needed by the tree are placed at the front of the freelist. When more pages are needed, the freelist is
 * checked first. Only if it is empty do we allocate a page from the end of the file.
 */
class FreeList {
    friend class BPlusTree;

    Pager *m_pager {};
    PointerMap *m_pointers {};
    Id m_head;

public:
    explicit FreeList(Pager &pager, PointerMap &pointers)
        : m_pager {&pager},
          m_pointers {&pointers}
    {}

    [[nodiscard]]
    auto is_empty() const -> bool
    {
        return m_head.is_null();
    }

    [[nodiscard]] auto pop() -> tl::expected<Page, Status>;
    [[nodiscard]] auto push(Page page) -> tl::expected<void, Status>;
};

/* Overflow chain management. The tree engine attempts to store all data in external node pages. If the user inserts
 * a record that is too large, part of the payload value (not the key) will be transferred to one or more overflow
 * chain pages. Like the freelist, an overflow chain forms a singly-linked list of pages. The difference is that
 * each overflow chain page contains both a pointer and payload data, while each freelist page only contains a
 * "next" pointer.
 */
class OverflowList {
    Pager *m_pager {};
    FreeList *m_freelist {};
    PointerMap *m_pointers {};

    std::string m_scratch; // TODO: Only needed for copy_chain(). Not actually necessary, it just makes it easier. Should fix that at some point.

public:
    explicit OverflowList(Pager &pager, FreeList &freelist, PointerMap &pointers)
        : m_pager {&pager},
          m_freelist {&freelist},
          m_pointers {&pointers} {}

    [[nodiscard]] auto write_chain(Id pid, Slice first, Slice second = {}) -> tl::expected<Id, Status>;
    [[nodiscard]] auto copy_chain(Id pid, Id overflow_id, Size size) -> tl::expected<Id, Status>;
    [[nodiscard]] auto read_chain(Id pid, Span out) -> tl::expected<void, Status>;
    [[nodiscard]] auto erase_chain(Id pid) -> tl::expected<void, Status>;
};

[[nodiscard]] auto read_next_id(const Page &page) -> Id;
auto write_next_id(Page &page, Id) -> void;

} // namespace Calico

#endif // CALICO_TREE_MEMORY_H

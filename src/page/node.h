#ifndef CALICO_PAGE_NODE_H
#define CALICO_PAGE_NODE_H

#include "cell.h"
#include "page.h"

namespace Calico {

class NodeHeader__ final {
public:
    [[nodiscard]] static auto parent_id(const Page_ &) -> Id;
    [[nodiscard]] static auto right_sibling_id(const Page_ &) -> Id;
    [[nodiscard]] static auto rightmost_child_id(const Page_ &) -> Id;
    [[nodiscard]] static auto left_sibling_id(const Page_ &) -> Id;
    [[nodiscard]] static auto reserved(const Page_ &) -> std::uint64_t;
    [[nodiscard]] static auto cell_count(const Page_ &) -> Size;
    [[nodiscard]] static auto cell_start(const Page_ &) -> Size;
    [[nodiscard]] static auto frag_count(const Page_ &) -> Size;
    [[nodiscard]] static auto free_start(const Page_ &) -> Size;
    [[nodiscard]] static auto free_total(const Page_ &) -> Size;
    static auto set_parent_id(Page_ &, Id) -> void;
    static auto set_right_sibling_id(Page_ &, Id) -> void;
    static auto set_rightmost_child_id(Page_ &, Id) -> void;
    static auto set_left_sibling_id(Page_ &, Id) -> void;
    static auto set_cell_count(Page_ &, Size) -> void;
    static auto set_cell_start(Page_ &, Size) -> void;
    static auto set_frag_count(Page_ &, Size) -> void;
    static auto set_free_start(Page_ &, Size) -> void;
    static auto set_free_total(Page_ &, Size) -> void;

    [[nodiscard]] static auto gap_size(const Page_ &) -> Size;
    [[nodiscard]] static auto max_usable_space(const Page_ &) -> Size;
    [[nodiscard]] static auto cell_area_offset(const Page_ &) -> Size;
    [[nodiscard]] static auto cell_directory_offset(const Page_ &) -> Size;
    [[nodiscard]] static auto header_offset(const Page_ &) -> Size;
};

class CellDirectory final {
public:
    struct Pointer {
        Size value;
    };

    [[nodiscard]] static auto get_pointer(const Page_ &, Size) -> Pointer;
    static auto set_pointer(Page_ &, Size, Pointer) -> void;
    static auto insert_pointer(Page_ &, Size, Pointer) -> void;
    static auto remove_pointer(Page_ &, Size) -> void;
};

class BlockAllocator__ final {
public:
    ~BlockAllocator__() = default;
    [[nodiscard]] static auto usable_space(const Page_ &) -> Size;
    static auto allocate(Page_ &, Size) -> Size;
    static auto free(Page_ &, Size, Size) -> void;
    static auto reset(Page_ &) -> void;

private:
    static constexpr Size FREE_BLOCK_HEADER_SIZE {2 * sizeof(std::uint16_t)};

    [[nodiscard]] static auto get_next_pointer(const Page_ &, Size) -> Size;
    [[nodiscard]] static auto get_block_size(const Page_ &, Size) -> Size;
    static auto set_next_pointer(Page_ &, Size, Size) -> void;
    static auto set_block_size(Page_ &, Size, Size) -> void;
    static auto allocate_from_gap(Page_ &, Size) -> Size;
    static auto allocate_from_free(Page_ &, Size) -> Size;
    static auto take_free_space(Page_ &, Size, Size, Size) -> Size;
};

class Node__ final {
public:
    struct FindGeResult {
        Size index {};
        bool found_eq {};
    };

    friend class Iterator;

    Node__(Page_ page, bool reset_header, Byte *scratch)
        : m_page {std::move(page)},
          m_scratch {scratch, m_page.size()}
    {
        reset(reset_header);
    }

    [[nodiscard]] auto id() const -> Id
    {
        return m_page.id();
    }

    [[nodiscard]] auto size() const -> Size
    {
        return m_page.size();
    }

    [[nodiscard]] auto type() const -> PageType
    {
        return m_page.type();
    }

    [[nodiscard]] auto page() const -> const Page_ &
    {
        return m_page;
    }

    auto page() -> Page_ &
    {
        return m_page;
    }

    auto take() -> Page_
    {
        return std::move(m_page);
    }

    [[nodiscard]] auto read_key(Size) const -> Slice;
    [[nodiscard]] auto read_cell(Size) const -> Cell__;
    [[nodiscard]] auto detach_cell(Size, Span) const -> Cell__;
    [[nodiscard]] auto find_ge(const Slice &key) const -> FindGeResult;
    auto remove(Size index, Size size) -> void;
    auto defragment() -> void;
    [[nodiscard]] auto overflow_cell() const -> const Cell__ &;

    auto set_overflow_cell(Cell__ cell, Size index) -> void;
    auto take_overflow_cell() -> Cell__;
    [[nodiscard]] auto is_overflowing() const -> bool;

    auto extract_cell(Size, Span) -> Cell__;
    auto insert(Cell__) -> void;
    auto insert(Size index, Cell__ cell) -> void;
    auto remove(const Slice &key) -> bool;
    [[nodiscard]] auto is_underflowing() const -> bool;
    [[nodiscard]] auto is_external() const -> bool;
    [[nodiscard]] auto child_id(Size) const -> Id;
    [[nodiscard]] auto parent_id() const -> Id;
    [[nodiscard]] auto right_sibling_id() const -> Id;
    [[nodiscard]] auto left_sibling_id() const -> Id;
    [[nodiscard]] auto rightmost_child_id() const -> Id;
    [[nodiscard]] auto cell_count() const -> Size;
    auto set_parent_id(Id) -> void;
    auto set_right_sibling_id(Id) -> void;
    auto set_left_sibling_id(Id id) -> void
    {
        return NodeHeader__::set_left_sibling_id(m_page, id);
    }
    auto set_rightmost_child_id(Id) -> void;
    auto set_child_id(Size, Id) -> void;

    [[nodiscard]] auto usable_space() const -> Size;
    [[nodiscard]] auto max_usable_space() const -> Size;
    [[nodiscard]] auto cell_area_offset() const -> Size;
    [[nodiscard]] auto header_offset() const -> Size;
    [[nodiscard]] auto overflow_index() const -> Size {return m_overflow_index;}
    auto reset(bool = false) -> void;

    auto TEST_validate() const -> void;

    [[nodiscard]] auto emplace(Size index, const Slice &key, const Slice &value, const PayloadMeta &meta) -> bool;

private:
    [[nodiscard]] auto allocate(Size index, Size size) -> Span;
    [[nodiscard]] auto make_room(Size, std::optional<Size>) -> Size;
    auto defragment(std::optional<Size>) -> void;

    Page_ m_page;
    std::optional<Cell__> m_overflow_cell {};
    Size m_overflow_index {};
    Span m_scratch;
};

[[nodiscard]] auto can_merge_siblings(const Node__ &, const Node__ &, const Cell__ &) -> bool;
auto merge_left(Node__ &, Node__ &, Node__ &, Size) -> void;
auto merge_right(Node__ &, Node__ &, Node__ &, Size) -> void;
auto merge_root(Node__ &, Node__ &) -> void;
auto split_root(Node__ &, Node__ &) -> void;
[[nodiscard]] auto split_non_root(Node__ &, Node__ &, Span) -> Cell__;

} // namespace Calico

#endif // CALICO_PAGE_NODE_H

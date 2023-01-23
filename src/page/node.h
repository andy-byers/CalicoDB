#ifndef CALICO_PAGE_NODE_H
#define CALICO_PAGE_NODE_H

#include "cell.h"
#include "page.h"

namespace Calico {

class NodeHeader final {
public:
    [[nodiscard]] static auto parent_id(const Page&) -> Id;
    [[nodiscard]] static auto right_sibling_id(const Page&) -> Id;
    [[nodiscard]] static auto rightmost_child_id(const Page&) -> Id;
    [[nodiscard]] static auto left_sibling_id(const Page&) -> Id;
    [[nodiscard]] static auto reserved(const Page&) -> std::uint64_t;
    [[nodiscard]] static auto cell_count(const Page&) -> Size;
    [[nodiscard]] static auto cell_start(const Page&) -> Size;
    [[nodiscard]] static auto frag_count(const Page&) -> Size;
    [[nodiscard]] static auto free_start(const Page&) -> Size;
    [[nodiscard]] static auto free_total(const Page&) -> Size;
    static auto set_parent_id(Page&, Id) -> void;
    static auto set_right_sibling_id(Page&, Id) -> void;
    static auto set_rightmost_child_id(Page&, Id) -> void;
    static auto set_left_sibling_id(Page&, Id) -> void;
    static auto set_cell_count(Page&, Size) -> void;
    static auto set_cell_start(Page&, Size) -> void;
    static auto set_frag_count(Page&, Size) -> void;
    static auto set_free_start(Page&, Size) -> void;
    static auto set_free_total(Page&, Size) -> void;

    [[nodiscard]] static auto gap_size(const Page&) -> Size;
    [[nodiscard]] static auto max_usable_space(const Page&) -> Size;
    [[nodiscard]] static auto cell_area_offset(const Page&) -> Size;
    [[nodiscard]] static auto cell_directory_offset(const Page&) -> Size;
    [[nodiscard]] static auto header_offset(const Page&) -> Size;
};

class CellDirectory final {
public:
    struct Pointer {
        Size value;
    };

    [[nodiscard]] static auto get_pointer(const Page&, Size) -> Pointer;
    static auto set_pointer(Page&, Size, Pointer) -> void;
    static auto insert_pointer(Page&, Size, Pointer) -> void;
    static auto remove_pointer(Page&, Size) -> void;
};

class BlockAllocator final {
public:
    ~BlockAllocator() = default;
    [[nodiscard]] static auto usable_space(const Page&) -> Size;
    static auto allocate(Page&, Size) -> Size;
    static auto free(Page&, Size, Size) -> void;
    static auto reset(Page&) -> void;

private:
    static constexpr Size FREE_BLOCK_HEADER_SIZE {2 * sizeof(std::uint16_t)};

    [[nodiscard]] static auto get_next_pointer(const Page&, Size) -> Size;
    [[nodiscard]] static auto get_block_size(const Page&, Size) -> Size;
    static auto set_next_pointer(Page&, Size, Size) -> void;
    static auto set_block_size(Page&, Size, Size) -> void;
    static auto allocate_from_gap(Page&, Size) -> Size;
    static auto allocate_from_free(Page&, Size) -> Size;
    static auto take_free_space(Page&, Size, Size, Size) -> Size;
};

class Node final {
public:
    struct FindGeResult {
        Size index {};
        bool found_eq {};
    };

    friend class Iterator;

    Node(Page page, bool reset_header, Byte *scratch)
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

    [[nodiscard]] auto page() const -> const Page &
    {
        return m_page;
    }

    auto page() -> Page &
    {
        return m_page;
    }

    auto take() -> Page
    {
        return std::move(m_page);
    }

    [[nodiscard]] auto read_key(Size) const -> Slice;
    [[nodiscard]] auto read_cell(Size) const -> Cell;
    [[nodiscard]] auto detach_cell(Size, Span) const -> Cell;
    [[nodiscard]] auto find_ge(const Slice &key) const -> FindGeResult;
    auto remove(Size index, Size size) -> void;
    auto defragment() -> void;
    [[nodiscard]] auto overflow_cell() const -> const Cell &;

    auto set_overflow_cell(Cell cell, Size index) -> void;
    auto take_overflow_cell() -> Cell;
    [[nodiscard]] auto is_overflowing() const -> bool;

    auto extract_cell(Size, Span) -> Cell;
    auto insert(Cell) -> void;
    auto insert(Size index, Cell cell) -> void;
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
        return NodeHeader::set_left_sibling_id(m_page, id);
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

private:
    [[nodiscard]] auto allocate(Size index, Size size) -> Size;
    [[nodiscard]] auto make_room(Size, std::optional<Size>) -> Size;
    auto defragment(std::optional<Size>) -> void;

    Page m_page;
    std::optional<Cell> m_overflow_cell {};
    Size m_overflow_index {};
    Span m_scratch;
};

[[nodiscard]] auto can_merge_siblings(const Node &, const Node &, const Cell &) -> bool;
auto merge_left(Node &, Node &, Node &, Size) -> void;
auto merge_right(Node &, Node &, Node &, Size) -> void;
auto merge_root(Node &, Node &) -> void;
auto split_root(Node &, Node &) -> void;
[[nodiscard]] auto split_non_root(Node &, Node &, Span) -> Cell;

} // namespace Calico

#endif // CALICO_PAGE_NODE_H

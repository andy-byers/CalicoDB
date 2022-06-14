#ifndef CUB_PAGE_NODE_H
#define CUB_PAGE_NODE_H

#include "cell.h"
#include "page.h"

namespace cub {

class Node final {
public:
    struct SearchResult {
        Index index{};
        bool found_eq{};
    };

    Node(Page, bool);
    ~Node() = default;

    [[nodiscard]] auto id() const -> PID
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

    [[nodiscard]] auto page() const -> const Page&
    {
        return m_page;
    }

    auto page() -> Page&
    {
        return m_page;
    }

    auto take() -> Page
    {
        return std::move(m_page);
    }

    [[nodiscard]] auto read_key(Index) const -> BytesView;
    [[nodiscard]] auto read_cell(Index) const -> Cell;
    [[nodiscard]] auto detach_cell(Index, Scratch) const -> Cell;
    [[nodiscard]] auto find_ge(BytesView) const -> SearchResult;
    auto extract_cell(Index, Scratch) -> Cell;
    auto insert(Cell) -> void;
    auto insert_at(Index, Cell) -> void;
    auto remove(BytesView) -> bool;
    auto remove_at(Index, Size) -> void;
    auto defragment() -> void;

    [[nodiscard]] auto overflow_cell() const -> const Cell&;
    auto set_overflow_cell(Cell) -> void;
    auto take_overflow_cell() -> Cell;

    [[nodiscard]] auto is_overflowing() const -> bool;
    [[nodiscard]] auto is_underflowing() const -> bool;
    [[nodiscard]] auto is_underflowing_() const -> bool; // TODO: Prep for `proactive_merges`
    [[nodiscard]] auto is_external() const -> bool;
    [[nodiscard]] auto child_id(Index) const -> PID;

    // Public header fields.
    [[nodiscard]] auto header_crc() const -> Index;
    [[nodiscard]] auto parent_id() const -> PID;
    [[nodiscard]] auto right_sibling_id() const -> PID;
    [[nodiscard]] auto rightmost_child_id() const -> PID;
    [[nodiscard]] auto cell_count() const -> Size;
    auto update_header_crc() -> void;
    auto set_parent_id(PID) -> void;
    auto set_right_sibling_id(PID) -> void;
    auto set_rightmost_child_id(PID) -> void;
    auto set_child_id(Index, PID) -> void;

    [[nodiscard]] auto usable_space() const -> Size;
    [[nodiscard]] auto max_usable_space() const -> Size;
    [[nodiscard]] auto cell_area_offset() const -> Size;
    [[nodiscard]] auto cell_pointers_offset() const -> Size;
    [[nodiscard]] auto header_offset() const -> Index;
    auto reset(bool = false) -> void;

    Node(Node&&) = default;
    auto operator=(Node&&) -> Node& = default;

private:
    [[nodiscard]] auto free_count() const -> Size;
    [[nodiscard]] auto cell_start() const -> Index;
    [[nodiscard]] auto free_start() const -> Index;
    [[nodiscard]] auto frag_count() const -> Size;
    auto set_cell_count(Size) -> void;
    auto set_free_count(Size) -> void;
    auto set_cell_start(Index) -> void;
    auto set_free_start(Index) -> void;
    auto set_frag_count(Size) -> void;

    [[nodiscard]] auto gap_size() const -> Size;
    [[nodiscard]] auto cell_pointer(Index) const -> Index;
    auto recompute_usable_space() -> void;
    auto set_cell_pointer(Index, Index) -> void;
    auto insert_cell_pointer(Index, Index) -> void;
    auto remove_cell_pointer(Index) -> void;
    auto defragment(std::optional<Index>) -> void;
    auto allocate(Size, std::optional<Index>) -> Index;
    auto allocate_from_gap(Size) -> Index;
    auto allocate_from_free(Size) -> Index;
    auto take_free_space(Index, Index, Size) -> Index;
    auto give_free_space(Index, Size) -> void;

    Page m_page;
    std::optional<Cell> m_overflow{};
    Size m_usable_space{};
};

auto transfer_cell(Node&, Node&, Index) -> void;
auto can_merge_siblings(const Node&, const Node&, const Cell&) -> bool;
auto merge_left(Node&, Node&, Node&, Index) -> void;
auto merge_right(Node&, Node&, Node&, Index) -> void;

} // cub

#endif // CUB_PAGE_NODE_H

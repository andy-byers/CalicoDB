#ifndef CCO_PAGE_NODE_H
#define CCO_PAGE_NODE_H

#include "cell.h"
#include "page.h"

namespace cco {

class NodeHeader final {
public:
    [[nodiscard]] auto parent_id() const -> PID;
    [[nodiscard]] auto right_sibling_id() const -> PID;
    [[nodiscard]] auto rightmost_child_id() const -> PID;
    [[nodiscard]] auto left_sibling_id() const -> PID;
    [[nodiscard]] auto reserved() const -> uint32_t;
    [[nodiscard]] auto cell_count() const -> Size;
    [[nodiscard]] auto cell_start() const -> Index;
    [[nodiscard]] auto frag_count() const -> Size;
    [[nodiscard]] auto free_count() const -> Size;
    [[nodiscard]] auto free_start() const -> Index;
    [[nodiscard]] auto free_total() const -> Size;
    auto set_parent_id(PID) -> void;
    auto set_right_sibling_id(PID) -> void;
    auto set_rightmost_child_id(PID) -> void;
    auto set_left_sibling_id(PID) -> void;
    auto set_cell_count(Size) -> void;
    auto set_cell_start(Index) -> void;
    auto set_frag_count(Size) -> void;
    auto set_free_count(Size) -> void;
    auto set_free_start(Index) -> void;
    auto set_free_total(Size) -> void;

    [[nodiscard]] auto gap_size() const -> Size;
    [[nodiscard]] auto max_usable_space() const -> Size;
    [[nodiscard]] auto cell_area_offset() const -> Size;
    [[nodiscard]] auto cell_directory_offset() const -> Size;
    [[nodiscard]] auto header_offset() const -> Index;

    explicit NodeHeader(Page &page)
        : m_page {&page} {}

    [[nodiscard]] auto page() const -> const Page&
    {
        return *m_page;
    }

    auto page() -> Page&
    {
        return *m_page;
    }

private:
    Page *m_page {};
};

class CellDirectory final {
public:
    struct Pointer {
        Index value;
    };

    explicit CellDirectory(NodeHeader &header)
        : m_page {&header.page()},
          m_header {&header} {}

    [[nodiscard]] auto get_pointer(Index) const -> Pointer;
    auto set_pointer(Index, Pointer) -> void;
    auto insert_pointer(Index, Pointer) -> void;
    auto remove_pointer(Index) -> void;

private:
    Page *m_page {};
    NodeHeader *m_header {};
};

class BlockAllocator final {
public:
    explicit BlockAllocator(NodeHeader&);
    ~BlockAllocator() = default;
    [[nodiscard]] auto usable_space() const -> Size;
    [[nodiscard]] auto compute_free_total() const -> Size;
    auto allocate(Size) -> Index;
    auto free(Index, Size) -> void;
    auto reset() -> void;

private:
    static constexpr Size FREE_BLOCK_HEADER_SIZE {2 * sizeof(uint16_t)};

    [[nodiscard]] auto get_next_pointer(Index) const -> Index;
    [[nodiscard]] auto get_block_size(Index) const -> Size;
    auto set_next_pointer(Index, Index) -> void;
    auto set_block_size(Index, Size) -> void;
    auto allocate_from_gap(Size) -> Index;
    auto allocate_from_free(Size) -> Index;
    auto take_free_space(Index, Index, Size) -> Index;

    Page *m_page {};
    NodeHeader *m_header {};
};

class Node final {
public:
    struct FindGeResult {
        Index index {};
        bool found_eq {};
    };

    ~Node() = default;

    Node(Page page, bool reset_header)
        : m_page {std::move(page)},
          m_header {m_page},
          m_directory {m_header},
          m_allocator {m_header}
    {
        reset(reset_header);
    }

    Node(Node &&rhs) noexcept
        : m_page {std::move(rhs.m_page)},
          m_header {NodeHeader {m_page}},
          m_directory {CellDirectory {m_header}},
          m_allocator {BlockAllocator {m_header}},
          m_overflow {rhs.m_overflow} {}

    auto operator=(Node &&rhs) noexcept -> Node&
    {
        if (this != &rhs) {
            m_page = std::move(rhs.m_page);
            m_header = NodeHeader {m_page};
            m_directory = CellDirectory {m_header};
            m_allocator = BlockAllocator {m_header};
            m_overflow = rhs.m_overflow;
            rhs.m_overflow.reset();
        }
        return *this;
    }

    auto header() -> NodeHeader&
    {
        return m_header;
    }

    [[nodiscard]] auto header() const -> const NodeHeader&
    {
        return m_header;
    }

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
    [[nodiscard]] auto find_ge(BytesView) const -> FindGeResult;
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
    [[nodiscard]] auto is_external() const -> bool;
    [[nodiscard]] auto child_id(Index) const -> PID;
    [[nodiscard]] auto parent_id() const -> PID;
    [[nodiscard]] auto right_sibling_id() const -> PID;
    [[nodiscard]] auto left_sibling_id() const -> PID;
    [[nodiscard]] auto rightmost_child_id() const -> PID;
    [[nodiscard]] auto cell_count() const -> Size;
    auto set_parent_id(PID) -> void;
    auto set_right_sibling_id(PID) -> void;
    auto set_left_sibling_id(PID id) -> void
    {
        return m_header.set_left_sibling_id(id);
    }
    auto set_rightmost_child_id(PID) -> void;
    auto set_child_id(Index, PID) -> void;

    [[nodiscard]] auto usable_space() const -> Size;
    [[nodiscard]] auto max_usable_space() const -> Size;
    [[nodiscard]] auto cell_area_offset() const -> Size;
    [[nodiscard]] auto header_offset() const -> Index;
    auto reset(bool = false) -> void;

    auto TEST_validate() const -> void;

private:
    auto defragment(std::optional<Index>) -> void;
    auto allocate(Size, std::optional<Index>) -> Index;

    Page m_page;
    NodeHeader m_header;
    CellDirectory m_directory;
    BlockAllocator m_allocator;
    std::optional<Cell> m_overflow {};
};

[[nodiscard]] auto can_merge_siblings(const Node&, const Node&, const Cell&) -> bool;
auto transfer_cell(Node&, Node&, Index) -> void;
auto merge_left(Node&, Node&, Node&, Index) -> void;
auto merge_right(Node&, Node&, Node&, Index) -> void;
auto merge_root(Node&, Node&) -> void;
auto split_root(Node&, Node&) -> void;
[[nodiscard]] auto split_non_root(Node&, Node&, Scratch) -> Cell;

[[nodiscard]] auto get_file_header_reader(const Node&) -> FileHeaderReader;
[[nodiscard]] auto get_file_header_writer(Node&) -> FileHeaderWriter;

} // cco

#endif // CCO_PAGE_NODE_H

#ifndef CUB_CELL_H
#define CUB_CELL_H

#include <optional>
#include "common.h"
#include "page.h"
#include "utils/types.h"

namespace cub {

class Cell {
public:
    static constexpr Size MIN_HEADER_SIZE = sizeof(uint16_t) + // Key size       (2B)
                                            sizeof(uint32_t);  // Value size     (4B)

    static constexpr Size MAX_HEADER_SIZE = MIN_HEADER_SIZE +
                                            PAGE_ID_SIZE +     // Left child ID  (4B)
                                            PAGE_ID_SIZE;      // Overflow ID    (4B)

    ~Cell() = default;
    [[nodiscard]] auto key() const -> BytesView;
    [[nodiscard]] auto size() const -> Size;
    [[nodiscard]] auto value_size() const -> Size;
    [[nodiscard]] auto overflow_size() const -> Size;
    [[nodiscard]] auto local_value() const -> BytesView;
    [[nodiscard]] auto overflow_id() const -> PID;
    [[nodiscard]] auto left_child_id() const -> PID;
    auto set_left_child_id(PID) -> void;
    auto set_overflow_id(PID) -> void;
    auto write(Bytes) const -> void;
    auto detach(Scratch) -> void;

    Cell(Cell&&) = default;
    auto operator=(Cell&&) -> Cell& = default;

private:
    friend class CellBuilder;
    friend class CellReader;
    Cell() = default;

    std::optional<Scratch> m_scratch;
    BytesView m_key;
    BytesView m_local_value;
    PID m_left_child_id;
    PID m_overflow_id;
    Size m_value_size{};
};

inline auto min_local(Size page_size)
{
    CUB_EXPECT_GT(page_size, 0);
    CUB_EXPECT_TRUE(is_power_of_two(page_size));
    return (page_size - PageLayout::HEADER_SIZE - NodeLayout::HEADER_SIZE) * 32 / 255 -
           Cell::MAX_HEADER_SIZE - CELL_POINTER_SIZE;
}

inline auto max_local(Size page_size)
{
    CUB_EXPECT_GT(page_size, 0);
    CUB_EXPECT_TRUE(is_power_of_two(page_size));
    return (page_size - PageLayout::HEADER_SIZE - NodeLayout::HEADER_SIZE) * 64 / 255 -
           Cell::MAX_HEADER_SIZE - CELL_POINTER_SIZE;
}

class CellBuilder {
public:
    explicit CellBuilder(Size);
    [[nodiscard]] auto build() const -> Cell;
    [[nodiscard]] auto overflow() const -> BytesView;
    auto set_key(BytesView) -> CellBuilder&;
    auto set_value(BytesView) -> CellBuilder&;

private:
    BytesView m_key;
    BytesView m_value;
    Size m_page_size{};
};

class CellReader {
public:
    CellReader(PageType, BytesView);
    [[nodiscard]] auto read(Index) const -> Cell;

private:
    BytesView m_page;
    PageType m_page_type{};
};

} // Cub

#endif // CUB_CELL_H
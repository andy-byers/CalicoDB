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
    [[nodiscard]] auto key() const -> RefBytes;
    [[nodiscard]] auto size() const -> Size;
    [[nodiscard]] auto value_size() const -> Size;
    [[nodiscard]] auto overflow_size() const -> Size;
    [[nodiscard]] auto local_value() const -> RefBytes;
    [[nodiscard]] auto overflow_id() const -> PID;
    [[nodiscard]] auto left_child_id() const -> PID;
    auto set_left_child_id(PID) -> void;
    auto set_overflow_id(PID) -> void;
    auto write(MutBytes) const -> void;
    auto detach(Scratch) -> void;

    Cell(Cell&&) = default;
    auto operator=(Cell&&) -> Cell& = default;

private:
    friend class CellBuilder;
    friend class CellReader;
    Cell() = default;

    std::optional<Scratch> m_scratch;
    RefBytes m_key;
    RefBytes m_local_value;
    PID m_left_child_id;
    PID m_overflow_id;
    Size m_value_size{};
};

inline constexpr auto min_local(Size page_size) -> Size
{
    CUB_EXPECT_GT(page_size, 0);
    CUB_EXPECT_TRUE(is_power_of_two(page_size));
    return (page_size - PageLayout::HEADER_SIZE - NodeLayout::HEADER_SIZE) * 32 / 255 -
           Cell::MAX_HEADER_SIZE - CELL_POINTER_SIZE;
}

inline constexpr auto max_local(Size page_size) -> Size
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
    [[nodiscard]] auto overflow() const -> RefBytes;
    auto set_key(RefBytes) -> CellBuilder&;
    auto set_value(RefBytes) -> CellBuilder&;

private:
    RefBytes m_key;
    RefBytes m_value;
    Size m_page_size{};
};

class CellReader {
public:
    CellReader(PageType, RefBytes);
    [[nodiscard]] auto read(Index) const -> Cell;

private:
    RefBytes m_page;
    PageType m_page_type{};
};

} // Cub

#endif // CUB_CELL_H
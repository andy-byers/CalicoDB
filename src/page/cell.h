#ifndef CUB_PAGE_CELL_H
#define CUB_PAGE_CELL_H

#include <optional>
#include "page.h"

namespace cub {

class Node;

class Cell {
public:
    struct Parameters {
        BytesView key;
        BytesView local_value;
        PID overflow_id;
        Size value_size {};
    };

    static auto read_at(const Node&, Index) -> Cell;
    explicit Cell(const Parameters&);

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
    friend class CellReader;
    Cell() = default;

    std::optional<Scratch> m_scratch;
    BytesView m_key;
    BytesView m_local_value;
    PID m_left_child_id;
    PID m_overflow_id;
    Size m_value_size {};
};

} // cub

#endif // CUB_PAGE_CELL_H
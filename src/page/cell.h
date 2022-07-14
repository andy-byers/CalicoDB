#ifndef CCO_PAGE_CELL_H
#define CCO_PAGE_CELL_H

#include <optional>
#include "page.h"
#include "utils/scratch.h"

namespace cco::page {

class Node;

class Cell {
public:
    struct Parameters {
        BytesView key;
        BytesView local_value;
        PID overflow_id;
        Size value_size {};
        Size page_size {};
        bool is_external {};
    };

    static auto read_at(BytesView, Size, bool) -> Cell;
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
    auto set_is_external(bool) -> void;
    auto set_left_child_id(PID) -> void;
    auto set_overflow_id(PID) -> void;
    auto write(Bytes) const -> void;
    auto detach(utils::Scratch, bool = false) -> void;

    Cell(const Cell&) = delete;
    auto operator=(const Cell&) -> Cell& = delete;
    Cell(Cell&&) = default;
    auto operator=(Cell&&) -> Cell& = default;

    [[nodiscard]] auto is_external() const -> bool
    {
        return m_is_external;
    }

    [[nodiscard]] auto is_attached() const -> bool
    {
        return m_scratch == std::nullopt;
    }

private:
    Cell() = default;

    std::optional<utils::Scratch> m_scratch;
    BytesView m_key;
    BytesView m_local_value;
    PID m_left_child_id;
    PID m_overflow_id;
    Size m_value_size {};
    Size m_page_size {};
    bool m_is_external {};
};

auto make_external_cell(BytesView, BytesView, Size) -> Cell;
auto make_internal_cell(BytesView, Size) -> Cell;

} // calico::page

#endif // CCO_PAGE_CELL_H
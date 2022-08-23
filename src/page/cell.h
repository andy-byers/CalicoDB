#ifndef CALICO_PAGE_CELL_H
#define CALICO_PAGE_CELL_H

#include "page.h"
#include "utils/scratch.h"
#include <optional>

namespace calico {

class Node;

class Cell {
public:
    struct Parameters {
        BytesView key;
        BytesView local_value;
        PageId overflow_id;
        Size value_size {};
        Size page_size {};
        bool is_external {};
    };

    static auto read_at(BytesView, Size, bool) -> Cell;
    static auto read_at(const Node &, Size) -> Cell;
    explicit Cell(const Parameters &);

    ~Cell() = default;
    [[nodiscard]] auto copy() const -> Cell;
    [[nodiscard]] auto key() const -> BytesView;
    [[nodiscard]] auto size() const -> Size;
    [[nodiscard]] auto value_size() const -> Size;
    [[nodiscard]] auto overflow_size() const -> Size;
    [[nodiscard]] auto local_value() const -> BytesView;
    [[nodiscard]] auto overflow_id() const -> PageId;
    [[nodiscard]] auto left_child_id() const -> PageId;
    auto set_is_external(bool) -> void;
    auto set_left_child_id(PageId) -> void;
    auto set_overflow_id(PageId) -> void;
    auto write(Bytes) const -> void;
    auto detach(Scratch, bool = false) -> void;

    [[nodiscard]] auto is_external() const -> bool
    {
        return m_is_external;
    }

    [[nodiscard]] auto is_attached() const -> bool
    {
        return m_is_attached;
    }

private:
    Cell() = default;

    BytesView m_key;
    BytesView m_local_value;
    PageId m_left_child_id;
    PageId m_overflow_id;
    Size m_value_size {};
    Size m_page_size {};
    bool m_is_external {};
    bool m_is_attached {true};
};

auto make_external_cell(BytesView, BytesView, Size) -> Cell;
auto make_internal_cell(BytesView, Size) -> Cell;

} // namespace calico

#endif // CALICO_PAGE_CELL_H
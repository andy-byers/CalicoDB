#ifndef CALICO_PAGE_CELL_H
#define CALICO_PAGE_CELL_H

#include "page.h"
#include "utils/scratch.h"
#include <optional>

namespace Calico {

class Node__;

class LocalValueSizeGetter {
public:
    explicit LocalValueSizeGetter(Size page_size);
    [[nodiscard]] auto operator()(Size key_size, Size value_size) const -> Size;

private:
    Size m_min_local {};
    Size m_max_local {};
};

struct PayloadMeta {
    Id overflow_id;
    Size local_value_size {};
};

class Cell__ {
public:
    struct Parameters {
        Byte *buffer {};
        Slice key;
        Slice local_value;
        Id overflow_id;
        Size value_size {};
        Size page_size {};
        bool is_external {};
    };

    static auto make_external(Byte *buffer, const Slice &key, const Slice &value, const LocalValueSizeGetter &lvs_getter) -> Cell__;
    static auto make_internal(Byte *buffer, const Slice &key) -> Cell__;
    static auto read_external(Byte *data, const LocalValueSizeGetter &lvs_getter) -> Cell__;
    static auto read_internal(Byte *data) -> Cell__;
    static auto read_at(Slice, Size, bool) -> Cell__;
    static auto read_at(const Node__ &, Size) -> Cell__;
    explicit Cell__(const Parameters &);

    ~Cell__() = default;
    [[nodiscard]] auto copy() const -> Cell__;
    [[nodiscard]] auto key() const -> Slice;
    [[nodiscard]] auto size() const -> Size;
    [[nodiscard]] auto value_size() const -> Size;
    [[nodiscard]] auto overflow_size() const -> Size;
    [[nodiscard]] auto local_value() const -> Slice;
    [[nodiscard]] auto overflow_id() const -> Id;
    [[nodiscard]] auto child_id() const -> Id;
    auto set_is_external(bool) -> void;
    auto set_child_id(Id child_id) -> void;
    auto set_overflow_id(Id) -> void;
    auto write(Span) const -> void;
    auto detach(Span, bool = false) -> void;

    [[nodiscard]]
    auto is_external() const -> bool
    {
        return m_is_external;
    }

    [[nodiscard]]
    auto is_attached() const -> bool
    {
        return m_is_attached;
    }

private:
    Cell__() = default;

        Slice m_key;
        Slice m_local_value;
        Id m_child_id;
        Id m_overflow_id;
        Size m_value_size {};
        Size m_page_size {};

    Size m_size {};
    Byte *m_data {};
    const Byte *m_key_ptr {};
    const Byte *m_val_ptr {};
    bool m_is_external {};
    bool m_is_attached {true};
};

auto make_external_cell(const Slice &, const Slice &, Size) -> Cell__;
auto make_internal_cell(const Slice &, Size) -> Cell__;

auto make_external_cell(Byte *buffer, const Slice &, const Slice &, Size) -> Cell__;
auto make_internal_cell(Byte *buffer, const Slice &, Size) -> Cell__;

} // namespace Calico

#endif // CALICO_PAGE_CELL_H
#ifndef CALICO_CURSOR_H
#define CALICO_CURSOR_H

#include <memory>
#include "bytes.h"

namespace calico {

class Node;
class NodePool;
class Internal;

class Cursor final {
public:
    Cursor() = default;
    ~Cursor() = default;

    [[nodiscard]] auto is_valid() const -> bool;
    [[nodiscard]] auto is_maximum() const -> bool;
    [[nodiscard]] auto is_minimum() const -> bool;
    [[nodiscard]] auto key() const -> BytesView;
    [[nodiscard]] auto value() const -> std::string;
    [[nodiscard]] auto record() const -> Record;
    auto increment() -> bool;
    auto decrement() -> bool;

    auto operator++() -> Cursor&;
    auto operator++(int) -> Cursor;
    auto operator--() -> Cursor&;
    auto operator--(int) -> Cursor;
    auto operator==(const Cursor&) const -> bool;
    auto operator!=(const Cursor&) const -> bool;

private:
    struct Position {
        static constexpr Index LEFT {0};
        static constexpr Index CURRENT {1};
        static constexpr Index RIGHT {2};

        [[nodiscard]] auto operator==(const Position &rhs) const -> bool;
        [[nodiscard]] auto is_minimum() const -> bool;
        [[nodiscard]] auto is_maximum() const -> bool;

        Size cell_count {};
        Index ids[3] {0, 1, 0};
        Index index {};
    };

    friend class Tree;

    Cursor(NodePool*, Internal*);
    [[nodiscard]] auto id() const -> Index;
    [[nodiscard]] auto index() const -> Index;
    auto move_to(Node, Index) -> void;
    auto seek_left() -> void;
    auto seek_right() -> void;
    auto invalidate() -> void;

    NodePool *m_pool {};
    Internal *m_internal {};
    Position m_position;
    bool m_is_valid {};
};

} // calico

#endif // CALICO_CURSOR_H

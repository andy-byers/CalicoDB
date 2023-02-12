#ifndef CALICO_TREE_CURSOR_INTERNAL_H
#define CALICO_TREE_CURSOR_INTERNAL_H

#include "node.h"
#include "tree.h"
#include "utils/expected.hpp"
#include <calico/cursor.h>
#include <functional>
#include <utils/types.h>

namespace Calico {

class CursorImpl : public Cursor {
public:
    friend class CursorInternal;

    explicit CursorImpl(const CursorActions &actions)
        : m_actions {&actions}
    {}

    ~CursorImpl() override = default;

    [[nodiscard]] auto is_valid() const -> bool override;
    [[nodiscard]] auto status() const -> Status override;
    [[nodiscard]] auto key() const -> Slice override;
    [[nodiscard]] auto value() const -> Slice override;

    auto seek(const Slice &key) -> void override;
    auto seek_first() -> void override;
    auto seek_last() -> void override;
    auto next() -> void override;
    auto previous() -> void override;

private:
    struct Location {
        Id pid {};
        PageSize index {};
        PageSize count {};
    };
    mutable Status m_status;
    mutable std::string m_key;
    mutable std::string m_value;
    const CursorActions *m_actions {};
    Location m_loc;
};

class CursorInternal {
    friend class CursorImpl;

    [[nodiscard]] static auto action_collect(const CursorImpl &cursor, Node node, Size index) -> tl::expected<std::string, Status>;
    [[nodiscard]] static auto action_acquire(const CursorImpl &cursor, Id pid) -> tl::expected<Node, Status>;
    [[nodiscard]] static auto action_search(const CursorImpl &cursor, const Slice &key) -> tl::expected<SearchResult, Status>;
    [[nodiscard]] static auto action_lowest(const CursorImpl &cursor) -> tl::expected<Node, Status>;
    [[nodiscard]] static auto action_highest(const CursorImpl &cursor) -> tl::expected<Node, Status>;
    static auto action_release(const CursorImpl &cursor, Node node) -> void;

    static auto seek_left(CursorImpl &cursor) -> void;
    static auto seek_right(CursorImpl &cursor) -> void;
    static auto seek_first(CursorImpl &cursor) -> void;
    static auto seek_last(CursorImpl &cursor) -> void;
    static auto seek_to(CursorImpl &cursor, Node node, Size index) -> void;
    static auto seek(CursorImpl &cursor, const Slice &key) -> void;

public:
    [[nodiscard]] static auto make_cursor(BPlusTree &tree) -> Cursor *;
    static auto invalidate(const Cursor &cursor, const Status &error) -> void;

    static auto TEST_validate(const Cursor &cursor) -> void;
};

} // namespace Calico

#endif // CALICO_TREE_CURSOR_INTERNAL_H

#ifndef CALICO_TREE_CURSOR_INTERNAL_H
#define CALICO_TREE_CURSOR_INTERNAL_H

#include "node.h"
#include "tree.h"
#include <calico/cursor.h>
#include <functional>
#include <utils/types.h>

namespace Calico {

class CursorImpl : public Cursor {
    struct Location {
        Id pid;
        PageSize index {};
        PageSize count {};
    };
    mutable Status m_status;
    mutable std::string m_key;
    mutable std::string m_value;
    mutable Size m_key_size {};
    mutable Size m_value_size {};
    BPlusTree *m_tree {};
    Location m_loc;

    auto seek_to(Node node, Size index) -> void;
    auto fetch_key() const -> Status;
    auto fetch_value() const -> Status;

public:
    friend class CursorInternal;

    explicit CursorImpl(BPlusTree &tree)
        : m_tree {&tree}
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
};

class CursorInternal {
public:
    [[nodiscard]] static auto make_cursor(BPlusTree &tree) -> Cursor *;
    static auto invalidate(const Cursor &cursor, const Status &error) -> void;
};

} // namespace Calico

#endif // CALICO_TREE_CURSOR_INTERNAL_H

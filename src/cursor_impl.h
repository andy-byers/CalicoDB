#ifndef CALICODB_CURSOR_IMPL_H
#define CALICODB_CURSOR_IMPL_H

#include "calicodb/cursor.h"
#include "node.h"
#include "tree.h"
#include "types.h"
#include <functional>

namespace calicodb
{

class CursorImpl : public Cursor
{
    struct Location {
        Id pid;
        PageSize index {};
        PageSize count {};
    };
    mutable Status m_status;
    std::string m_key;
    std::string m_value;
    std::size_t m_key_size {};
    std::size_t m_value_size {};
    BPlusTree *m_tree {};
    Location m_loc;

    auto seek_to(Node node, std::size_t index) -> void;
    auto fetch_payload() -> Status;

public:
    friend class CursorInternal;

    explicit CursorImpl(BPlusTree &tree)
        : m_tree {&tree}
    {
    }

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

class CursorInternal
{
public:
    [[nodiscard]] static auto make_cursor(BPlusTree &tree) -> Cursor *;
    static auto invalidate(const Cursor &cursor, Status error) -> void;
};

} // namespace calicodb

#endif // CALICODB_CURSOR_IMPL_H

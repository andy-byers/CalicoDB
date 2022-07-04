#ifndef CALICO_TREE_INTERFACE_H
#define CALICO_TREE_INTERFACE_H

#include <optional>
#include "calico/database.h"
#include "page/node.h"

namespace calico {

class FileHeader;
class Internal;
class NodePool;

class ITree {
public:
    virtual ~ITree() = default;
    [[nodiscard]] virtual auto cell_count() const -> Size = 0;
    [[nodiscard]] virtual auto node_count() const -> Size = 0;
    [[nodiscard]] virtual auto internal() const -> const Internal& = 0;
    [[nodiscard]] virtual auto pool() const -> const NodePool& = 0;
    virtual auto internal() -> Internal& = 0;
    virtual auto pool() -> NodePool& = 0;
    virtual auto save_header(FileHeader&) const -> void = 0;
    virtual auto load_header(const FileHeader&) -> void = 0;
    virtual auto insert(BytesView, BytesView) -> bool = 0;
    virtual auto erase(Cursor) -> bool = 0;
/* TODO:
    virtual auto erase(Cursor lower, Cursor one_past_upper) -> Size;
*/
    virtual auto find(BytesView, bool) -> Cursor = 0;
    virtual auto find_minimum() -> Cursor = 0;
    virtual auto find_maximum() -> Cursor = 0;
    virtual auto root(bool) -> Node = 0;
};

} // calico

#endif // CALICO_TREE_INTERFACE_H

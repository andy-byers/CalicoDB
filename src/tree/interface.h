#ifndef CCO_TREE_INTERFACE_H
#define CCO_TREE_INTERFACE_H

#include <optional>
#include "calico/cursor.h"
#include "calico/error.h"

namespace cco {

class Internal;
class NodePool;

namespace page {
    class Cell;
    class FileHeader;
    class Link;
    class Node;
    class Page;
} // page

class ITree {
public:
    virtual ~ITree() = default;
    [[nodiscard]] virtual auto cell_count() const -> Size = 0;
    [[nodiscard]] virtual auto node_count() const -> Size = 0;
    [[nodiscard]] virtual auto internal() const -> const Internal& = 0;
    [[nodiscard]] virtual auto pool() const -> const NodePool& = 0;
    [[nodiscard]] virtual auto close() -> Result<void> = 0;
    virtual auto internal() -> Internal& = 0;
    virtual auto pool() -> NodePool& = 0;
    virtual auto save_header(page::FileHeader&) const -> void = 0;
    virtual auto load_header(const page::FileHeader&) -> void = 0;
    virtual auto insert(BytesView, BytesView) -> Result<bool> = 0;
    virtual auto erase(Cursor) -> Result<bool> = 0;
    virtual auto find_exact(BytesView) -> Cursor = 0;
    virtual auto find(BytesView key) -> Cursor = 0;
    virtual auto find_minimum() -> Cursor = 0;
    virtual auto find_maximum() -> Cursor = 0;
    virtual auto root(bool) -> Result<page::Node> = 0;
};

} // calico

#endif // CCO_TREE_INTERFACE_H

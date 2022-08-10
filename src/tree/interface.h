#ifndef CCO_TREE_INTERFACE_H
#define CCO_TREE_INTERFACE_H

#include "calico/cursor.h"
#include "calico/status.h"
#include "utils/identifier.h"
#include "utils/result.h"
#include <optional>

namespace cco {

class Cell;
class FileHeaderReader;
class FileHeaderWriter;
class Link;
class Internal;
class Node;
class NodePool;
class Page;

// Depends on BufferPool
class ITree {
public:
    virtual ~ITree() = default;
    [[nodiscard]] virtual auto cell_count() const -> Size = 0;
    [[nodiscard]] virtual auto insert(BytesView, BytesView) -> Result<bool> = 0;
    [[nodiscard]] virtual auto erase(Cursor) -> Result<bool> = 0;
    [[nodiscard]] virtual auto find_exact(BytesView) -> Cursor = 0;
    [[nodiscard]] virtual auto find(BytesView key) -> Cursor = 0;
    [[nodiscard]] virtual auto find_minimum() -> Cursor = 0;
    [[nodiscard]] virtual auto find_maximum() -> Cursor = 0;
    [[nodiscard]] virtual auto root(bool) -> Result<Node> = 0;
    [[nodiscard]] virtual auto allocate_root() -> Result<Node> = 0;
    virtual auto save_header(FileHeaderWriter &) const -> void = 0;
    virtual auto load_header(const FileHeaderReader &) -> void = 0;
    virtual auto TEST_validate_node(PageId) -> void = 0;
};

} // namespace cco

#endif // CCO_TREE_INTERFACE_H

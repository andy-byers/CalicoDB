#ifndef CCO_TREE_TREE_H
#define CCO_TREE_TREE_H

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
class Tree {
public:
    virtual ~Tree() = default;
    virtual auto cell_count() const -> Size = 0;
    virtual auto insert(BytesView, BytesView) -> Result<bool> = 0;
    virtual auto erase(Cursor) -> Result<bool> = 0;
    virtual auto find_exact(BytesView) -> Cursor = 0;
    virtual auto find(BytesView key) -> Cursor = 0;
    virtual auto find_minimum() -> Cursor = 0;
    virtual auto find_maximum() -> Cursor = 0;
    virtual auto root(bool) -> Result<Node> = 0;
    virtual auto save_state(FileHeader &) const -> void = 0;
    virtual auto load_state(const FileHeader &) -> void = 0;
    virtual auto TEST_validate_node(PageId) -> void = 0;
};

} // namespace cco

#endif // CCO_TREE_TREE_H

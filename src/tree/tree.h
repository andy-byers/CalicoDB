#ifndef CALICO_TREE_TREE_H
#define CALICO_TREE_TREE_H

#include "calico/cursor.h"
#include "calico/status.h"
#include "utils/expected.hpp"
#include "utils/types.h"
#include <optional>

namespace Calico {

class Cell;
class Link;
class Internal;
class Node;
class NodeManager;
class Page;
struct FileHeader;

// Depends on BufferPool
class Tree {
public:
    using Ptr = std::unique_ptr<Tree>;

    virtual ~Tree() = default;
    virtual auto record_count() const -> Size = 0;
    virtual auto insert(Slice, Slice) -> Status = 0;
    virtual auto erase(Cursor) -> Status = 0;
    virtual auto find_exact(Slice) -> Cursor = 0;
    virtual auto find(Slice key) -> Cursor = 0;
    virtual auto find_minimum() -> Cursor = 0;
    virtual auto find_maximum() -> Cursor = 0;
    virtual auto root(bool) -> tl::expected<Node, Status> = 0;
    virtual auto save_state(FileHeader &) const -> void = 0;
    virtual auto load_state(const FileHeader &) -> void = 0;
    virtual auto TEST_validate_nodes() -> void = 0;
    virtual auto TEST_validate_order() -> void = 0;
    virtual auto TEST_validate_links() -> void = 0;
};

} // namespace Calico

#endif // CALICO_TREE_TREE_H

#ifndef CUB_TREE_INTERFACE_H
#define CUB_TREE_INTERFACE_H

#include <optional>

#include "common.h"
#include "page/node.h"

namespace cub {

class FileHeader;

class ITree {
public:
    struct Position {
        Node node;
        Index index {};
    };

    struct SearchResult {
        Node node;
        Index index {};
        bool found_eq {};
    };

    virtual ~ITree() = default;
    virtual auto node_count() const -> Size = 0;
    virtual auto cell_count() const -> Size = 0;
    virtual auto find_root(bool) -> Node = 0;
    virtual auto find_ge(RefBytes, bool) -> SearchResult = 0;
    virtual auto find_local_min(Node) -> Position = 0;
    virtual auto find_local_max(Node) -> Position = 0;
    virtual auto collect_value(const Node&, Index) const -> std::string = 0;
    virtual auto insert(Position, RefBytes, RefBytes) -> Position = 0;
    virtual auto modify(Position, RefBytes) -> Position = 0;
    virtual auto remove(Position) -> Position = 0;
    virtual auto save_header(FileHeader&) -> void = 0;
    virtual auto allocate_node(PageType) -> Node = 0;
    virtual auto acquire_node(PID, bool) -> Node = 0;
    virtual auto destroy_node(Node) -> void = 0;
};

} // cub

#endif //CUB_TREE_INTERFACE_H

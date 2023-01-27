#ifndef CALICO_TREE_TREE_H
#define CALICO_TREE_TREE_H

#include "calico/cursor.h"
#include "calico/status.h"
#include "utils/expected.hpp"
#include "utils/types.h"
#include <optional>

namespace Calico {

class Cell__;
class Link;
class Internal;
class Node__;
class NodeManager;
class Page_;
struct FileHeader__;

class Tree {
public:
    using Ptr = std::unique_ptr<Tree>;

    virtual ~Tree() = default;
    virtual auto record_count() const -> Size = 0;
    virtual auto insert(const Slice &, const Slice &) -> Status = 0;
    virtual auto erase(Cursor) -> Status = 0;
    virtual auto find_exact(const Slice &) -> Cursor = 0;
    virtual auto find(const Slice &key) -> Cursor = 0;
    virtual auto find_minimum() -> Cursor = 0;
    virtual auto find_maximum() -> Cursor = 0;
    virtual auto root(bool) -> tl::expected<Node__, Status> = 0;
    virtual auto save_state(FileHeader__ &) const -> void = 0;
    virtual auto load_state(const FileHeader__ &) -> void = 0;

#if not NDEBUG
    virtual auto TEST_to_string(bool integer_keys) -> std::string = 0;
    virtual auto TEST_validate_nodes() -> void = 0;
    virtual auto TEST_validate_order() -> void = 0;
    virtual auto TEST_validate_links() -> void = 0;
#endif // not NDEBUG
};


class Tree_ {
public:
    using Ptr = std::unique_ptr<Tree_>;

    struct FindResult {
        Id pid;
        Size index {};
        bool exact {};
    };

    virtual ~Tree_() = default;
    [[nodiscard]] virtual auto find(const Slice &key) const -> FindResult = 0;
    [[nodiscard]] virtual auto insert(const Slice &key, const Slice &value) -> bool = 0;
    [[nodiscard]] virtual auto erase(const Slice &key) -> bool = 0;
    virtual auto save_state(FileHeader__ &header) const -> void = 0;
    virtual auto load_state(const FileHeader__ &header) -> void = 0;
};

} // namespace Calico

#endif // CALICO_TREE_TREE_H
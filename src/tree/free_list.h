#ifndef CALICO_TREE_FREE_LIST_H
#define CALICO_TREE_FREE_LIST_H

#include <optional>
#include "utils/identifier.h"

namespace calico {

class FileHeader;
class IBufferPool;
class Page;

class FreeList {
public:
    struct Parameters {
        IBufferPool *buffer_pool{};
        PID free_start{};
        Size free_count{};
    };

    explicit FreeList(const Parameters&);
    ~FreeList() = default;
    auto push(Page) -> void;
    auto pop() -> std::optional<Page>;
    auto save_header(FileHeader&) const -> void;
    auto load_header(const FileHeader&) -> void;

    [[nodiscard]] auto is_empty() const -> bool
    {
        return m_free_count == 0;
    }

private:
    IBufferPool *m_pool;
    PID m_free_start{};
    Size m_free_count{};
};

} // calico

#endif // CALICO_TREE_FREE_LIST_H

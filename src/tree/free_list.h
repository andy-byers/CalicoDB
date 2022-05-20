#ifndef CUB_TREE_FREELIST_H
#define CUB_TREE_FREELIST_H

#include <optional>
#include "common.h"
#include "utils/layout.h"

namespace cub {

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
    [[nodiscard]] auto free_start() const -> PID;
    [[nodiscard]] auto free_count() const -> Size;
    auto push(Page) -> void;
    auto pop() -> std::optional<Page>;

private:
    IBufferPool *m_pool;
    PID m_free_start{};
    Size m_free_count{};
};

} // cub

#endif // CUB_TREE_FREELIST_H

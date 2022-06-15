#ifndef CUB_POOL_INTERFACE_H
#define CUB_POOL_INTERFACE_H

#include "cub/bytes.h"
#include "utils/utils.h"

namespace cub {

class FileHeader;
class Frame;
class Page;
struct PID;
struct LSN;

class IBufferPool {
public:
    virtual ~IBufferPool() = default;
    [[nodiscard]] virtual auto uses_transactions() const -> bool = 0;
    [[nodiscard]] virtual auto hit_ratio() const -> double = 0;
    [[nodiscard]] virtual auto page_count() const -> Size = 0;
    [[nodiscard]] virtual auto page_size() const -> Size = 0;
    [[nodiscard]] virtual auto block_size() const -> Size = 0;
    [[nodiscard]] virtual auto flushed_lsn() const -> LSN = 0;
    [[nodiscard]] virtual auto allocate(PageType) -> Page = 0;
    [[nodiscard]] virtual auto acquire(PID, bool) -> Page = 0;
    [[nodiscard]] virtual auto can_commit() const -> bool = 0;
    virtual auto commit() -> void = 0;
    virtual auto abort() -> void = 0;
    virtual auto try_flush() -> bool = 0;
    virtual auto try_flush_wal() -> bool = 0;
    virtual auto purge() -> void = 0;
    virtual auto recover() -> bool = 0;
    virtual auto save_header(FileHeader&) -> void = 0;
    virtual auto load_header(const FileHeader&) -> void = 0;
    virtual auto on_page_release(Page&) -> void = 0;
    virtual auto on_page_error() -> void = 0;
};

} // cub

#endif // CUB_POOL_INTERFACE_H

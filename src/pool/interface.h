#ifndef CALICO_POOL_INTERFACE_H
#define CALICO_POOL_INTERFACE_H

#include "calico/bytes.h"
#include "calico/error.h"
#include "page/page.h"
#include "utils/utils.h"

namespace calico {

class FileHeader;
class Frame;
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


    [[nodiscard]] virtual auto noex_close() -> Result<void> = 0;
    [[nodiscard]] virtual auto noex_allocate(PageType) -> Result<Page> = 0;
    [[nodiscard]] virtual auto noex_acquire(PID, bool) -> Result<Page> = 0;
    [[nodiscard]] virtual auto noex_commit() -> Result<void> = 0;
    [[nodiscard]] virtual auto noex_abort() -> Result<void> = 0;
    [[nodiscard]] virtual auto noex_try_flush() -> Result<bool> = 0;
    [[nodiscard]] virtual auto noex_try_flush_wal() -> Result<bool> = 0;
    [[nodiscard]] virtual auto noex_purge() -> Result<void> = 0;
    [[nodiscard]] virtual auto noex_recover() -> Result<bool> = 0;
    [[nodiscard]] virtual auto noex_save_header(FileHeader&) -> Result<void> = 0;
    [[nodiscard]] virtual auto noex_load_header(const FileHeader&) -> Result<void> = 0;
    [[nodiscard]] virtual auto noex_on_page_release(Page&) -> Result<void> = 0;
    [[nodiscard]] virtual auto noex_on_page_error() -> Result<void> = 0;
};

} // calico

#endif // CALICO_POOL_INTERFACE_H

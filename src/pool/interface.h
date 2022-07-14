#ifndef CCO_POOL_INTERFACE_H
#define CCO_POOL_INTERFACE_H

#include "calico/bytes.h"
#include "calico/error.h"
#include "utils/utils.h"

namespace cco {

constexpr auto DATA_NAME = "data";

namespace page {
    class FileHeader;
    class Page;
} // page

class Frame;
struct PID;
struct LSN;

class IBufferPool {
public:
    virtual ~IBufferPool() = default;
    [[nodiscard]] virtual auto hit_ratio() const -> double = 0;
    [[nodiscard]] virtual auto page_count() const -> Size = 0;
    [[nodiscard]] virtual auto page_size() const -> Size = 0;
    [[nodiscard]] virtual auto allocate() -> Result<page::Page> = 0;
    [[nodiscard]] virtual auto acquire(PID, bool) -> Result<page::Page> = 0;
    [[nodiscard]] virtual auto release(page::Page) -> Result<void> = 0;
    [[nodiscard]] virtual auto flush() -> Result<void> = 0;
    [[nodiscard]] virtual auto purge() -> Result<void> = 0;
    [[nodiscard]] virtual auto close() -> Result<void> = 0;
    virtual auto on_release(page::Page&) -> void = 0;
    virtual auto save_header(page::FileHeader&) -> void = 0;
    virtual auto load_header(const page::FileHeader&) -> void = 0;
};

} // calico

#endif // CCO_POOL_INTERFACE_H

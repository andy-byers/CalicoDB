#ifndef CCO_POOL_INTERFACE_H
#define CCO_POOL_INTERFACE_H

#include "calico/bytes.h"
#include "calico/status.h"
#include "utils/result.h"
#include "utils/utils.h"
#include <optional>

namespace cco {

constexpr auto DATA_NAME = "data";

namespace page {
    class FileHeaderReader;
    class FileHeaderWriter;
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
    [[nodiscard]] virtual auto can_commit() const -> bool = 0;
    [[nodiscard]] virtual auto allocate() -> Result<page::Page> = 0;
    [[nodiscard]] virtual auto acquire(PID, bool) -> Result<page::Page> = 0;
    [[nodiscard]] virtual auto fetch(PID, bool) -> Result<page::Page> = 0;
    [[nodiscard]] virtual auto release(page::Page) -> Result<void> = 0;
    [[nodiscard]] virtual auto recover() -> Result<void> = 0;
    [[nodiscard]] virtual auto flush() -> Result<void> = 0;
    [[nodiscard]] virtual auto close() -> Result<void> = 0;
    [[nodiscard]] virtual auto commit() -> Result<void> = 0;
    [[nodiscard]] virtual auto abort() -> Result<void> = 0;
    [[nodiscard]] virtual auto status() const -> Status = 0;
    virtual auto purge() -> void = 0;
    virtual auto clear_error() -> void = 0;
    virtual auto on_release(page::Page&) -> void = 0;
    virtual auto save_header(page::FileHeaderWriter&) -> void = 0;
    virtual auto load_header(const page::FileHeaderReader&) -> void = 0;
};

} // cco

#endif // CCO_POOL_INTERFACE_H

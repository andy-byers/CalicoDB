#ifndef CCO_POOL_INTERFACE_H
#define CCO_POOL_INTERFACE_H

#include "calico/bytes.h"
#include "calico/status.h"
#include "utils/result.h"
#include "utils/identifier.h"
#include "utils/utils.h"
#include <optional>

namespace cco {

constexpr auto DATA_FILENAME = "data";

class FileHeader;
class Frame;
class Page;

class IBufferPool {
public:
    virtual ~IBufferPool() = default;
    [[nodiscard]] virtual auto flushed_lsn() const -> SequenceNumber = 0;
    [[nodiscard]] virtual auto page_count() const -> Size = 0;
    [[nodiscard]] virtual auto status() const -> Status = 0;
    [[nodiscard]] virtual auto can_commit() const -> bool = 0;
    [[nodiscard]] virtual auto allocate() -> Result<Page> = 0;
    [[nodiscard]] virtual auto acquire(PageId, bool) -> Result<Page> = 0;
    [[nodiscard]] virtual auto release(Page) -> Status = 0;
    [[nodiscard]] virtual auto flush() -> Status = 0;
    virtual auto reset_status() -> void = 0;
    virtual auto update_page(Page &, Size, Index) -> void = 0;
    virtual auto save_state(FileHeader &) -> void = 0;
    virtual auto load_state(const FileHeader &) -> void = 0;
};

} // namespace cco

#endif // CCO_POOL_INTERFACE_H

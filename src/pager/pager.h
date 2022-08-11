#ifndef CCO_POOL_INTERFACE_H
#define CCO_POOL_INTERFACE_H

#include "calico/bytes.h"
#include "calico/status.h"
#include "calico/wal.h"
#include "utils/identifier.h"
#include "utils/result.h"
#include "utils/utils.h"
#include <optional>

namespace cco {

constexpr auto DATA_FILENAME = "data";

struct FileHeader;
struct PageDelta;
class Page;

/*
 * TODO: We could expose this layer for customization at some point.
 */

class Pager {
public:
    virtual ~Pager() = default;
    virtual auto flushed_lsn() const -> SequenceNumber = 0;
    virtual auto page_count() const -> Size = 0;
    virtual auto status() const -> Status = 0;
    virtual auto allocate() -> Result<Page> = 0;
    virtual auto acquire(PageId, bool) -> Result<Page> = 0;
    virtual auto release(Page) -> Status = 0;
    virtual auto flush() -> Status = 0;
    virtual auto reset_status() -> void = 0;
    virtual auto save_state(FileHeader &) -> void = 0;
    virtual auto load_state(const FileHeader &) -> void = 0;
};

} // namespace cco

#endif // CCO_POOL_INTERFACE_H

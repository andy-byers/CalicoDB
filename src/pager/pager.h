#ifndef CALICO_POOL_INTERFACE_H
#define CALICO_POOL_INTERFACE_H

#include "calico/bytes.h"
#include "calico/status.h"
#include "calico/wal.h"
#include "utils/result.h"
#include "utils/types.h"
#include "utils/utils.h"
#include <optional>

namespace calico {

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
    virtual auto flushed_lsn() const -> SequenceId = 0;
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

#endif // CALICO_POOL_INTERFACE_H

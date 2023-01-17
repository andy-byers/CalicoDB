#ifndef CALICO_PAGER_INTERFACE_H
#define CALICO_PAGER_INTERFACE_H

#include "calico/slice.h"
#include "calico/status.h"
#include "utils/types.h"
#include "utils/utils.h"
#include "wal/wal.h"
#include <optional>
#include "utils/expected.hpp"

namespace Calico {

constexpr auto DATA_FILENAME = "data";

struct FileHeader;
struct PageDelta;
class Page;

/*
 * TODO: We could expose this layer for customization at some point.
 */

class Pager {
public:
    using Ptr = std::unique_ptr<Pager>;

    virtual ~Pager() = default;
    [[nodiscard]] virtual auto recovery_lsn() -> Id = 0;
    [[nodiscard]] virtual auto page_count() const -> Size = 0;
    [[nodiscard]] virtual auto page_size() const -> Size = 0;
    [[nodiscard]] virtual auto bytes_written() const -> Size = 0;
    [[nodiscard]] virtual auto hit_ratio() const -> double = 0;
    virtual auto allocate() -> tl::expected<Page, Status> = 0;
    virtual auto acquire(Id, bool) -> tl::expected<Page, Status> = 0;
    virtual auto release(Page) -> Status = 0;
    virtual auto flush(Id lsn_limit) -> Status = 0;
    virtual auto save_state(FileHeader &) -> void = 0;
    virtual auto load_state(const FileHeader &) -> void = 0;
};

} // namespace Calico

#endif // CALICO_PAGER_INTERFACE_H

#ifndef CALICO_PAGER_INTERFACE_H
#define CALICO_PAGER_INTERFACE_H

#include <optional>
#include <tl/expected.hpp>
#include "calico/bytes.h"
#include "calico/status.h"
#include "utils/types.h"
#include "utils/utils.h"
#include "wal/wal.h"

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
    using Ptr = std::unique_ptr<Pager>;

    virtual ~Pager() = default;
    [[nodiscard]] virtual auto flushed_lsn() const -> Id = 0;
    [[nodiscard]] virtual auto page_count() const -> Size = 0;
    [[nodiscard]] virtual auto page_size() const -> Size = 0;
    [[nodiscard]] virtual auto hit_ratio() const -> double = 0;
    [[nodiscard]] virtual auto status() const -> Status = 0;
    virtual auto allocate() -> tl::expected<Page, Status> = 0;
    virtual auto acquire(Id, bool) -> tl::expected<Page, Status> = 0;
    virtual auto release(Page) -> Status = 0;
    virtual auto flush(Id lsn_limit) -> Status = 0;
    virtual auto save_state(FileHeader &) -> void = 0;
    virtual auto load_state(const FileHeader &) -> void = 0;
};

} // namespace calico

#endif // CALICO_PAGER_INTERFACE_H

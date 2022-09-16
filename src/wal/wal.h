#ifndef CALICO_WAL_H
#define CALICO_WAL_H

#include <functional>
#include <variant>
#include <vector>
#include "calico/bytes.h"
#include "calico/status.h"
#include "utils/types.h"

namespace calico {

struct FileHeader;
struct SequenceId;

struct PageDelta {
    Size offset {};
    Size size {};
};

struct DeltaContent {
    Size offset {};
    BytesView data {};
};

struct DeltasDescriptor {
    PageId pid;
    SequenceId lsn;
    std::vector<DeltaContent> deltas;
};

struct FullImageDescriptor {
    PageId pid;
    SequenceId lsn;
    BytesView image;
};

struct CommitDescriptor {
    SequenceId lsn;
};

using PayloadDescriptor = std::variant<DeltasDescriptor, FullImageDescriptor, CommitDescriptor>;
using GetPayload = std::function<Status(const PayloadDescriptor&)>;
using GetDeltas = std::function<Status(const DeltasDescriptor&)>;
using GetFullImage = std::function<Status(const FullImageDescriptor&)>;

class WriteAheadLog {
public:
    virtual ~WriteAheadLog() = default;

    [[nodiscard]]
    virtual auto is_enabled() const -> bool
    {
        return true;
    }

    [[nodiscard]] virtual auto status() const -> Status = 0;
    [[nodiscard]] virtual auto is_working() const -> bool = 0;
    [[nodiscard]] virtual auto flushed_lsn() const -> std::uint64_t = 0;
    [[nodiscard]] virtual auto current_lsn() const -> std::uint64_t = 0;
    [[nodiscard]] virtual auto log(std::uint64_t page_id, BytesView image) -> Status = 0;
    [[nodiscard]] virtual auto log(std::uint64_t page_id, BytesView image, const std::vector<PageDelta> &deltas) -> Status = 0;
    [[nodiscard]] virtual auto flush() -> Status = 0;
    [[nodiscard]] virtual auto commit() -> Status = 0;
    [[nodiscard]] virtual auto start_workers() -> Status = 0;
    [[nodiscard]] virtual auto stop_workers() -> Status = 0;
    [[nodiscard]] virtual auto start_recovery(const GetDeltas &delta_cb, const GetFullImage &image_cb) -> Status = 0;
    [[nodiscard]] virtual auto finish_recovery() -> Status = 0;
    [[nodiscard]] virtual auto start_abort(const GetFullImage &image_cb) -> Status = 0;
    [[nodiscard]] virtual auto finish_abort() -> Status = 0;
    virtual auto allow_cleanup(std::uint64_t pager_lsn) -> void = 0;
};

} // namespace calico

#endif // CALICO_WAL_H

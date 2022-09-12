#ifndef CALICO_WAL_H
#define CALICO_WAL_H

#include <functional>
#include <variant>
#include <vector>
#include "calico/bytes.h"
#include "calico/status.h"

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
    std::uint64_t page_id {};

    // LSN here corresponds to the page LSN of the referenced page after it was updated.
    std::uint64_t page_lsn {};
    std::vector<DeltaContent> deltas;
};

struct FullImageDescriptor {
    std::uint64_t page_id {};
    BytesView image;
};

struct CommitDescriptor {
    SequenceId lsn;
};

using PayloadDescriptor = std::variant<DeltasDescriptor, FullImageDescriptor, CommitDescriptor>;
using GetPayload = std::function<Status(PayloadDescriptor)>;

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
    [[nodiscard]] virtual auto commit() -> Status = 0;
    [[nodiscard]] virtual auto stop_workers() -> Status = 0;
    [[nodiscard]] virtual auto start_workers() -> Status = 0;
    [[nodiscard]] virtual auto start_recovery(const GetPayload &redo, const GetPayload &undo) -> Status = 0;
    [[nodiscard]] virtual auto finish_recovery() -> Status = 0;
    [[nodiscard]] virtual auto abort_last(const GetPayload &callback) -> Status = 0;
    virtual auto allow_cleanup(std::uint64_t pager_lsn) -> void = 0;
};

} // namespace calico

#endif // CALICO_WAL_H

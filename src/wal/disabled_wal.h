#ifndef CALICO_DISABLED_WAL_H
#define CALICO_DISABLED_WAL_H

#include "wal.h"

namespace calico {

class DisabledWriteAheadLog: public WriteAheadLog {
public:
    ~DisabledWriteAheadLog() override = default;

    [[nodiscard]]
    auto is_enabled() const -> bool override
    {
        return false;
    }

    [[nodiscard]]
    auto is_working() const -> bool override
    {
        return false;
    }

    [[nodiscard]]
    auto worker_status() const -> Status override
    {
        return Status::ok();
    }

    [[nodiscard]]
    auto flushed_lsn() const -> Id override
    {
        return Id::null();
    }

    [[nodiscard]]
    auto current_lsn() const -> Id override
    {
        return Id::null();
    }

    [[nodiscard]]
    auto log(WalPayloadIn) -> Status override
    {
        return Status::ok();
    }

    [[nodiscard]]
    auto flush() -> Status override
    {
        return Status::ok();
    }

    [[nodiscard]]
    auto advance() -> Status override
    {
        return Status::ok();
    }

    [[nodiscard]]
    auto stop_workers() -> Status override
    {
        return Status::ok();
    }

    [[nodiscard]]
    auto start_workers() -> Status override
    {
        return Status::ok();
    }

    [[nodiscard]]
    auto roll_forward(Id, const Callback &) -> Status override
    {
        return Status::ok();
    }

    [[nodiscard]]
    auto roll_backward(Id, const Callback &) -> Status override
    {
        return Status::ok();
    }

    [[nodiscard]]
    auto remove_before(Id) -> Status override
    {
        return Status::ok();
    }

    [[nodiscard]]
    auto remove_after(Id) -> Status override
    {
        return Status::ok();
    }
};

} // namespace calico

#endif // CALICO_DISABLED_WAL_H

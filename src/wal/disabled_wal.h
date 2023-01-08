#ifndef CALICO_DISABLED_WAL_H
#define CALICO_DISABLED_WAL_H

#include "wal.h"

namespace Calico {

class DisabledWriteAheadLog: public WriteAheadLog {
public:
    ~DisabledWriteAheadLog() override = default;

    [[nodiscard]]
    auto is_enabled() const -> bool override
    {
        return false;
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

    auto log(WalPayloadIn) -> void override
    {

    }

    auto flush() -> void override
    {

    }

    auto advance() -> void override
    {

    }

    [[nodiscard]]
    auto roll_forward(Id, const Callback &) -> Status override
    {
        return ok();
    }

    [[nodiscard]]
    auto roll_backward(Id, const Callback &) -> Status override
    {
        return ok();
    }

    auto cleanup(Id) -> void override
    {

    }

    [[nodiscard]] auto start_workers() -> Status override
    {
        return ok();
    }

    [[nodiscard]]
    auto remove_after(Id) -> Status override
    {
        return ok();
    }
};

} // namespace Calico

#endif // CALICO_DISABLED_WAL_H

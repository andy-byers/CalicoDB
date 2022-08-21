#ifndef CALICO_DISABLED_WAL_H
#define CALICO_DISABLED_WAL_H

#include "calico/wal.h"

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
    auto is_writing() const -> bool override
    {
        return false;
    }

    [[nodiscard]]
    auto flushed_lsn() const -> std::uint64_t override
    {
        return 0;
    }

    [[nodiscard]]
    auto current_lsn() const -> std::uint64_t override
    {
        return 0;
    }

    [[nodiscard]]
    auto log_image(std::uint64_t, BytesView) -> Status override
    {
        return Status::ok();
    }

    [[nodiscard]]
    auto log_deltas(std::uint64_t, BytesView, const std::vector<PageDelta> &) -> Status override
    {
        return Status::ok();
    }

    [[nodiscard]]
    auto log_commit() -> Status override
    {
        return Status::ok();
    }

    [[nodiscard]]
    auto stop_writer() -> Status override
    {
        return Status::ok();
    }

    [[nodiscard]]
    auto start_writer() -> Status override
    {
        return Status::ok();
    }

    [[nodiscard]]
    auto redo_all(const RedoCallback &) -> Status override
    {
        return Status::ok();
    }

    [[nodiscard]]
    auto undo_last(const UndoCallback &) -> Status override
    {
        return Status::ok();
    }

    auto allow_cleanup(std::uint64_t) -> void override {}
    auto save_state(FileHeader &) -> void override {}
    auto load_state(const FileHeader &) -> void override {}
};

} // namespace calico

#endif // CALICO_DISABLED_WAL_H

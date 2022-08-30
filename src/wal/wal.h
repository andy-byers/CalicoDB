#ifndef CALICO_WAL_H
#define CALICO_WAL_H

#include "calico/bytes.h"
#include "calico/status.h"

namespace calico {

struct FileHeader;

struct PageDelta {
    Size offset {};
    Size size {};
};

struct DeltaContent {
    Size offset {};
    BytesView data {};
};

struct RedoDescriptor {
    std::uint64_t page_id {};
    std::uint64_t page_lsn {};
    std::vector<DeltaContent> deltas;
    bool is_commit {};
};

struct UndoDescriptor {
    std::uint64_t page_id {};
    BytesView image;
};

using RedoCallback = std::function<Status(RedoDescriptor)>;

using UndoCallback = std::function<Status(UndoDescriptor)>;

class WriteAheadLog {
public:
    virtual ~WriteAheadLog() = default;

    [[nodiscard]]
    virtual auto is_enabled() const -> bool
    {
        return true;
    }

    [[nodiscard]] virtual auto is_writing() const -> bool = 0;
    [[nodiscard]] virtual auto flushed_lsn() const -> std::uint64_t = 0;
    [[nodiscard]] virtual auto current_lsn() const -> std::uint64_t = 0;
    [[nodiscard]] virtual auto log_image(std::uint64_t page_id, BytesView image) -> Status = 0;
    [[nodiscard]] virtual auto log_deltas(std::uint64_t page_id, BytesView image, const std::vector<PageDelta> &deltas) -> Status = 0;
    [[nodiscard]] virtual auto log_commit() -> Status = 0;
    [[nodiscard]] virtual auto flush_pending() -> Status = 0;
    [[nodiscard]] virtual auto stop_writer() -> Status = 0;
    [[nodiscard]] virtual auto start_writer() -> Status = 0;
    [[nodiscard]] virtual auto setup_and_recover(const RedoCallback &redo_cb, const UndoCallback &undo_cb) -> Status = 0;
    [[nodiscard]] virtual auto abort_last(const UndoCallback &callback) -> Status = 0;
    virtual auto allow_cleanup(std::uint64_t pager_lsn) -> void = 0;
    virtual auto save_state(FileHeader &) -> void = 0;
    virtual auto load_state(const FileHeader &) -> void = 0;
};

} // namespace calico

#endif // CALICO_WAL_H

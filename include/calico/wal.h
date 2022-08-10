#ifndef CCO_WAL_INTERFACE_H
#define CCO_WAL_INTERFACE_H

#include "page/file_header.h"
#include "utils/result.h"
#include "wal/wal_record.h"
#include <optional>

namespace cco {

class FileHeader; // TODO: Will need to be exposed in the public API.

/**
 * Interface representing a write-ahead log (WAL).
 *
 * Implementations are allowed to do writing and segment cleanup in the background.
 */
class WriteAheadLog {
public:
    virtual ~WriteAheadLog() = default;

    [[nodiscard]]
    virtual auto is_enabled() const -> bool
    {
        return true;
    }

    /**
     * Get the log sequence number (LSN) of the last WAL record written to disk.
     *
     * Since implementations of this interface are allowed to write in the background, the value returned by this method need not be exact. It must, however,
     * be less than or equal to the actual flushed LSN.
     *
     * @return The last LSN written to disk.
     */
    virtual auto flushed_lsn() const -> std::uint64_t = 0;

    /**
     * Log a record containing the entire contents of a database page, before it was made dirty by a write.
     *
     * Note that because the buffer pool implementation is allowed to "steal" frames (sometimes causing a dirty page to be written to disk during a transaction
     * and its frame reused), this method may be called multiple times for a given page during a given transaction. Implementations are allowed ignore subsequent
     * calls on the same page until commit() is called.
     *
     * TODO: Expose the update objects themselves instead of Page?
     *
     *       auto log_initial(RecordInfo, InitialInfo) -> Status;
     *       auto log_updates(RecordInfo, UpdateInfo) -> Status;
     *
     * @param page
     * @return
     */
    virtual auto log_initial(Page &page) -> Status = 0;
    virtual auto log_updates(Page &page) -> Status = 0;
    virtual auto commit() -> Status = 0;
    virtual auto abort() -> Status = 0;

    // TODO: Work on this interface and maybe avoid these. We're going to end up with a bunch more stuff in "include" otherwise. We'll potentially be managing a background
    //       writer thread from this object, so we have to keep that in mind. We may need to be able to explicitly stop and restart this thread, or just advance it to another
    //       segment so we can work on the one it was on.
    virtual auto save_state(FileHeader &) -> void = 0;
    virtual auto load_state(const FileHeader &) -> void = 0;
};

} // namespace cco

#endif // CCO_WAL_INTERFACE_H

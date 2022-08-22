#ifndef CALICO_WAL_H
#define CALICO_WAL_H

#include "bytes.h"
#include "status.h"

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

/**
 * Interface representing a write-ahead log (WAL).
 *
 * Implementations are allowed to do writing and cleanup in the background. Some of these methods return status objects. Note that if a non-OK status
 * is received, the WAL may be stopped and used to roll back the transaction (if possible).
 */
class WriteAheadLog {
public:
    virtual ~WriteAheadLog() = default;

    /**
     * Flag indicating if the WAL is enabled.
     *
     * If this method returns false, then the WAL does not have to do anything except provide stub method implementations. Also, if the WAL was enabled
     * when creating a given database, then it must always be enabled when opening that database (and vice versa).
     *
     * @return True if the WAL is enabled, false otherwise.
     */
    [[nodiscard]]
    virtual auto is_enabled() const -> bool
    {
        return true;
    }

    /**
     * Flag indicating if the WAL is being written to.
     *
     * This method provides a way to determine if any background writer/cleanup threads are running. If the WAL implementation does these things synchronously,
     * it should emulate this behavior so that the pager component knows how to behave. For example, if start_writer() returned Status::ok(), this method should
     * return true until stop_writer() is called and returns an OK status.
     *
     * @return True if the WAL is being written to, false otherwise.
     */
    virtual auto is_writing() const -> bool = 0;

    /**
     * Get the LSN of the last WAL record written to disk.
     *
     * Since implementations of this interface are allowed to write in the background, the value returned by this method need not be exact. It must, however,
     * be less than or equal to the actual flushed LSN.
     *
     * @return The last LSN written to disk.
     */
    virtual auto flushed_lsn() const -> std::uint64_t = 0;

    /**
     * Get the LSN of the next WAL record.
     *
     * This value, unlike the flushed LSN, needs to be exact. It is used to update the page LSN of a database page, before a WAL record is generated for it.
     *
     * @return The LSN of the next WAL record.
     */
    virtual auto current_lsn() const -> std::uint64_t = 0;

    /**
     * Log a record containing the entire contents of a database page, before it was made dirty by a write.
     *
     * Note that because the block pool implementation is allowed to "steal" frames (sometimes causing a dirty page to be written to disk during a transaction
     * and its frame reused), this method may be called multiple times for a given page during a given transaction. Implementations are allowed ignore subsequent
     * calls on the same page until commit() is called.
     *
     * @param page_id Page ID of the page contained in the image.
     * @param image Contents of the page before the first modification.
     * @return A status indicating success or failure.
     */
    virtual auto log_image(std::uint64_t page_id, BytesView image) -> Status = 0;

    /**
     *
     * @param page_id Page ID of the page that these collect_deltas are for.
     * @param image Contents of the page after the modifications.
     * @param deltas A collection of ranges representing unique regions of the page that have been updated.
     * @return A status indicating success or failure.
     */
    virtual auto log_deltas(std::uint64_t page_id, BytesView image, const std::vector<PageDelta> &deltas) -> Status = 0;

    /**
     *
     * @return A status indicating success or failure.
     */
    virtual auto log_commit() -> Status = 0;

    /**
     * Enter the stopped state.
     *
     * The WAL is allowed to write out records and perform cleanup routines in the background. This method should cause that routine to stop, and should not return
     * until it is safe to modify the entire log.
     *
     * @return A status indicating success or failure.
     */
    virtual auto stop_writer() -> Status = 0;

    /**
     * Enter the running state.
     *
     * This method starts up the background writer routine, if present, otherwise it does nothing.
     *
     * @return A status indicating success or failure.
     */
    virtual auto start_writer() -> Status = 0;

    /**
     * Open and roll the entire WAL.
     *
     * This method must be called before the WAL object is safe to use. It calls the provided callback for each delta record, and every commit record, in order,
     * allowing updates from the WAL to be applied to the database, if needed. It should return with success if the database is in a consistent state after all
     * updates have been applied. If the WAL is missing a commit record at the end, this method should return with a "not found" status. Otherwise, it
     * should report the error appropriately.
     *
     * @param callback A callback to call on each delta or commit record. The procedure will end early if a non-OK status is returned by the callback (indicating
     *                 the updates could not be applied for some reason).
     * @return A status indicating success or failure.
     */
    virtual auto open_and_recover(const RedoCallback &redo_cb, const UndoCallback &undo_cb) -> Status = 0;

    /**
     * Roll back the most recent transaction.
     *
     * This method calls the provided callback for each full image record belonging to the most recent transaction in reverse order. It can be used either at the
     * end of recovery, if a commit record was not encountered, or during a transaction to abort.
     *
     * @see open_and_recover()
     * @param callback A callback to call on each full page image.
     * @return A status indicating success or failure.
     */
    virtual auto undo_last(const UndoCallback &callback) -> Status = 0;

    /**
     * Indicate the point to which the log can be cleared.
     *
     * We only need to keep WAL records until their corresponding database pages have been written to disk. Afterwards, they are considered obsolete and we are free
     * to delete them.
     *
     * @param pager_lsn Largest page LSN written to disk by the pager component.
     */
    virtual auto allow_cleanup(std::uint64_t pager_lsn) -> void = 0;

    virtual auto save_state(FileHeader &) -> void = 0;

    virtual auto load_state(const FileHeader &) -> void = 0;
};

} // namespace calico

#endif // CALICO_WAL_H

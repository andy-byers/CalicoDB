#ifndef CALICODB_RECOVERY_H
#define CALICODB_RECOVERY_H

#include "calicodb/env.h"
#include "types.h"
#include <unordered_map>

namespace calicodb
{

class Pager;
class WriteAheadLog;
class WalSet;

/* Recovery routine. Makes the database consistent with the WAL. This process is split up into 2
 * phases which are described below.
 */
class Recovery
{
public:
    explicit Recovery(Pager &pager, WriteAheadLog &wal, Lsn &commit_lsn);
    [[nodiscard]] auto recover() -> Status;

    [[nodiscard]] auto operator()(std::unordered_map<Id, Lsn> &checkpoints) -> Status;

private:
    [[nodiscard]] auto open_reader(Id segment, std::unique_ptr<Reader> &out) -> Status;
    [[nodiscard]] auto find_checkpoints(std::unordered_map<Id, Lsn> *checkpoints) -> Status;


    [[nodiscard]] auto recover_phase_1() -> Status;
    [[nodiscard]] auto recover_phase_2() -> Status;

    struct LogRange {
        Lsn commit_lsn;
        Lsn recent_lsn;
    };

    /* Phase 1: Read the WAL and apply missing updates until we reach the end. Maintain a commit LSN
     * and a most-recent LSN for each table we encounter. Then, read the WAL again, this time reverting
     * changes for tables that had a commit LSN less than their most-recent LSN. If, for a given table,
     * we find a most-recent LSN, but not a commit LSN, we assume the commit LSN to be what is written
     * in the table header. Once finished, we will have some updates in-memory that are not yet on disk.
     * Update each table's header LSN to match the most-recent LSN created for that table. This prevents
     * the WAL records we just reverted from being considered again if we crash while cleaning up.
     */
    [[nodiscard]] auto recovery_phase_1() -> Status;

    /* Phase 2: Flush the page cache, resize the database file to match the header page count, then remove
     * all WAL segments. This part is only run when the database is closed, otherwise, we just flush the
     * page cache.
     */
    [[nodiscard]] auto recovery_phase_2() -> Status;

    std::unordered_map<Id, LogRange, Id::Hash> m_live;
    LogRange m_global;

    std::string m_reader_data;
    std::string m_reader_tail;
    Pager *m_pager {};
    Env *m_env {};
    WalSet *m_set {};
    WriteAheadLog *m_wal {};
    Lsn *m_commit_lsn {};
};

} // namespace calicodb

#endif // CALICODB_RECOVERY_H

#ifndef CALICODB_DB_IMPL_H
#define CALICODB_DB_IMPL_H

#include "calicodb/db.h"
#include "calicodb/options.h"
#include "calicodb/table.h"

#include "header.h"
#include "pager.h"
#include "tree.h"
#include "wal.h"
#include "wal_writer.h"

namespace calicodb
{

class Cursor;
class DBImpl;
class Env;
class TableImpl;
class WriteAheadLog;

class DBImpl : public DB
{
public:
    friend class DB;

    DBImpl() = default;
    ~DBImpl() override;

    [[nodiscard]] static auto destroy(const Options &options, const std::string &filename) -> Status;
    [[nodiscard]] static auto repair(const Options &options, const std::string &filename) -> Status;
    [[nodiscard]] auto open(const Options &options, const Slice &filename) -> Status;

    [[nodiscard]] auto get_property(const Slice &name, std::string *out) const -> bool override;
    [[nodiscard]] auto new_table(const TableOptions &options, const Slice &name, Table **out) -> Status override;
    [[nodiscard]] auto status() const -> Status override;
    [[nodiscard]] auto vacuum() -> Status override;

    [[nodiscard]] auto create_table(const Slice &name, LogicalPageId *root_id) -> Status;
    [[nodiscard]] auto open_table(const LogicalPageId &root_id, TableState **out) -> Status;
    [[nodiscard]] auto commit_table(const LogicalPageId &root_id, TableState &state) -> Status;
    auto close_table(const LogicalPageId &root_id) -> void;

    [[nodiscard]] auto record_count() const -> std::size_t;
    auto TEST_validate() const -> void;

    WriteAheadLog *wal {};
    Pager *pager {};

private:
    [[nodiscard]] auto do_open(Options sanitized) -> Status;
    [[nodiscard]] auto ensure_consistency() -> Status;
    [[nodiscard]] auto load_state() -> Status;
    [[nodiscard]] auto do_vacuum() -> Status;
    auto save_state(Page &root, Lsn checkpoint_lsn) const -> void;

    [[nodiscard]] auto open_wal_reader(Id segment, std::unique_ptr<Reader> *out) -> Status;

    /* Determine the header checkpoint LSN for every table in the database. This amounts to iterating
     * over the root table's records and finding the table root pages, from which the checkpoint LSNs
     * are read.
     */
    [[nodiscard]] auto find_checkpoints(std::unordered_map<Id, Lsn, Id::Hash> *checkpoints) -> Status;

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
    [[nodiscard]] auto recovery_phase_1(std::unordered_map<Id, LogRange, Id::Hash> ranges) -> Status;

    /* Phase 2: Flush the page cache, resize the database file to match the header page count, then remove
     * all WAL segments. This part is only run when the database is closed, otherwise, we just flush the
     * page cache.
     */
    [[nodiscard]] auto recovery_phase_2(Lsn recent_lsn) -> Status;

    std::string m_reader_data;
    std::string m_reader_tail;

    // State for open tables. Tables are keyed by their table ID.
    std::unordered_map<Id, TableState, Id::Hash> m_tables;

    // Mapping from root page IDs to table IDs for open tables. This is necessary for vacuum.
    std::unordered_map<Id, Id, Id::Hash> m_root_map;

    // Pointer to the root table state, which is kept in m_tables.
    TableState *m_root {};

    mutable Status m_status;
    std::string m_filename;
    std::string m_wal_prefix;
    std::string m_scratch;
    Env *m_env {};
    InfoLogger *m_info_log {};
    std::size_t m_txn_size {};
    std::size_t m_record_count {};
    std::size_t m_bytes_written {};
    Id m_freelist_head;
    Id m_last_table_id;
    bool m_owns_env {};
    bool m_owns_info_log {};
    bool m_is_running {};
};

auto setup(const std::string &, Env &, const Options &, FileHeader &state) -> Status;

} // namespace calicodb

#endif // CALICODB_DB_IMPL_H

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
struct TableState;

template<class T>
using IdMap = std::unordered_map<Id, T, Id::Hash>;

class TableSet {
public:
    using Iterator = IdMap<TableState>::const_iterator;

    [[nodiscard]] auto get(Id table_id) const -> const TableState *;
    [[nodiscard]] auto get(Id table_id) -> TableState *;
    [[nodiscard]] auto begin() const -> Iterator;
    [[nodiscard]] auto end() const -> Iterator;
    auto add(const LogicalPageId &root_id) -> void;
    auto erase(Id table_id) -> void;

private:
    IdMap<TableState> m_tables;
};

class DBImpl : public DB
{
public:
    friend class DB;

    explicit DBImpl(const Options &options, const Options &sanitized, std::string filename);
    ~DBImpl() override;

    [[nodiscard]] static auto destroy(const Options &options, const std::string &filename) -> Status;
    [[nodiscard]] static auto repair(const Options &options, const std::string &filename) -> Status;
    [[nodiscard]] auto open(const Options &sanitized) -> Status;

    [[nodiscard]] auto get_property(const Slice &name, std::string *out) const -> bool override;
    [[nodiscard]] auto new_table(const TableOptions &options, const Slice &name, Table **out) -> Status override;
    [[nodiscard]] auto checkpoint() -> Status override;
    [[nodiscard]] auto status() const -> Status override;
    [[nodiscard]] auto vacuum() -> Status override;

    [[nodiscard]] auto create_table(const Slice &name, LogicalPageId *root_id) -> Status;
    [[nodiscard]] auto open_table(TableState &out) -> Status;
    auto close_table(const LogicalPageId &root_id) -> void;

    [[nodiscard]] auto record_count() const -> std::size_t;
    [[nodiscard]] auto TEST_tables() const -> const TableSet &;
    auto TEST_validate() const -> void;

    WriteAheadLog *wal {};
    Pager *pager {};

private:
    [[nodiscard]] auto ensure_consistency() -> Status;
    [[nodiscard]] auto do_checkpoint() -> Status;
    [[nodiscard]] auto load_state() -> Status;
    [[nodiscard]] auto do_vacuum() -> Status;

    [[nodiscard]] auto open_wal_reader(Id segment, std::unique_ptr<Reader> *out) -> Status;

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

    std::string m_reader_data;
    std::string m_reader_tail;

    // State for put tables. Tables are keyed by their table ID.
    TableSet m_tables;

    // Pointer to the root table state, which is kept in m_tables.
    TableState *m_root {};

    mutable DBState m_state;

    std::string m_filename;
    std::string m_wal_prefix;
    Env *m_env {};
    InfoLogger *m_info_log {};
    Lsn m_commit_lsn;
    Id m_freelist_head;
    Id m_last_table_id;
    bool m_owns_env {};
    bool m_owns_info_log {};
    bool m_is_running {};
};

auto setup(const std::string &, Env &, const Options &, FileHeader *state) -> Status;

} // namespace calicodb

#endif // CALICODB_DB_IMPL_H

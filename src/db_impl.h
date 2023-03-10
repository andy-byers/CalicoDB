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

#include <map>

namespace calicodb
{

class Cursor;
class DBImpl;
class Env;
class TableImpl;
class WriteAheadLog;
struct TableState;

static constexpr auto kRootTableName = "calicodb_root";

class TableSet
{
public:
    using Iterator = std::map<Id, TableState>::const_iterator;

    [[nodiscard]] auto get(Id table_id) const -> const TableState *;
    [[nodiscard]] auto get(Id table_id) -> TableState *;
    [[nodiscard]] auto begin() const -> Iterator;
    [[nodiscard]] auto end() const -> Iterator;
    auto add(const LogicalPageId &root_id) -> void;
    auto erase(Id table_id) -> void;

private:
    std::map<Id, TableState> m_tables;
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
    [[nodiscard]] auto create_table(const TableOptions &options, const std::string &name, Table **out) -> Status override;
    [[nodiscard]] auto drop_table(const std::string &name) -> Status override;
    [[nodiscard]] auto checkpoint() -> Status override;
    [[nodiscard]] auto status() const -> Status override;
    [[nodiscard]] auto vacuum() -> Status override;

    [[nodiscard]] auto record_count() const -> std::size_t;
    [[nodiscard]] auto TEST_tables() const -> const TableSet &;
    auto TEST_validate() const -> void;

    WriteAheadLog *wal {};
    Pager *pager {};

private:
    [[nodiscard]] auto remove_empty_table(const std::string &name, TableState &state) -> Status;
    [[nodiscard]] auto save_file_header() -> Status;
    [[nodiscard]] auto load_file_header() -> Status;
    [[nodiscard]] auto do_vacuum() -> Status;
    [[nodiscard]] auto open_wal_reader(Id segment, std::unique_ptr<Reader> *out) -> Status;
    [[nodiscard]] auto ensure_consistency() -> Status;
    [[nodiscard]] auto recovery_phase_1() -> Status;
    [[nodiscard]] auto recovery_phase_2() -> Status;
    [[nodiscard]] auto construct_new_table(const Slice &name, LogicalPageId *root_id) -> Status;
    auto close_table(const LogicalPageId &root_id) -> void;

    friend class TableImpl;

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
    bool m_owns_env {};
    bool m_owns_info_log {};
    bool m_is_running {};
};

auto setup(const std::string &, Env &, const Options &, FileHeader *state) -> Status;

} // namespace calicodb

#endif // CALICODB_DB_IMPL_H

// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_DB_IMPL_H
#define CALICODB_DB_IMPL_H

#include "calicodb/db.h"
#include "calicodb/table.h"

#include "header.h"
#include "pager.h"
#include "tree.h"
#include "wal.h"

#include <functional>
#include <map>

namespace calicodb
{

class Cursor;
class DBImpl;
class Env;
class TableImpl;
class Wal;
struct TableState;

static constexpr auto kRootTableName = "calicodb.root";
static constexpr auto kDefaultTableName = "calicodb.default";

struct TableState {
    LogicalPageId root_id;
    TreeStatistics stats;
    Tree *tree = nullptr;
    bool write = false;
    bool open = false;
};

class TableImpl : public Table
{
public:
    friend class DBImpl;

    ~TableImpl() override = default;
    explicit TableImpl(std::string name, TableState &state, Id table_id);

    [[nodiscard]] auto name() const -> const std::string & override
    {
        return m_name;
    }

    [[nodiscard]] auto id() const -> Id
    {
        return m_id;
    }

    [[nodiscard]] auto state() -> TableState &
    {
        return *m_state;
    }

    [[nodiscard]] auto state() const -> const TableState &
    {
        return *m_state;
    }

private:
    std::string m_name;
    TableState *m_state;
    Id m_id;
};

class TableSet final
{
public:
    using Iterator = std::vector<TableState *>::const_iterator;

    ~TableSet();
    [[nodiscard]] auto get(Id table_id) const -> const TableState *;
    [[nodiscard]] auto get(Id table_id) -> TableState *;
    [[nodiscard]] auto begin() const -> Iterator;
    [[nodiscard]] auto end() const -> Iterator;
    auto add(const LogicalPageId &root_id) -> void;
    auto erase(Id table_id) -> void;

private:
    std::vector<TableState *> m_tables;
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
    [[nodiscard]] auto default_table() const -> Table * override;
    [[nodiscard]] auto create_table(const TableOptions &options, const std::string &name, Table *&out) -> Status override;
    [[nodiscard]] auto list_tables(std::vector<std::string> &out) const -> Status override;
    [[nodiscard]] auto drop_table(Table *&table) -> Status override;
    auto close_table(Table *&table) -> void override;

    [[nodiscard]] auto begin_txn(const TxnOptions &options) -> unsigned override;
    [[nodiscard]] auto commit_txn(unsigned txn) -> Status override;
    [[nodiscard]] auto rollback_txn(unsigned txn) -> Status override;

    [[nodiscard]] auto status() const -> Status override;
    [[nodiscard]] auto vacuum() -> Status override;

    using DB::new_cursor;
    [[nodiscard]] auto new_cursor(const Table &table) const -> Cursor * override;

    using DB::get;
    [[nodiscard]] auto get(const Table &table, const Slice &key, std::string *value) const -> Status override;

    using DB::put;
    [[nodiscard]] auto put(Table &table, const Slice &key, const Slice &value) -> Status override;

    using DB::erase;
    [[nodiscard]] auto erase(Table &table, const Slice &key) -> Status override;

    [[nodiscard]] auto TEST_wal() const -> const Wal &;
    [[nodiscard]] auto TEST_pager() const -> const Pager &;
    [[nodiscard]] auto TEST_tables() const -> const TableSet &;
    [[nodiscard]] auto TEST_state() const -> const DBState &;
    auto TEST_validate() const -> void;

private:
    [[nodiscard]] auto get_table_info(std::vector<std::string> &names, std::vector<LogicalPageId> *roots) const -> Status;
    [[nodiscard]] auto construct_new_table(const Slice &name, LogicalPageId &root_id) -> Status;
    [[nodiscard]] auto remove_empty_table(const std::string &name, TableState &state) -> Status;
    [[nodiscard]] auto checkpoint_if_needed(bool force = false) -> Status;
    [[nodiscard]] auto load_file_header() -> Status;

    [[nodiscard]] auto ensure_txn_started(bool &implicit_txn) -> Status;
    [[nodiscard]] auto ensure_txn_finished(bool implicit_txn) -> Status;
    [[nodiscard]] auto do_put(Table &table, const Slice &key, const Slice &value) -> Status;
    [[nodiscard]] auto do_erase(Table &table, const Slice &key) -> Status;
    [[nodiscard]] auto do_vacuum() -> Status;
    [[nodiscard]] auto do_create_table(const TableOptions &options, const std::string &name, Table *&out) -> Status;
    [[nodiscard]] auto do_drop_table(Table *&table) -> Status;

    mutable DBState m_state;

    TableSet m_tables;
    Table *m_default = nullptr;
    Table *m_root = nullptr;

    Wal *m_wal = nullptr;
    Pager *m_pager = nullptr;
    Env *m_env = nullptr;
    LogFile *m_log = nullptr;

    unsigned m_txn = 0;

    const std::string m_db_filename;
    const std::string m_wal_filename;
    const std::string m_log_filename;
    const bool m_owns_env;
    const bool m_owns_log;
    const bool m_sync;
};

} // namespace calicodb

#endif // CALICODB_DB_IMPL_H

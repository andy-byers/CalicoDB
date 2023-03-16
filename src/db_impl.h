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
static constexpr auto kDefaultTableName = "default";

struct TableState {
    LogicalPageId root_id;
    Tree *tree {};
    bool write {};
    bool open {};
};

class TableImpl : public Table
{
public:
    friend class DBImpl;

    ~TableImpl() override = default;
    explicit TableImpl(const TableOptions &options, std::string name, Id table_id);

    [[nodiscard]] auto name() const -> const std::string & override
    {
        return m_name;
    }

    [[nodiscard]] auto id() const -> Id
    {
        return m_id;
    }

private:
    TableOptions m_options;
    std::string m_name;
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

    [[nodiscard]] auto checkpoint() -> Status override;
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

    [[nodiscard]] auto TEST_wal() const -> const WriteAheadLog &;
    [[nodiscard]] auto TEST_pager() const -> const Pager &;
    [[nodiscard]] auto TEST_tables() const -> const TableSet &;
    [[nodiscard]] auto TEST_state() const -> const DBState &;
    auto TEST_validate() const -> void;

private:
    [[nodiscard]] auto remove_empty_table(const std::string &name, TableState &state) -> Status;
    [[nodiscard]] auto do_checkpoint() -> Status;
    [[nodiscard]] auto load_file_header() -> Status;
    [[nodiscard]] auto do_vacuum() -> Status;
    [[nodiscard]] auto open_wal_reader(Id segment, std::unique_ptr<Reader> &out) -> Status;
    [[nodiscard]] auto ensure_consistency() -> Status;
    [[nodiscard]] auto recovery_phase_1() -> Status;
    [[nodiscard]] auto recovery_phase_2() -> Status;
    [[nodiscard]] auto construct_new_table(const Slice &name, LogicalPageId &root_id) -> Status;

    TableSet m_tables;
    Table *m_default {};
    Table *m_root {};

    mutable DBState m_state;

    std::string m_filename;
    std::string m_wal_prefix;
    Env *m_env {};
    InfoLogger *m_info_log {};
    WriteAheadLog *m_wal {};
    Pager *m_pager {};
    bool m_owns_env {};
    bool m_owns_info_log {};
};

auto setup_db(const std::string &, Env &, const Options &, FileHeader &state) -> Status;

} // namespace calicodb

#endif // CALICODB_DB_IMPL_H

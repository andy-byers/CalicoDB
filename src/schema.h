// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_SCHEMA_H
#define CALICODB_SCHEMA_H

#include "calicodb/db.h"
#include "page.h"
#include "tree.h"
#include <unordered_map>

namespace calicodb
{

class TableImpl;

// Representation of the database schema
class Schema final
{
public:
    explicit Schema(Pager &pager, Status &status);
    ~Schema();

    [[nodiscard]] auto new_cursor() -> Cursor *;
    [[nodiscard]] auto create_table(const TableOptions &options, const Slice &name, bool readonly, Table **tb_out) -> Status;
    [[nodiscard]] auto drop_table(const Slice &name) -> Status;

    [[nodiscard]] auto vacuum_page(Id page_id, bool &success) -> Status;

    // Write updated root page IDs for tables that were closed during vacuum, if any
    // tables were rerooted
    [[nodiscard]] auto vacuum_finish() -> Status;

    auto TEST_validate() const -> void;

private:
    [[nodiscard]] auto corrupted_root_id(const Slice &name, const Slice &value) -> Status;
    [[nodiscard]] auto construct_table_state(Id root_id, bool readonly) -> Table *;
    [[nodiscard]] auto decode_root_id(const Slice &data, Id &out) -> bool;
    static auto encode_root_id(Id id, std::string &out) -> void;

    template <class T>
    using HashMap = std::unordered_map<Id, T, Id::Hash>;

    // Change the root page of a table from "old_id" to "new_id" during vacuum
    friend class Tree;
    auto vacuum_reroot(Id old_id, Id new_id) -> void;

    struct RootedTable {
        Table *table = nullptr;
        Id root;
    };

    HashMap<RootedTable> m_tables;
    HashMap<Id> m_reroot;
    Status *m_status;
    Pager *m_pager;
    Tree m_map;
};

} // namespace calicodb

#endif // CALICODB_SCHEMA_H

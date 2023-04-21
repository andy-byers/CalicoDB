// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_DB_H
#define CALICODB_DB_H

#include "options.h"
#include "status.h"
#include <vector>

namespace calicodb
{

class Cursor;
class Env;
class Table;
class Txn;

// On-disk collection of tables
class DB
{
public:
    [[nodiscard]] static auto open(const Options &options, const std::string &filename, DB *&db) -> Status;
    [[nodiscard]] static auto destroy(const Options &options, const std::string &filename) -> Status;

    explicit DB();
    virtual ~DB();

    DB(const DB &) = delete;
    auto operator=(const DB &) -> void = delete;

    [[nodiscard]] virtual auto get_property(const Slice &name, std::string *out) const -> bool = 0;
    [[nodiscard]] virtual auto begin(const TxnOptions &options, Txn *&txn) -> Status = 0;
    [[nodiscard]] virtual auto commit(Txn &txn) -> Status = 0;
    virtual auto rollback(Txn &txn) -> void = 0;
};

class Txn
{
public:
    virtual ~Txn();
    [[nodiscard]] virtual auto status() const -> Status = 0;
    [[nodiscard]] virtual auto new_table(const TableOptions &options, const std::string &name, Table *&out) -> Status = 0;
    [[nodiscard]] virtual auto drop_table(const std::string &name) -> Status = 0;
    [[nodiscard]] virtual auto vacuum() -> Status = 0;
};

class Table
{
public:
    virtual ~Table();
    [[nodiscard]] virtual auto new_cursor() const -> Cursor * = 0;
    [[nodiscard]] virtual auto get(const Slice &key, std::string *value) const -> Status = 0;
    [[nodiscard]] virtual auto put(const Slice &key, const Slice &value) -> Status = 0;
    [[nodiscard]] virtual auto erase(const Slice &key) -> Status = 0;
};

} // namespace calicodb

#endif // CALICODB_DB_H

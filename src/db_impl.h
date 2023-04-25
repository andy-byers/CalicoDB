// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_DB_IMPL_H
#define CALICODB_DB_IMPL_H

#include "calicodb/db.h"

#include "header.h"
#include "pager.h"
#include "tree.h"
#include "wal.h"

#include <functional>
#include <map>

namespace calicodb
{

class Env;
class TxnImpl;
class Wal;

class DBImpl : public DB
{
public:
    friend class DB;

    explicit DBImpl(const Options &options, const Options &sanitized, std::string filename);
    ~DBImpl() override;

    [[nodiscard]] static auto destroy(const Options &options, const std::string &filename) -> Status;
    [[nodiscard]] auto open(const Options &sanitized) -> Status;

    [[nodiscard]] auto get_property(const Slice &name, std::string *out) const -> bool override;
    [[nodiscard]] auto start(bool write, Txn *&out) -> Status override;
    auto finish(Txn *&out) -> void override;

    [[nodiscard]] auto TEST_wal() const -> const Wal &;
    [[nodiscard]] auto TEST_pager() const -> const Pager &;
    [[nodiscard]] auto TEST_state() const -> const DBState &;

private:
    DBState m_state;
    Wal *m_wal = nullptr;
    Pager *m_pager = nullptr;

    Env *m_env = nullptr;
    Sink *m_log = nullptr;

    const std::string m_db_filename;
    const std::string m_wal_filename;
    const std::string m_shm_filename;
    const std::string m_log_filename;
    const bool m_owns_env;
    const bool m_owns_log;
    const bool m_sync;
};

} // namespace calicodb

#endif // CALICODB_DB_IMPL_H

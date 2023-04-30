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

struct DBState {
    Status status;
    bool use_wal = false;
};

class DBImpl : public DB
{
public:
    friend class DB;

    explicit DBImpl(const Options &options, const Options &sanitized, std::string filename);
    ~DBImpl() override;

    [[nodiscard]] static auto destroy(const Options &options, const std::string &filename) -> Status;
    [[nodiscard]] auto open(const Options &sanitized) -> Status;

    [[nodiscard]] auto get_property(const Slice &name, std::string *out) const -> bool override;
    [[nodiscard]] auto new_txn(bool write, Txn *&txn) -> Status override;

    [[nodiscard]] auto TEST_pager() const -> const Pager &;
    [[nodiscard]] auto TEST_state() const -> const DBState &;

private:
    DBState m_state;
    Pager *m_pager = nullptr;

    Env *const m_env = nullptr;
    Sink *const m_log = nullptr;
    BusyHandler *const m_busy = nullptr;

    TxnImpl *m_txn = nullptr;

    const std::string m_db_filename;
    const std::string m_wal_filename;
    const bool m_owns_env;
    const bool m_owns_log;
    const bool m_sync;
};

inline auto db_impl(DB *db) -> DBImpl *
{
    return reinterpret_cast<DBImpl *>(db);
}
inline auto db_impl(const DB *db) -> const DBImpl *
{
    return reinterpret_cast<const DBImpl *>(db);
}

} // namespace calicodb

#endif // CALICODB_DB_IMPL_H

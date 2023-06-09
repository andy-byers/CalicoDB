// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_DB_IMPL_H
#define CALICODB_DB_IMPL_H

#include "calicodb/db.h"

#include "header.h"
#include "tree.h"
#include "wal.h"

#include <functional>
#include <map>

namespace calicodb
{

class Env;
class Pager;
class TxImpl;
class Wal;

class DBImpl : public DB
{
public:
    friend class DB;

    explicit DBImpl(const Options &options, const Options &sanitized, std::string filename);
    ~DBImpl() override;

    static auto destroy(const Options &options, const std::string &filename) -> Status;
    auto open(const Options &sanitized) -> Status;

    [[nodiscard]] auto get_property(const Slice &name, std::string *out) const -> bool override;
    auto new_tx(const Tx *&tx) const -> Status override;
    auto new_tx(WriteTag, Tx *&tx) -> Status override;
    auto checkpoint(bool reset) -> Status override;

    [[nodiscard]] auto TEST_pager() const -> const Pager &;

private:
    template <class TxType>
    auto prepare_tx(bool write, TxType *&tx_out) const -> Status;

    mutable Status m_status;
    mutable TxImpl *m_tx = nullptr;
    char *m_scratch = nullptr;

    Pager *m_pager = nullptr;

    Env *const m_env = nullptr;
    Logger *const m_log = nullptr;
    BusyHandler *const m_busy = nullptr;

    const std::string m_db_filename;
    const std::string m_wal_filename;
    const bool m_owns_log;
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

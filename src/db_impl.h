// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_DB_IMPL_H
#define CALICODB_DB_IMPL_H

#include "calicodb/db.h"
#include "error.h"
#include "ptr.h"
#include "stat.h"

namespace calicodb
{

class File;
class Env;
class Pager;
class TxImpl;
class Wal;

class DBImpl : public DB
{
public:
    friend class DB;

    ~DBImpl() override;

    static auto destroy(const Options &options, const char *filename) -> Status;
    auto open(const Options &sanitized) -> Status;

    [[nodiscard]] auto get_property(const Slice &name, Slice *out) const -> bool override;
    auto new_tx(Tx *&tx) const -> Status override;
    auto new_tx(WriteTag, Tx *&tx) -> Status override;
    auto checkpoint(bool reset) -> Status override;

    [[nodiscard]] auto TEST_pager() const -> const Pager &;

private:
    struct Parameters {
        Options sanitized;
        UniqueBuffer db_name;
        UniqueBuffer wal_name;
        UniqueBuffer scratch;
    };
    friend class Alloc;
    explicit DBImpl(Parameters param);

    template <class TxType>
    auto prepare_tx(bool write, TxType *&tx_out) const -> Status;

    mutable ErrorState m_errors;
    mutable Status m_status;
    mutable TxImpl *m_tx = nullptr;
    mutable Stat m_stat;
    mutable UniqueBuffer m_scratch;
    mutable UniqueBuffer m_property;
    mutable ObjectPtr<Pager> m_pager;

    UserPtr<File> m_file;
    Env *const m_env;
    Logger *const m_log;
    BusyHandler *const m_busy;

    const size_t m_auto_ckpt;
    const UniqueBuffer m_db_filename;
    const UniqueBuffer m_wal_filename;
    const bool m_owns_log;
    const bool m_owns_env;
};

} // namespace calicodb

#endif // CALICODB_DB_IMPL_H

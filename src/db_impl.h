// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_DB_IMPL_H
#define CALICODB_DB_IMPL_H

#include "buffer.h"
#include "calicodb/db.h"
#include "unique_ptr.h"

namespace calicodb
{

class File;
class Env;
class Pager;
class TxImpl;
class Wal;

class DBImpl
    : public DB,
      public HeapObject
{
public:
    friend class DB;

    ~DBImpl() override;

    static auto destroy(const Options &options, const char *filename) -> Status;
    auto open(const Options &sanitized) -> Status;

    auto get_property(const Slice &name, void *value_out) const -> Status override;
    auto new_reader(Tx *&tx) const -> Status override;
    auto new_writer(Tx *&tx) -> Status override;
    auto checkpoint(CheckpointMode mode, CheckpointInfo *info_out) -> Status override;

    [[nodiscard]] auto TEST_pager() const -> Pager &;

private:
    struct Parameters {
        Options original;
        Options sanitized;
        String db_name;
        String wal_name;
    };
    friend class Mem;
    explicit DBImpl(Parameters param);

    template <class TxType>
    auto prepare_tx(bool write, TxType *&tx_out) const -> Status;

    mutable Status m_status;
    mutable TxImpl *m_tx = nullptr;
    mutable ObjectPtr<Pager> m_pager;
    mutable Stats m_stats;

    UserPtr<File> m_file;
    ObjectPtr<Wal> m_temp_wal;
    Env *const m_env;
    Logger *const m_log;
    BusyHandler *const m_busy;

    const size_t m_auto_ckpt;
    const String m_db_filename;
    const String m_wal_filename;
    const bool m_owns_env;
};

} // namespace calicodb

#endif // CALICODB_DB_IMPL_H

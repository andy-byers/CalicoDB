// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_WAL_INTERNAL_H
#define CALICODB_WAL_INTERNAL_H

#include "calicodb/wal.h"

namespace calicodb
{

struct WalOptionsExtra : public WalOptions {
    Logger *info_log;
    BusyHandler *busy;
    Options::SyncMode sync_mode;
    Options::LockMode lock_mode;
};

[[nodiscard]] auto new_default_wal(const WalOptionsExtra &options, const char *filename) -> Wal *;

class WalPagesImpl : public Wal::Pages
{
    PageRef *const m_first;
    PageRef *m_itr;
    mutable Data m_data;

public:
    explicit WalPagesImpl(PageRef &first);
    ~WalPagesImpl() override;

    auto value() const -> Data * override;
    auto next() -> void override;
    auto reset() -> void override;
};

} // namespace calicodb

#endif // CALICODB_WAL_H

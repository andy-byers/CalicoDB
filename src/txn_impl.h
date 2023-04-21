// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_TXN_IMPL_H
#define CALICODB_TXN_IMPL_H

#include "schema.h"
#include "table_impl.h"

namespace calicodb
{

class TxnImpl : public Txn
{
public:
    explicit TxnImpl(Pager &pager, DBState &state);
    ~TxnImpl() override;
    [[nodiscard]] auto status() const -> Status override;
    [[nodiscard]] auto new_table(const TableOptions &options, const std::string &name, Table *&out) -> Status override;
    [[nodiscard]] auto drop_table(const std::string &name) -> Status override;
    [[nodiscard]] auto vacuum() -> Status override;

private:
    Schema m_schema;
    Pager *m_pager;
    DBState *m_state;
};

} // namespace calicodb

#endif // CALICODB_TXN_IMPL_H

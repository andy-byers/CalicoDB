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
    explicit TxnImpl(Pager &pager, Status &status, bool write);
    ~TxnImpl() override;

    [[nodiscard]] auto status() const -> Status override
    {
        return *m_status;
    }

    [[nodiscard]] auto schema() const -> Cursor & override
    {
        return *m_schema;
    }

    [[nodiscard]] auto create_table(const TableOptions &options, const Slice &name, Table **tb_out) -> Status override;
    [[nodiscard]] auto drop_table(const Slice &name) -> Status override;
    [[nodiscard]] auto vacuum() -> Status override;
    [[nodiscard]] auto commit() -> Status override;

    auto TEST_validate() const -> void
    {
        m_schema_obj.TEST_validate();
    }

private:
    friend class DBImpl;

    [[nodiscard]] auto vacuum_freelist() -> Status;

    TxnImpl **m_backref = nullptr;
    Schema m_schema_obj;
    Cursor *m_schema;
    Pager *m_pager;
    Status *m_status;
    bool m_write = false;
};

inline auto txn_impl(Txn *tx) -> TxnImpl *
{
    return reinterpret_cast<TxnImpl *>(tx);
}
inline auto txn_impl(const Txn *tx) -> const TxnImpl *
{
    return reinterpret_cast<const TxnImpl *>(tx);
}

} // namespace calicodb

#endif // CALICODB_TXN_IMPL_H

// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_TX_IMPL_H
#define CALICODB_TX_IMPL_H

#include "bucket_impl.h"
#include "calicodb/tx.h"
#include "schema.h"

namespace calicodb
{

struct BucketOptions;
struct Stats;

class TxImpl
    : public Tx,
      public HeapObject
{
public:
    struct Parameters {
        Pager *pager;
        Stats *stat;
        bool writable;
    };
    explicit TxImpl(const Parameters &param);
    ~TxImpl() override;

    auto status() const -> Status override
    {
        return m_schema.pager().status();
    }

    auto toplevel() const -> Cursor & override
    {
        return m_toplevel;
    }

    auto create_bucket(const Slice &name, Bucket **b_out) -> Status override;
    auto create_bucket_if_missing(const Slice &name, Bucket **b_out) -> Status override;
    auto open_bucket(const Slice &name, Bucket *&b_out) const -> Status override;
    auto drop_bucket(const Slice &name) -> Status override;

    auto vacuum() -> Status override;
    auto commit() -> Status override;

    auto TEST_validate() const -> void
    {
        m_schema.TEST_validate();
    }

private:
    // m_backref is not known until after the constructor runs. Let DBImpl set it.
    friend class DBImpl;

    mutable Schema m_schema;
    mutable BucketImpl m_main;
    mutable CursorImpl m_toplevel;
    TxImpl **m_backref = nullptr;
};

} // namespace calicodb

#endif // CALICODB_TX_IMPL_H

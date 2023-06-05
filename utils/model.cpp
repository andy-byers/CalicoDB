// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "model.h"

namespace calicodb
{

ModelDB::~ModelDB()
{
    if (m_owns_store) {
        delete m_store;
    }
}

auto ModelDB::new_tx(WriteTag, Tx *&tx_out) -> Status
{
    tx_out = new ModelTx(*m_store);
    return Status::ok();
}

auto ModelDB::new_tx(const Tx *&tx_out) const -> Status
{
    tx_out = new ModelTx(*m_store);
    return Status::ok();
}

ModelTx::~ModelTx() = default;

auto ModelTx::create_bucket(const BucketOptions &, const Slice &, Bucket *) -> Status
{
    return Status::ok();
}

auto ModelTx::open_bucket(const Slice &, Bucket &) const -> Status
{
    return Status::ok();
}

auto ModelTx::new_cursor(const Bucket &) const -> Cursor *
{
    return new ModelCursor(m_temp);
}

ModelCursor::~ModelCursor() = default;

} // namespace calicodb
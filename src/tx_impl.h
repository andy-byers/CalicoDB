// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_TX_IMPL_H
#define CALICODB_TX_IMPL_H

#include "schema.h"

namespace calicodb
{

struct Bucket;
struct BucketOptions;

class TxImpl : public Tx
{
public:
    explicit TxImpl(Pager &pager, Status &status, char *scratch);
    ~TxImpl() override;

    [[nodiscard]] auto status() const -> Status override
    {
        return *m_status;
    }

    [[nodiscard]] auto schema() const -> Cursor & override
    {
        return *m_schema;
    }

    auto create_bucket(const BucketOptions &options, const Slice &name, Bucket *b_out) -> Status override;
    [[nodiscard]] auto open_bucket(const Slice &name, Bucket &b_out) const -> Status override;
    auto drop_bucket(const Slice &name) -> Status override;
    auto vacuum() -> Status override;
    auto commit() -> Status override;

    [[nodiscard]] auto new_cursor(const Bucket &b) const -> Cursor * override;
    [[nodiscard]] auto get(const Bucket &b, const Slice &key, std::string *value) const -> Status override;
    auto put(const Bucket &b, const Slice &key, const Slice &value) -> Status override;
    auto erase(const Bucket &b, const Slice &key) -> Status override;

    auto TEST_validate() const -> void
    {
        m_schema_obj.TEST_validate();
    }

private:
    // m_backref is not known until after the constructor runs. Let DBImpl set it.
    friend class DBImpl;

    auto vacuum_freelist() -> Status;

    mutable Schema m_schema_obj;
    TxImpl **m_backref = nullptr;
    Cursor *m_schema;
    Pager *m_pager;
    Status *m_status;
};

inline auto tx_impl(Tx *tx) -> TxImpl *
{
    return reinterpret_cast<TxImpl *>(tx);
}
inline auto tx_impl(const Tx *tx) -> const TxImpl *
{
    return reinterpret_cast<const TxImpl *>(tx);
}

} // namespace calicodb

#endif // CALICODB_TX_IMPL_H

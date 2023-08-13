// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_TX_IMPL_H
#define CALICODB_TX_IMPL_H

#include "schema.h"

namespace calicodb
{

class ErrorState;
struct Bucket;
struct BucketOptions;
struct Stat;

class TxImpl : public Tx
{
public:
    struct Parameters {
        const Status *status;
        ErrorState *errors;
        Pager *pager;
        Stat *stat;
        char *scratch;
        bool writable;
    };
    explicit TxImpl(const Parameters &param);
    ~TxImpl() override;

    [[nodiscard]] auto status() const -> Status override
    {
        return *m_status;
    }

    [[nodiscard]] auto schema() const -> Cursor & override
    {
        return m_schema.cursor();
    }

    auto create_bucket(const BucketOptions &options, const Slice &name, Cursor **c_out) -> Status override;
    auto open_bucket(const Slice &name, Cursor *&c_out) const -> Status override;
    auto drop_bucket(const Slice &name) -> Status override;
    auto vacuum() -> Status override;
    auto commit() -> Status override;

    auto get(Cursor &c, const Slice &key, std::string *value) const -> Status override;
    auto put(Cursor &c, const Slice &key, const Slice &value) -> Status override;
    auto erase(Cursor &c, const Slice &key) -> Status override;
    auto erase(Cursor &c) -> Status override;

    auto TEST_validate() const -> void
    {
        m_schema.TEST_validate();
    }

private:
    // m_backref is not known until after the constructor runs. Let DBImpl set it.
    friend class DBImpl;

    mutable Schema m_schema;
    ErrorState *const m_errors;
    const Status *const m_status;
    Pager *const m_pager;
    TxImpl **m_backref = nullptr;
    const bool m_writable;
};

} // namespace calicodb

#endif // CALICODB_TX_IMPL_H

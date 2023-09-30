// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_TX_IMPL_H
#define CALICODB_TX_IMPL_H

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
        const Status *status;
        Pager *pager;
        Stats *stat;
        bool writable;
    };
    explicit TxImpl(const Parameters &param);
    ~TxImpl() override;

    auto status() const -> Status override
    {
        return *m_status;
    }

    [[nodiscard]] auto schema() const -> Cursor & override
    {
        return *m_schema.cursor();
    }

    auto create_bucket(const BucketOptions &options, const Slice &name, Cursor **c_out) -> Status override;
    auto open_bucket(const Slice &name, Cursor *&c_out) const -> Status override;
    auto drop_bucket(const Slice &name) -> Status override;
    auto vacuum() -> Status override;
    auto commit() -> Status override;

    auto put(Cursor &c, const Slice &key, const Slice &value) -> Status override;
    auto erase(Cursor &c, const Slice &key) -> Status override;
    auto erase(Cursor &c) -> Status override;

    auto TEST_validate() const -> void
    {
        m_schema.TEST_validate();
    }

private:
    template <class Operation>
    auto run_write_operation(const Operation &operation) const -> Status
    {
        Status s;
        if (!m_writable) {
            s = Status::not_supported("transaction is readonly");
        } else if (!m_status->is_ok()) {
            s = *m_status;
        } else {
            s = operation();
            if (!s.is_ok()) {
                m_pager->set_status(s);
            }
        }
        return s;
    }

    // m_backref is not known until after the constructor runs. Let DBImpl set it.
    friend class DBImpl;

    mutable Schema m_schema;
    const Status *const m_status;
    Pager *const m_pager;
    TxImpl **m_backref = nullptr;
    const bool m_writable;
};

} // namespace calicodb

#endif // CALICODB_TX_IMPL_H

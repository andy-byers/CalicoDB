// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "model.h"
#include "utils.h"

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

auto ModelTx::save_cursor() const -> void
{
    if (m_last_c && m_last_c->is_valid() && !m_saved) {
        m_saved_key = m_last_c->key().to_string();
        m_saved_val = m_last_c->value().to_string();
        m_saved = true;
    }
}

auto ModelTx::load_cursor() const -> std::pair<bool, std::string>
{
    if (m_saved) {
        m_saved = false;
        m_last_c->seek(m_saved_key);
        return {true, m_saved_key};
    }
    return {false, ""};
}

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
    m_last_c = new ModelCursor(*this, m_temp);
    m_saved = false;
    return m_last_c;
}

auto ModelTx::put(const Bucket &, const Slice &key, const Slice &value) -> Status
{
    save_cursor();
    m_temp.insert_or_assign(key.to_string(), value.to_string());
    return Status::ok();
}

auto ModelTx::put(Cursor &c, const Slice &key, const Slice &value) -> Status
{
    m_temp.insert_or_assign(key.to_string(), value.to_string());
    c.seek(key);
    return Status::ok();
}

auto ModelTx::erase(const Bucket &, const Slice &key) -> Status
{
    save_cursor();
    m_temp.erase(key.to_string());
    return Status::ok();
}

auto ModelTx::erase(Cursor &c) -> Status
{
    if (!c.status().is_ok()) {
        return c.status();
    } else if (!c.is_valid()) {
        return Status::invalid_argument();
    }
    const auto key = c.key().to_string();
    m_temp.erase(key);
    c.seek(key);
    return Status::ok();
}

ModelCursor::~ModelCursor() = default;

} // namespace calicodb
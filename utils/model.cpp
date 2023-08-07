// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "model.h"
#include "db_impl.h"
#include "pager.h"
#include "tree.h"
#include "utils.h"

namespace calicodb
{

auto ModelDB::open(const Options &options, const std::string &filename, KVStore &store, DB *&db_out) -> Status
{
    DB *db;
    auto s = DB::open(options, filename, db);
    if (s.is_ok()) {
        db_out = new ModelDB(store, *db);
    }
    return s;
}

ModelDB::~ModelDB()
{
    delete m_db;
}

auto ModelDB::check_consistency() const -> void
{
    reinterpret_cast<const DBImpl *>(m_db)->TEST_pager().assert_state();
}

auto ModelDB::new_tx(WriteTag, Tx *&tx_out) -> Status
{
    auto s = m_db->new_tx(WriteTag(), tx_out);
    if (s.is_ok()) {
        tx_out = new ModelTx(*m_store, *tx_out);
    }
    return s;
}

auto ModelDB::new_tx(Tx *&tx_out) const -> Status
{
    auto s = m_db->new_tx(tx_out);
    if (s.is_ok()) {
        tx_out = new ModelTx(*m_store, *tx_out);
    }
    return s;
}

ModelTx::~ModelTx()
{
    delete m_tx;
}

auto ModelTx::check_consistency() const -> void
{
    for (const auto &[name, map] : m_temp) {
        Bucket b;
        CHECK_OK(m_tx->open_bucket(name, b));
        auto *c = m_tx->new_cursor(b);
        c->seek_first();
        while (c->is_valid()) {
            const auto key = c->key().to_string();
            const auto itr = map.find(key);
            CHECK_TRUE(itr != end(map));
            CHECK_EQ(itr->first, key);
            CHECK_EQ(itr->second, c->value().to_string());
            c->next();
        }
        delete c;
    }
}

auto ModelTx::save_cursors(Cursor *exclude) const -> void
{
    for (auto *c : m_cursors) {
        CHECK_TRUE(c != nullptr);
        if (c != exclude && c != &m_schema) {
            reinterpret_cast<ModelCursorBase<KVMap> *>(c)->save_position();
        }
    }
}

auto ModelTx::create_bucket(const BucketOptions &options, const Slice &name, Bucket *b_out) -> Status
{
    Bucket b;
    auto s = m_tx->create_bucket(options, name, &b);
    if (s.is_ok()) {
        // NOOP if `name` already exists.
        auto [itr, _] = m_temp.insert({name.to_string(), {}});
        b = add_model_bucket({b.state, &itr->second});
        if (b_out) {
            *b_out = b;
        }
    }
    return s;
}

auto ModelTx::open_bucket(const Slice &name, Bucket &b_out) const -> Status
{
    Bucket b;
    auto s = m_tx->open_bucket(name, b);
    if (s.is_ok()) {
        auto itr = m_temp.find(name.to_string());
        CHECK_TRUE(itr != end(m_temp));
        b_out = add_model_bucket({b.state, &itr->second});
    }
    return s;
}

auto ModelTx::new_cursor(const Bucket &b) const -> Cursor *
{
    m_cursors.emplace_front();
    auto *c = new ModelCursorBase<KVMap>(
        *m_tx->new_cursor(get_real_bucket(b)),
        *this,
        *get_fake_bucket(b),
        begin(m_cursors));
    m_cursors.front() = c;
    return c;
}

auto ModelTx::put(const Bucket &b, const Slice &key, const Slice &value) -> Status
{
    save_cursors();
    get_fake_bucket(b)->insert_or_assign(key.to_string(), value.to_string());
    return m_tx->put(get_real_bucket(b), key, value);
}

auto ModelTx::put(Cursor &c, const Slice &key, const Slice &value) -> Status
{
    auto &mc = reinterpret_cast<ModelCursorBase<KVMap> &>(c);
    save_cursors(&mc);
    mc.load_position();
    mc.m_itr = mc.map().insert_or_assign(mc.m_itr, key.to_string(), value.to_string());
    mc.m_saved = false;
    auto s = m_tx->put(*mc.m_c, key, value);
    if (s.is_ok()) {
        mc.check_record();
    }
    return s;
}

auto ModelTx::erase(const Bucket &b, const Slice &key) -> Status
{
    save_cursors();
    get_fake_bucket(b)->erase(key.to_string());
    return m_tx->erase(get_real_bucket(b), key);
}

auto ModelTx::erase(Cursor &c) -> Status
{
    auto &mc = reinterpret_cast<ModelCursorBase<KVMap> &>(c);
    save_cursors(&mc);
    mc.load_position();
    auto s = m_tx->erase(*mc.m_c);
    if (s.is_ok()) {
        mc.m_itr = mc.map().erase(mc.m_itr);
        mc.m_saved = false;
        mc.check_record();
    }
    return s;
}

} // namespace calicodb
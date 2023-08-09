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
        Cursor *c;
        CHECK_OK(m_tx->open_bucket(name, c));
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

auto ModelTx::create_bucket(const BucketOptions &options, const Slice &name, Cursor **c_out) -> Status
{
    Cursor *c = nullptr;
    auto **c_ptr = c_out ? &c : nullptr;
    auto s = m_tx->create_bucket(options, name, c_ptr);
    if (s.is_ok()) {
        // NOOP if `name` already exists.
        auto [itr, _] = m_temp.insert({name.to_string(), {}});
        if (c_out) {
            CHECK_TRUE(c != nullptr);
            *c_out = open_model_cursor(*c, itr->second);
            ;
        }
    }
    return s;
}

auto ModelTx::open_bucket(const Slice &name, Cursor *&c_out) const -> Status
{
    Cursor *c;
    auto s = m_tx->open_bucket(name, c);
    if (s.is_ok()) {
        auto itr = m_temp.find(name.to_string());
        CHECK_TRUE(itr != end(m_temp));
        c_out = open_model_cursor(*c, itr->second);
    }
    return s;
}

auto ModelTx::open_model_cursor(Cursor &c, KVMap &map) const -> Cursor *
{
    m_cursors.emplace_front();
    auto *m = new ModelCursorBase<KVMap>(
        c,
        *this,
        map,
        begin(m_cursors));
    m_cursors.front() = m;
    return m;
}

auto ModelTx::put(Cursor &c, const Slice &key, const Slice &value) -> Status
{
    auto &m = use_cursor<KVMap>(c);
    auto s = m_tx->put(c, key, value);
    if (s.is_ok()) {
        m.m_itr = m.m_map->insert_or_assign(m.m_itr, key.to_string(), value.to_string());
    } else {
        m.m_itr = end(*m.m_map);
    }
    return s;
}

auto ModelTx::erase(Cursor &c, const Slice &key) -> Status
{
    auto &m = use_cursor<KVMap>(c);
    auto s = m_tx->erase(c, key);
    if (s.is_ok()) {
        m.m_itr = m.m_map->lower_bound(key.to_string());
        if (m.m_itr != end(*m.m_map) && m.m_itr->first == key) {
            m.m_itr = m.m_map->erase(m.m_itr);
        }
    }
    return s;
}

auto ModelTx::erase(Cursor &c) -> Status
{
    auto &m = use_cursor<KVMap>(c);
    auto s = m_tx->erase(m);
    if (s.is_ok()) {
        m.m_itr = m.m_map->erase(m.m_itr);
        m.check_record();
    } else {
        m.m_itr = end(*m.m_map);
    }
    return s;
}

} // namespace calicodb
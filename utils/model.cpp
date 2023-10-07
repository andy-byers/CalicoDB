// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "model.h"
#include "db_impl.h"
#include "internal.h"
#include "pager.h"
#include "tree.h"

namespace calicodb
{

auto ModelDB::open(const Options &options, const char *filename, KVStore &store, DB *&db_out) -> Status
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

auto ModelDB::new_writer(Tx *&tx_out) -> Status
{
    auto s = m_db->new_writer(tx_out);
    if (s.is_ok()) {
        tx_out = new ModelTx(*m_store, *tx_out);
    }
    return s;
}

auto ModelDB::new_reader(Tx *&tx_out) const -> Status
{
    auto s = m_db->new_reader(tx_out);
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
        CHECK_OK(m_tx->open_bucket(name.c_str(), c));
        auto copy = map;
        c->seek_first();
        while (c->is_valid()) {
            const auto key = to_string(c->key());
            const auto itr = map.find(key);
            CHECK_TRUE(itr != end(map));
            CHECK_EQ(itr->first, key);
            CHECK_EQ(itr->second, to_string(c->value()));
            copy.erase(key);
            c->next();
        }
        CHECK_TRUE(copy.empty());
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
    auto s = m_tx->create_bucket(options, name, c_out);
    if (s.is_ok()) {
        // NOOP if `name` already exists.
        m_schema.move_to(m_temp.insert(m_schema.m_itr, {to_string(name), {}}));
        if (c_out) {
            CHECK_TRUE(*c_out != nullptr);
            *c_out = open_model_cursor(**c_out, m_schema.m_itr->second);
        }
    } else if (c_out) {
        CHECK_EQ(*c_out, nullptr);
    }
    return s;
}

auto ModelTx::open_bucket(const Slice &name, Cursor *&c_out) const -> Status
{
    auto s = m_tx->open_bucket(name, c_out);
    if (s.is_ok()) {
        m_schema.move_to(m_temp.find(to_string(name)));
        CHECK_TRUE(m_schema.m_itr != end(m_temp));
        c_out = open_model_cursor(*c_out, m_schema.m_itr->second);
    } else {
        CHECK_EQ(c_out, nullptr);
    }
    return s;
}

auto ModelTx::open_model_cursor(Cursor &c, KVMap &map) const -> Cursor *
{
    m_cursors.emplace_front();
    m_cursors.front() = new ModelCursorBase<KVMap>(
        c, *this, map, begin(m_cursors));
    return m_cursors.front();
}

auto ModelTx::put(Cursor &c, const Slice &key, const Slice &value) -> Status
{
    const auto key_copy = to_string(key);
    const auto value_copy = to_string(value);
    auto &m = use_cursor<KVMap>(c);
    auto s = m_tx->put(c, key, value);
    if (s.is_ok()) {
        m.m_itr = m.m_map->insert_or_assign(m.m_itr, key_copy, value_copy);
    } else {
        m.m_itr = end(*m.m_map);
    }
    return s;
}

auto ModelTx::erase(Cursor &c, const Slice &key) -> Status
{
    const auto key_copy = to_string(key);
    auto &m = use_cursor<KVMap>(c);
    auto s = m_tx->erase(c, key);
    if (s.is_ok()) {
        m.m_itr = m.m_map->lower_bound(key_copy);
        if (m.m_itr != end(*m.m_map) && m.m_itr->first == key_copy) {
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
// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "model.h"
#include "db_impl.h"
#include "internal.h"
#include <set>

namespace calicodb
{

auto ModelDB::open(const Options &options, const char *filename, ModelStore &store, DB *&db_out) -> Status
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
    // Only buckets are allowed on the first level.
    for (const auto &[name, subtree_or_value] : m_temp.tree) {
        CHECK_TRUE(std::holds_alternative<ModelStore>(subtree_or_value));
        auto &subtree = std::get<ModelStore>(subtree_or_value).tree;

        TestBucket b;
        CHECK_OK(test_open_bucket(*m_tx, name, b));
        check_consistency(subtree, *b);
    }
}

auto ModelTx::check_consistency(const ModelStore::Tree &tree, const Bucket &bucket) const -> void
{
    std::set<std::string> copy_keys;
    std::transform(begin(tree), end(tree), inserter(copy_keys, begin(copy_keys)), [](auto &entry) {
        return entry.first;
    });

    for (const auto &[key, subtree_or_value] : tree) {
        copy_keys.erase(key);
        if (std::holds_alternative<std::string>(subtree_or_value)) {
            std::string value;
            CHECK_OK(bucket.get(key, &value));
            CHECK_EQ(value, std::get<std::string>(subtree_or_value));
            continue;
        }
        TestBucket subbucket;
        CHECK_OK(test_open_bucket(bucket, key, subbucket));
        auto &subtree = std::get<ModelStore>(subtree_or_value).tree;
        check_consistency(subtree, *subbucket);
    }

    CHECK_TRUE(copy_keys.empty());
}

auto ModelTx::save_cursors(Cursor *exclude) const -> void
{
    for (auto &b : m_buckets) {
        b->save_cursors(exclude);
    }
}

auto ModelTx::create_bucket(const Slice &name, Bucket **b_out) -> Status
{
    auto name_copy = name.to_string();
    auto s = m_tx->create_bucket(name, b_out);
    if (s.is_ok()) {
        // NOOP if `name` already exists.
        auto [itr, _] = m_temp.tree.insert({name_copy, ModelStore()});
        if (b_out) {
            CHECK_TRUE(*b_out != nullptr);
            *b_out = open_model_bucket(std::move(name_copy), **b_out,
                                       std::get<ModelStore>(itr->second));
        }
    } else if (b_out) {
        CHECK_EQ(*b_out, nullptr);
    }
    return s;
}

auto ModelTx::create_bucket_if_missing(const Slice &name, Bucket **b_out) -> Status
{
    auto name_copy = name.to_string();
    auto s = m_tx->create_bucket_if_missing(name, b_out);
    if (s.is_ok()) {
        auto [itr, _] = m_temp.tree.insert({name_copy, ModelStore()});
        if (b_out) {
            CHECK_TRUE(*b_out != nullptr);
            *b_out = open_model_bucket(std::move(name_copy), **b_out,
                                       std::get<ModelStore>(itr->second));
        }
    } else if (b_out) {
        CHECK_EQ(*b_out, nullptr);
    }
    return s;
}

auto ModelTx::open_bucket(const Slice &name, Bucket *&b_out) const -> Status
{
    auto name_copy = name.to_string();
    auto s = m_tx->open_bucket(name, b_out);
    if (s.is_ok()) {
        auto itr = m_temp.tree.find(name_copy);
        CHECK_TRUE(itr != end(m_temp.tree));
        CHECK_TRUE(std::holds_alternative<ModelStore>(itr->second));
        b_out = open_model_bucket(std::move(name_copy), *b_out,
                                  std::get<ModelStore>(itr->second));
    } else {
        CHECK_EQ(b_out, nullptr);
    }
    return s;
}

auto ModelTx::drop_bucket(const Slice &name) -> Status
{
    const auto name_copy = name.to_string();
    auto s = m_tx->drop_bucket(name);
    if (s.is_ok()) {
        CHECK_EQ(1, m_temp.tree.erase(name_copy));
    }
    return s;
}

auto ModelTx::open_model_bucket(std::string name, Bucket &b, ModelStore &store) const -> Bucket *
{
    m_buckets.emplace_front();
    m_buckets.front() = new ModelBucket(
        std::move(name), store, b, m_buckets, begin(m_buckets));
    return m_buckets.front();
}

ModelBucket::~ModelBucket()
{
    if (m_live) {
        m_parent_buckets->erase(m_backref);
    }
    delete m_b;
}

auto ModelBucket::open_model_bucket(std::string name, Bucket &b, ModelStore &store) const -> Bucket *
{
    m_child_buckets.emplace_front();
    m_child_buckets.front() = new ModelBucket(
        std::move(name), store, b, m_child_buckets, begin(m_child_buckets));
    return m_child_buckets.front();
}

auto ModelBucket::open_model_cursor(Cursor &c, ModelStore::Tree &tree) const -> Cursor *
{
    m_cursors.emplace_front();
    m_cursors.front() = new ModelCursor(
        c, *this, tree, begin(m_cursors));
    return m_cursors.front();
}

auto ModelBucket::deactivate() -> void
{
    m_live = false;
    for (auto *c : m_cursors) {
        c->m_live = false;
    }
    for (auto *b : m_child_buckets) {
        b->deactivate();
    }
}

auto ModelBucket::new_cursor() const -> Cursor *
{
    if (auto *c = m_b->new_cursor()) {
        return open_model_cursor(*c, *m_temp);
    }
    return nullptr;
}

auto ModelBucket::create_bucket(const Slice &name, Bucket **b_out) -> Status
{
    auto name_copy = name.to_string();
    auto s = m_b->create_bucket(name, b_out);
    if (s.is_ok()) {
        // NOOP if `name` already exists.
        auto [itr, _] = m_temp->insert({name_copy, ModelStore()});
        if (b_out) {
            CHECK_TRUE(*b_out != nullptr);
            CHECK_TRUE(std::holds_alternative<ModelStore>(itr->second));
            *b_out = open_model_bucket(std::move(name_copy), **b_out,
                                       std::get<ModelStore>(itr->second));
        }
    } else if (b_out) {
        CHECK_EQ(*b_out, nullptr);
    }
    return s;
}

auto ModelBucket::create_bucket_if_missing(const Slice &name, Bucket **b_out) -> Status
{
    auto name_copy = name.to_string();
    auto s = m_b->create_bucket_if_missing(name, b_out);
    if (s.is_ok()) {
        auto [itr, _] = m_temp->insert({name_copy, ModelStore()});
        if (b_out) {
            CHECK_TRUE(*b_out != nullptr);
            *b_out = open_model_bucket(std::move(name_copy), **b_out,
                                       std::get<ModelStore>(itr->second));
        }
    } else if (b_out) {
        CHECK_EQ(*b_out, nullptr);
    }
    return s;
}

auto ModelBucket::drop_bucket(const Slice &name) -> Status
{
    const auto name_copy = name.to_string();
    auto s = m_b->drop_bucket(name);
    if (s.is_ok()) {
        for (auto &b : m_child_buckets) {
            b->deactivate();
        }
        CHECK_EQ(1, m_temp->erase(name_copy));
    }
    return s;
}

auto ModelBucket::open_bucket(const Slice &name, Bucket *&b_out) const -> Status
{
    auto name_copy = name.to_string();
    auto s = m_b->open_bucket(name, b_out);
    if (s.is_ok()) {
        auto itr = m_temp->find(name.to_string());
        CHECK_TRUE(itr != end(*m_temp));
        CHECK_TRUE(std::holds_alternative<ModelStore>(itr->second));
        b_out = open_model_bucket(std::move(name_copy), *b_out,
                                  std::get<ModelStore>(itr->second));
    } else {
        CHECK_EQ(b_out, nullptr);
    }
    return s;
}

auto ModelBucket::get(const Slice &key, CALICODB_STRING *value_out) const -> Status
{
    const auto key_copy = key.to_string();
    auto s = m_b->get(key, value_out);
    if (s.is_ok()) {
        const auto itr = m_temp->find(key_copy);
        CHECK_TRUE(itr != end(*m_temp));
        CHECK_EQ(itr->first, key_copy);
        CHECK_TRUE(std::holds_alternative<std::string>(itr->second));
        CHECK_EQ(std::get<std::string>(itr->second), *value_out);
    }
    return s;
}

auto ModelBucket::put(const Slice &key, const Slice &value) -> Status
{
    const auto key_copy = key.to_string();
    const auto value_copy = value.to_string();
    auto s = m_b->put(key, value);
    if (s.is_ok()) {
        m_temp->insert_or_assign(key_copy, value_copy);
    }
    return s;
}

auto ModelBucket::put(Cursor &c, const Slice &value) -> Status
{
    const auto key_copy = c.key().to_string();
    const auto value_copy = value.to_string();
    auto &m = use_cursor(c);
    auto s = m_b->put(c, value);
    if (s.is_ok()) {
        m.move_to(m_temp->insert_or_assign(m.m_itr, key_copy, value_copy));
    } else {
        m.invalidate();
    }
    return s;
}

auto ModelBucket::erase(const Slice &key) -> Status
{
    const auto key_copy = to_string(key);
    auto s = m_b->erase(key);
    if (s.is_ok()) {
        CHECK_EQ(m_temp->erase(key_copy), 1);
    }
    return s;
}

auto ModelBucket::erase(Cursor &c) -> Status
{
    auto &m = use_cursor(c);
    auto s = m_b->erase(c);
    if (s.is_ok()) {
        m.move_to(m_temp->erase(m.m_itr));
        m.check_record();
    } else {
        m.invalidate();
    }
    return s;
}

auto ModelBucket::save_cursors(Cursor *exclude) const -> void
{
    for (auto *c : m_cursors) {
        if (c != exclude) {
            c->save_position();
        }
    }
}

ModelCursor::~ModelCursor()
{
    if (m_live) {
        m_b->m_cursors.erase(m_backref);
    }
    delete m_c;
}

} // namespace calicodb
// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.
//
// This file contains classes that model the intended behavior of higher-level
// CalicoDB components. Note that these classes don't attempt to catch certain
// types of API misuse (for example, ModelBucket will write to a bucket in a read-
// only transaction without complaint).

#ifndef CALICODB_UTILS_MODEL_H
#define CALICODB_UTILS_MODEL_H

#include "calicodb/cursor.h"
#include "calicodb/db.h"
#include "common.h"
#include "logging.h"
#include <iostream>
#include <list>
#include <map>

#define CHECK_TRUE(cond)                                 \
    do {                                                 \
        if (!(cond)) {                                   \
            std::cerr << "expected `" << #cond << "`\n"; \
            std::abort();                                \
        }                                                \
    } while (0)

#define CHECK_FALSE(cond) \
    CHECK_TRUE(!(cond))

#define CHECK_OK(expr)                                                 \
    do {                                                               \
        if (auto assert_s = (expr); !assert_s.is_ok()) {               \
            std::fprintf(                                              \
                stderr,                                                \
                "expected `(" #expr ").is_ok()` but got \"%s: %s\"\n", \
                assert_s.type_name(), assert_s.message());             \
            std::abort();                                              \
        }                                                              \
    } while (0)

#define CHECK_EQ(lhs, rhs)                                                                             \
    do {                                                                                               \
        if ((lhs) != (rhs)) {                                                                          \
            std::cerr << "expected `" << #lhs "` (" << (lhs) << ") == `" #rhs "` (" << (rhs) << ")\n"; \
            std::abort();                                                                              \
        }                                                                                              \
    } while (0)

namespace calicodb
{

using KVMap = std::map<std::string, std::string>;
using KVStore = std::map<std::string, KVMap>;

class ModelDB : public DB
{
    KVStore *const m_store;
    DB *const m_db;

public:
    static auto open(const Options &options, const char *filename, KVStore &store, DB *&db_out) -> Status;

    explicit ModelDB(KVStore &store, DB &db)
        : m_store(&store),
          m_db(&db)
    {
    }

    ~ModelDB() override;

    auto check_consistency() const -> void;

    auto get_property(const Slice &name, Slice *value_out) const -> bool override
    {
        return m_db->get_property(name, value_out);
    }

    auto new_tx(WriteTag, Tx *&tx_out) -> Status override;
    [[nodiscard]] auto new_tx(Tx *&tx_out) const -> Status override;

    auto checkpoint(bool reset) -> Status override
    {
        return m_db->checkpoint(reset);
    }
};

class ModelTx;

template <class Map>
class ModelCursorBase : public Cursor
{
    static constexpr bool kIsSchema = std::is_same_v<Map, KVStore>;

    friend class ModelTx;

    std::list<Cursor *>::iterator m_backref;
    Cursor *const m_c;
    const ModelTx *const m_tx;

    mutable typename Map::const_iterator m_itr;
    Map *m_map;

    mutable std::string m_saved_key;
    mutable std::string m_saved_val;
    mutable bool m_saved;

    auto save_position() const -> void
    {
        if (!m_saved && m_c->is_valid()) {
            m_saved_key = m_c->key().to_string();
            m_saved_val = m_c->value().to_string();
            // The element at m_itr may have been erased. This will cause m_itr to be
            // invalidated, but we won't be able to tell, since it probably won't equal
            // end(*m_map). This makes sure the iterator can still be used as a hint in
            // ModelTx::put().
            m_itr = end(*m_map);
            m_saved = true;
        }
    }

    auto load_position() const -> std::pair<bool, std::string>
    {
        if (m_saved) {
            m_saved = false;
            m_itr = m_map->find(m_saved_key);
            return {true, m_saved_key};
        }
        return {false, ""};
    }

public:
    explicit ModelCursorBase(Cursor &c, const ModelTx &tx, Map &map, std::list<Cursor *>::iterator backref)
        : m_backref(backref),
          m_c(&c),
          m_tx(&tx),
          m_itr(end(map)),
          m_map(&map),
          m_saved(false)
    {
    }

    ~ModelCursorBase() override;

    [[nodiscard]] auto map() -> Map &
    {
        return *m_map;
    }

    [[nodiscard]] auto handle() -> void * override
    {
        return m_c->handle();
    }

    [[nodiscard]] auto is_valid() const -> bool override
    {
        if (m_c->status().is_ok()) {
            CHECK_EQ(m_c->is_valid(),
                     m_itr != end(*m_map) || m_saved);
            check_record();
        }
        return m_c->is_valid();
    }

    [[nodiscard]] auto status() const -> Status override
    {
        return m_c->status();
    }

    auto check_record() const -> void
    {
        if (m_c->is_valid()) {
            const auto key = m_saved
                                 ? m_saved_key
                                 : m_itr->first;
            CHECK_EQ(key, m_c->key().to_string());

            std::string value;
            if constexpr (!kIsSchema) {
                value = m_saved ? m_saved_val : m_itr->second;
            }
            CHECK_EQ(value, m_c->value().to_string());
        }
    }

    [[nodiscard]] auto key() const -> Slice override
    {
        return m_c->key();
    }

    [[nodiscard]] auto value() const -> Slice override
    {
        return m_c->value();
    }

    auto find(const Slice &key) -> void override
    {
        m_saved = false;
        m_itr = m_map->find(key.to_string());
        m_c->find(key);
    }

    auto seek(const Slice &key) -> void override
    {
        m_saved = false;
        m_itr = m_map->lower_bound(key.to_string());
        m_c->seek(key);
    }

    auto seek_first() -> void override
    {
        m_saved = false;
        m_itr = begin(*m_map);
        m_c->seek_first();
    }

    auto seek_last() -> void override
    {
        m_saved = false;
        m_itr = end(*m_map);
        if (!m_map->empty()) {
            --m_itr;
        }
        m_c->seek_last();
    }

    auto next() -> void override
    {
        load_position();
        if (m_itr != end(*m_map)) {
            ++m_itr;
        }
        m_c->next();
    }

    auto previous() -> void override
    {
        load_position();
        if (m_itr == begin(*m_map)) {
            m_itr = end(*m_map);
        } else {
            --m_itr;
        }
        m_c->previous();
    }
};

template class ModelCursorBase<KVMap>;

class ModelTx : public Tx
{
    friend class ModelCursorBase<KVMap>;
    friend class ModelCursorBase<KVStore>;

    mutable KVStore m_temp;
    KVStore *const m_base;
    Tx *const m_tx;

    template <class Target>
    auto use_cursor(Cursor &c) const -> ModelCursorBase<Target> &
    {
        auto &m = reinterpret_cast<ModelCursorBase<Target> &>(c);
        save_cursors(&m);
        m.load_position();
        return m;
    }

    auto open_model_cursor(Cursor &c, KVMap &map) const -> Cursor *;
    auto save_cursors(Cursor *exclude = nullptr) const -> void;
    mutable std::list<Cursor *> m_cursors;
    mutable ModelCursorBase<KVStore> m_schema;

public:
    explicit ModelTx(KVStore &store, Tx &tx)
        : m_temp(store),
          m_base(&store),
          m_tx(&tx),
          m_schema(tx.schema(), *this, m_temp, end(m_cursors))
    {
        m_cursors.emplace_front(&m_schema);
        m_schema.m_backref = begin(m_cursors);
    }

    ~ModelTx() override;

    // WARNING: Invalidates all open cursors.
    auto check_consistency() const -> void;

    [[nodiscard]] auto status() const -> Status override
    {
        return m_tx->status();
    }

    [[nodiscard]] auto schema() const -> Cursor & override
    {
        return m_schema;
    }

    auto create_bucket(const BucketOptions &options, const Slice &name, Cursor **c_out) -> Status override;
    [[nodiscard]] auto open_bucket(const Slice &name, Cursor *&c_out) const -> Status override;

    auto drop_bucket(const Slice &name) -> Status override
    {
        auto s = m_tx->drop_bucket(name);
        if (s.is_ok()) {
            const auto num_erased = m_temp.erase(name.to_string());
            CHECK_EQ(s.is_ok(), num_erased);
        }
        return s;
    }

    auto vacuum() -> Status override
    {
        save_cursors();
        return m_tx->vacuum();
    }

    auto commit() -> Status override
    {
        auto s = m_tx->commit();
        if (s.is_ok()) {
            *m_base = m_temp;
        }
        return s;
    }

    auto put(Cursor &c, const Slice &key, const Slice &value) -> Status override;
    auto erase(Cursor &c, const Slice &key) -> Status override;
    auto erase(Cursor &c) -> Status override;
};

template <class Map>
ModelCursorBase<Map>::~ModelCursorBase()
{
    if constexpr (!kIsSchema) {
        m_tx->m_cursors.erase(m_backref);
        delete m_c;
    }
}

} // namespace calicodb

#endif // CALICODB_UTILS_MODEL_H

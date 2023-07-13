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
#include <map>

namespace calicodb
{

using KVMap = std::map<std::string, std::string>;
class ModelCursor;

class ModelDB : public DB
{
    KVMap *m_store;
    bool m_owns_store;

public:
    explicit ModelDB(KVMap *store)
        : m_store(store)
    {
        if (m_store) {
            m_owns_store = false;
        } else {
            m_store = new KVMap;
            m_owns_store = true;
        }
    }

    ~ModelDB() override;

    auto get_property(const Slice &, std::string *) const -> bool override
    {
        return false;
    }

    auto new_tx(WriteTag, Tx *&tx_out) -> Status override;
    [[nodiscard]] auto new_tx(const Tx *&tx_out) const -> Status override;

    auto checkpoint(bool) -> Status override
    {
        return Status::ok();
    }
};

class ModelTx : public Tx
{
    friend class ModelCursor;

    KVMap m_temp;
    KVMap *m_base;
    Cursor *m_schema;

    auto save_cursor() const -> void;
    auto load_cursor() const -> std::pair<bool, std::string>;
    mutable ModelCursor *m_last_c = nullptr;
    mutable std::string m_saved_key;
    mutable std::string m_saved_val;
    mutable bool m_saved = false;

public:
    explicit ModelTx(KVMap &base)
        : m_temp(base),
          m_base(&base)
    {
    }

    ~ModelTx() override;

    [[nodiscard]] auto status() const -> Status override
    {
        return Status::ok();
    }

    [[nodiscard]] auto schema() const -> Cursor & override
    {
        return *m_schema; // TODO
    }

    auto create_bucket(const BucketOptions &options, const Slice &name, Bucket *tb_out) -> Status override;
    [[nodiscard]] auto open_bucket(const Slice &name, Bucket &tb_out) const -> Status override;

    auto drop_bucket(const Slice &) -> Status override
    {
        return Status::ok();
    }

    auto vacuum() -> Status override
    {
        save_cursor();
        return Status::ok();
    }

    auto commit() -> Status override
    {
        *m_base = m_temp;
        return Status::ok();
    }

    [[nodiscard]] auto new_cursor(const Bucket &) const -> Cursor * override;

    [[nodiscard]] auto get(const Bucket &, const Slice &key, std::string *value) const -> Status override
    {
        Status s;
        const auto itr = m_temp.find(key.to_string());
        if (itr == end(m_temp)) {
            s = Status::not_found();
        }
        if (value) {
            *value = s.is_ok() ? itr->second : "";
        }
        return s;
    }

    auto put(const Bucket &, const Slice &key, const Slice &value) -> Status override;
    auto put(Cursor &c, const Slice &key, const Slice &value) -> Status override;
    auto erase(const Bucket &, const Slice &key) -> Status override;
    auto erase(Cursor &c) -> Status override;
};

class ModelCursor : public Cursor
{
    const ModelTx *const m_tx;
    KVMap::const_iterator m_itr;
    const KVMap *m_map;

public:
    explicit ModelCursor(const ModelTx &tx, const KVMap &map)
        : m_tx(&tx),
          m_itr(end(map)),
          m_map(&map)
    {
    }

    ~ModelCursor() override;

    [[nodiscard]] auto token() -> void * override
    {
        return nullptr;
    }

    [[nodiscard]] auto is_valid() const -> bool override
    {
        return m_itr != end(*m_map) || m_tx->m_saved;
    }

    [[nodiscard]] auto status() const -> Status override
    {
        return Status::ok();
    }

    [[nodiscard]] auto key() const -> Slice override
    {
        return m_tx->m_saved ? m_tx->m_saved_key : m_itr->first;
    }

    [[nodiscard]] auto value() const -> Slice override
    {
        return m_tx->m_saved ? m_tx->m_saved_val : m_itr->second;
    }

    auto seek(const Slice &key) -> void override
    {
        m_tx->m_saved = false;
        m_itr = m_map->lower_bound(key.to_string());
    }

    auto seek_first() -> void override
    {
        m_tx->m_saved = false;
        m_itr = begin(*m_map);
    }

    auto seek_last() -> void override
    {
        m_tx->m_saved = false;
        m_itr = end(*m_map);
        if (!m_map->empty()) {
            --m_itr;
        }
    }

    auto next() -> void override
    {
        m_tx->load_cursor();
        if (m_itr != end(*m_map)) {
            ++m_itr;
        }
    }

    auto previous() -> void override
    {
        m_tx->load_cursor();
        if (m_itr == begin(*m_map)) {
            m_itr = end(*m_map);
        } else {
            --m_itr;
        }
    }
};

} // namespace calicodb

#endif // CALICODB_UTILS_MODEL_H

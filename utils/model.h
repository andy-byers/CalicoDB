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
#include <map>

namespace calicodb
{

using KVMap = std::map<std::string, std::string>;

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

    [[nodiscard]] auto new_tx(WriteTag, Tx *&tx_out) -> Status override;
    [[nodiscard]] auto new_tx(const Tx *&tx_out) const -> Status override;

    [[nodiscard]] auto checkpoint(bool) -> Status override
    {
        return Status::ok();
    }
};

class ModelTx : public Tx
{
    KVMap m_temp;
    KVMap *m_base;
    Cursor *m_schema;

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

    [[nodiscard]] auto create_bucket(const BucketOptions &options, const Slice &name, Bucket *tb_out) -> Status override;
    [[nodiscard]] auto open_bucket(const Slice &name, Bucket &tb_out) const -> Status override;

    [[nodiscard]] auto drop_bucket(const Slice &) -> Status override
    {
        return Status::ok();
    }

    [[nodiscard]] auto vacuum() -> Status override
    {
        return Status::ok();
    }

    [[nodiscard]] auto commit() -> Status override
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

    [[nodiscard]] auto put(const Bucket &, const Slice &key, const Slice &value) -> Status override
    {
        if (key.is_empty()) {
            return Status::invalid_argument();
        }
        m_temp.insert_or_assign(key.to_string(), value.to_string());
        return Status::ok();
    }

    [[nodiscard]] auto erase(const Bucket &, const Slice &key) -> Status override
    {
        m_temp.erase(key.to_string());
        return Status::ok();
    }
};

class ModelCursor : public Cursor
{
    KVMap::const_iterator m_itr;
    const KVMap *m_map;

public:
    explicit ModelCursor(const KVMap &map)
        : m_itr(end(map)),
          m_map(&map)
    {
    }

    ~ModelCursor() override;

    [[nodiscard]] auto is_valid() const -> bool override
    {
        return m_itr != end(*m_map);
    }

    [[nodiscard]] auto status() const -> Status override
    {
        return Status::ok();
    }

    [[nodiscard]] auto key() const -> Slice override
    {
        return m_itr->first;
    }

    [[nodiscard]] auto value() const -> Slice override
    {
        return m_itr->second;
    }

    auto seek(const Slice &key) -> void override
    {
        m_itr = m_map->lower_bound(key.to_string());
    }

    auto seek_first() -> void override
    {
        m_itr = begin(*m_map);
    }

    auto seek_last() -> void override
    {
        m_itr = end(*m_map);
        if (!m_map->empty()) {
            --m_itr;
        }
    }

    auto next() -> void override
    {
        if (m_itr != end(*m_map)) {
            ++m_itr;
        }
    }

    auto previous() -> void override
    {
        if (m_itr == begin(*m_map)) {
            m_itr = end(*m_map);
        } else {
            --m_itr;
        }
    }
};

} // namespace calicodb

#endif // CALICODB_UTILS_MODEL_H

// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.
//
// This file contains classes that model the intended behavior of higher-level
// CalicoDB components. Note that these classes don't attempt to catch certain
// types of API misuse (for example, ModelTable will write to a table in a read-
// only transaction without complaint).

#ifndef CALICODB_TEST_TOOLS_MODEL_H
#define CALICODB_TEST_TOOLS_MODEL_H

#include "calicodb/cursor.h"
#include "calicodb/db.h"
#include <map>

namespace calicodb::tools
{

using KVMap = std::map<std::string, std::string>;
using KVStore = std::map<std::string, KVMap>;

class ModelDB : public DB
{
    KVStore *m_store;
    bool m_owns_store;

public:
    explicit ModelDB(KVStore *store)
        : m_store(store)
    {
        if (m_store) {
            m_owns_store = false;
        } else {
            m_store = new KVStore;
            m_owns_store = true;
        }
    }

    ~ModelDB() override;

    auto get_property(const Slice &, std::string *) const -> bool override
    {
        return false;
    }

    [[nodiscard]] auto new_txn(bool write, Txn *&out) -> Status override;

    [[nodiscard]] auto checkpoint(bool) -> Status override
    {
        return Status::ok();
    }
};

class ModelTxn : public Txn
{
    KVStore *m_base;
    KVStore m_temp;
    Cursor *m_schema; // TODO

public:
    explicit ModelTxn(KVStore &base)
        : m_base(&base),
          m_temp(*m_base)
    {
    }

    ~ModelTxn() override;

    [[nodiscard]] auto status() const -> Status override
    {
        return Status::ok();
    }

    [[nodiscard]] auto schema() const -> Cursor & override
    {
        return *m_schema; // TODO
    }

    [[nodiscard]] auto new_table(const TableOptions &options, const Slice &name, Table *&out) -> Status override;

    [[nodiscard]] auto drop_table(const Slice &name) -> Status override
    {
        // Table `name` should be closed.
        m_temp.erase(name.to_string());
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
};

class ModelTable : public Table
{
    KVMap *m_map;

public:
    explicit ModelTable(KVMap &map)
        : m_map(&map)
    {
    }

    ~ModelTable() override;

    [[nodiscard]] auto new_cursor() const -> Cursor * override;

    [[nodiscard]] auto get(const Slice &key, std::string *value) const -> Status override
    {
        Status s;
        const auto itr = m_map->find(key.to_string());
        if (itr == end(*m_map)) {
            s = Status::not_found();
        }
        if (value) {
            *value = s.is_ok() ? itr->second : "";
        }
        return s;
    }

    [[nodiscard]] auto put(const Slice &key, const Slice &value) -> Status override
    {
        m_map->insert_or_assign(key.to_string(), value.to_string());
        return Status::ok();
    }

    [[nodiscard]] auto erase(const Slice &key) -> Status override
    {
        m_map->erase(key.to_string());
        return Status::ok();
    }
};

class ModelCursor : public Cursor
{
    KVMap::const_iterator m_itr;
    KVMap *m_map;

public:
    explicit ModelCursor(KVMap &map)
        : m_map(&map),
          m_itr(end(map))
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

} // namespace calicodb::tools

#endif // CALICODB_TEST_TOOLS_MODEL_H

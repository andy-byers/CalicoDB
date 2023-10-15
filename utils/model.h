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
#include "cursor_impl.h"
#include "logging.h"
#include <iostream>
#include <list>
#include <map>
#include <memory>
#include <variant>

#define CHECK_TRUE(cond)                                 \
    do {                                                 \
        if (!(cond)) {                                   \
            std::cerr << "expected `" << #cond << "`\n"; \
            std::abort();                                \
        }                                                \
    } while (0)

#define CHECK_FALSE(cond) \
    CHECK_TRUE(!(cond))

#define CHECK_OK(expr)                                             \
    do {                                                           \
        if (auto assert_s = (expr); !assert_s.is_ok()) {           \
            std::fprintf(                                          \
                stderr,                                            \
                "expected `(" #expr ").is_ok()` but got \"%s\"\n", \
                assert_s.message());                               \
            std::abort();                                          \
        }                                                          \
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

class ModelBucket;
class ModelTx;

struct ModelStore {
    using Node = std::variant<ModelStore, std::string>;
    using Tree = std::map<std::string, Node>;
    Tree tree;
};

class ModelDB : public DB
{
    ModelStore *const m_store;
    DB *const m_db;

public:
    static auto open(const Options &options, const char *filename, ModelStore &store, DB *&db_out) -> Status;

    explicit ModelDB(ModelStore &store, DB &db)
        : m_store(&store),
          m_db(&db)
    {
    }

    ~ModelDB() override;

    auto check_consistency() const -> void;

    auto get_property(const Slice &name, void *value_out) const -> Status override
    {
        return m_db->get_property(name, value_out);
    }

    auto new_writer(Tx *&tx_out) -> Status override;
    auto new_reader(Tx *&tx_out) const -> Status override;

    auto checkpoint(CheckpointMode mode, CheckpointInfo *info_out) -> Status override
    {
        return m_db->checkpoint(mode, info_out);
    }
};

class ModelCursor : public Cursor
{
    friend class ModelBucket;

    std::list<ModelCursor *>::iterator m_backref;
    mutable ModelStore::Tree::iterator m_itr;
    ModelStore::Tree *m_tree;
    const ModelBucket *const m_b;
    Cursor *const m_c;

    mutable std::string m_saved_key;
    mutable std::string m_saved_val;
    mutable bool m_saved;
    bool m_live = true;

    auto save_position() const -> void
    {
        if (!m_saved && m_c->is_valid()) {
            m_saved_key = to_string(m_c->key());
            m_saved_val = to_string(m_c->value());
            // The element at m_itr may have been erased. This will cause m_itr to be
            // invalidated, but we won't be able to tell, since it probably won't equal
            // end(*m_tree). This makes sure the iterator can still be used as a hint in
            // ModelTx::put().
            m_itr = end(*m_tree);
            m_saved = true;
        }
    }

    auto load_position() const -> std::pair<bool, std::string>
    {
        if (m_saved) {
            m_saved = false;
            m_itr = m_tree->lower_bound(m_saved_key);
            return {true, m_saved_key};
        }
        return {false, ""};
    }

    auto move_to(typename ModelStore::Tree::iterator position) const -> void
    {
        m_saved = false;
        m_itr = position;
    }

    auto invalidate() const -> void
    {
        move_to(end(*m_tree));
    }

public:
    explicit ModelCursor(Cursor &c, const ModelBucket &b, ModelStore::Tree &tree, std::list<ModelCursor *>::iterator backref)
        : m_backref(backref),
          m_itr(end(tree)),
          m_tree(&tree),
          m_b(&b),
          m_c(&c),
          m_saved(false)
    {
    }

    ~ModelCursor() override;

    [[nodiscard]] auto tree() -> ModelStore::Tree &
    {
        return *m_tree;
    }

    [[nodiscard]] auto handle() -> void * override
    {
        return m_c->handle();
    }

    [[nodiscard]] auto is_valid() const -> bool override
    {
        if (m_c->status().is_ok()) {
            CHECK_EQ(m_c->is_valid(),
                     m_itr != end(*m_tree) || m_saved);
            check_record();
        }
        return m_c->is_valid();
    }

    [[nodiscard]] auto is_bucket() const -> bool override
    {
        return m_c->is_bucket();
    }

    [[nodiscard]] auto status() const -> Status override
    {
        return m_c->status();
    }

    auto check_record() const -> void
    {
        if (m_c->is_valid()) {
            CHECK_EQ(m_c->key().to_string(),
                     m_saved ? m_saved_key : m_itr->first);
            if (!m_c->is_bucket()) {
                CHECK_EQ(m_c->value().to_string(),
                         m_saved ? m_saved_val : std::get<std::string>(m_itr->second));
            }
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
        m_itr = m_tree->find(to_string(key));
        m_c->find(key);
    }

    auto seek(const Slice &key) -> void override
    {
        m_saved = false;
        m_itr = m_tree->lower_bound(to_string(key));
        m_c->seek(key);
    }

    auto seek_first() -> void override
    {
        m_saved = false;
        m_itr = begin(*m_tree);
        m_c->seek_first();
    }

    auto seek_last() -> void override
    {
        m_saved = false;
        m_itr = end(*m_tree);
        if (!m_tree->empty()) {
            --m_itr;
        }
        m_c->seek_last();
    }

    auto next() -> void override
    {
        load_position();
        if (m_itr != end(*m_tree)) {
            ++m_itr;
        }
        m_c->next();
    }

    auto previous() -> void override
    {
        load_position();
        if (m_itr == begin(*m_tree)) {
            m_itr = end(*m_tree);
        } else {
            --m_itr;
        }
        m_c->previous();
    }

    auto validate() const -> void
    {
        reinterpret_cast<const CursorImpl *>(m_c)->TEST_check_state();
    }
};

class ModelBucket : public Bucket
{
    friend class ModelCursor;
    friend class ModelTx;

    const std::string m_name;
    std::list<ModelBucket *>::iterator m_backref;
    std::list<ModelBucket *> *const m_parent_buckets;
    mutable std::list<ModelBucket *> m_child_buckets;
    mutable std::list<ModelCursor *> m_cursors;
    ModelStore::Tree *const m_temp;
    Bucket *const m_b;
    bool m_live = true;

    auto use_cursor(Cursor &c) const -> ModelCursor &
    {
        auto &m = reinterpret_cast<ModelCursor &>(c);
        save_cursors(&m);
        m.load_position();
        return m;
    }

    auto open_model_bucket(std::string name, Bucket &b, ModelStore &store) const -> Bucket *;
    auto open_model_cursor(Cursor &c, ModelStore::Tree &tree) const -> Cursor *;
    auto save_cursors(Cursor *exclude = nullptr) const -> void;
    auto deactivate() -> void;

public:
    explicit ModelBucket(std::string name, ModelStore &store, Bucket &b, std::list<ModelBucket *> &parent_buckets, std::list<ModelBucket *>::iterator backref)
        : m_name(std::move(name)),
          m_backref(backref),
          m_parent_buckets(&parent_buckets),
          m_temp(&store.tree),
          m_b(&b)
    {
    }

    ~ModelBucket() override;
    [[nodiscard]] auto new_cursor() const -> Cursor * override;
    auto create_bucket(const Slice &key, Bucket **b_out) -> Status override;
    auto create_bucket_if_missing(const Slice &key, Bucket **b_out) -> Status override;
    auto open_bucket(const Slice &key, Bucket *&b_out) const -> Status override;
    auto drop_bucket(const Slice &key) -> Status override;
    auto get(const Slice &key, CALICODB_STRING *value_out) const -> Status override;
    auto put(const Slice &key, const Slice &value) -> Status override;
    auto put(Cursor &c, const Slice &value) -> Status override;
    auto erase(const Slice &key) -> Status override;
    auto erase(Cursor &c) -> Status override;
};

class ModelTx : public Tx
{
    friend class ModelBucket;

    mutable std::list<ModelBucket *> m_buckets;
    mutable ModelStore m_temp;
    ModelStore *const m_base;
    Tx *const m_tx;

    auto open_model_bucket(std::string name, Bucket &b, ModelStore &store) const -> Bucket *;
    auto save_cursors(Cursor *exclude) const -> void;
    auto check_consistency(const ModelStore::Tree &tree, const Bucket &bucket) const -> void;

public:
    explicit ModelTx(ModelStore &store, Tx &tx)
        : m_temp(store),
          m_base(&store),
          m_tx(&tx)
    {
    }

    ~ModelTx() override;

    [[nodiscard]] auto status() const -> Status override
    {
        return m_tx->status();
    }

    [[nodiscard]] auto toplevel() const -> Cursor & override
    {
        return m_tx->toplevel();
    }

    auto vacuum() -> Status override
    {
        save_cursors(nullptr);
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

    auto create_bucket(const Slice &name, Bucket **b_out) -> Status override;
    auto create_bucket_if_missing(const Slice &name, Bucket **b_out) -> Status override;
    auto open_bucket(const Slice &name, Bucket *&b_out) const -> Status override;
    auto drop_bucket(const Slice &name) -> Status override;

    auto check_consistency() const -> void;
};

} // namespace calicodb

#endif // CALICODB_UTILS_MODEL_H

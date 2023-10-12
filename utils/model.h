//// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
//// This source code is licensed under the MIT License, which can be found in
//// LICENSE.md. See AUTHORS.md for a list of contributor names.
////
//// This file contains classes that model the intended behavior of higher-level
//// CalicoDB components. Note that these classes don't attempt to catch certain
//// types of API misuse (for example, ModelBucket will write to a bucket in a read-
//// only transaction without complaint).
//
// #ifndef CALICODB_UTILS_MODEL_H
// #define CALICODB_UTILS_MODEL_H
//
// #include "calicodb/cursor.h"
// #include "calicodb/db.h"
// #include "common.h"
// #include "cursor_impl.h"
// #include "logging.h"
// #include <iostream>
// #include <list>
// #include <map>
//
// #define CHECK_TRUE(cond)                                 \
//    do {                                                 \
//        if (!(cond)) {                                   \
//            std::cerr << "expected `" << #cond << "`\n"; \
//            std::abort();                                \
//        }                                                \
//    } while (0)
//
// #define CHECK_FALSE(cond) \
//    CHECK_TRUE(!(cond))
//
// #define CHECK_OK(expr)                                             \
//    do {                                                           \
//        if (auto assert_s = (expr); !assert_s.is_ok()) {           \
//            std::fprintf(                                          \
//                stderr,                                            \
//                "expected `(" #expr ").is_ok()` but got \"%s\"\n", \
//                assert_s.message());                               \
//            std::abort();                                          \
//        }                                                          \
//    } while (0)
//
// #define CHECK_EQ(lhs, rhs)                                                                             \
//    do {                                                                                               \
//        if ((lhs) != (rhs)) {                                                                          \
//            std::cerr << "expected `" << #lhs "` (" << (lhs) << ") == `" #rhs "` (" << (rhs) << ")\n"; \
//            std::abort();                                                                              \
//        }                                                                                              \
//    } while (0)
//
// namespace calicodb
//{
//
// using KVMap = std::map<std::string, std::string>;
// using KVStore = std::map<std::string, KVMap>;
//
// class ModelDB : public DB
//{
//    KVStore *const m_store;
//    DB *const m_db;
//
// public:
//    static auto open(const Options &options, const char *filename, KVStore &store, DB *&db_out) -> Status;
//
//    explicit ModelDB(KVStore &store, DB &db)
//        : m_store(&store),
//          m_db(&db)
//    {
//    }
//
//    ~ModelDB() override;
//
//    auto check_consistency() const -> void;
//
//    auto get_property(const Slice &name, void *value_out) const -> Status override
//    {
//        return m_db->get_property(name, value_out);
//    }
//
//    auto new_writer(Tx *&tx_out) -> Status override;
//    auto new_reader(Tx *&tx_out) const -> Status override;
//
//    auto checkpoint(CheckpointMode mode, CheckpointInfo *info_out) -> Status override
//    {
//        return m_db->checkpoint(mode, info_out);
//    }
//};
//
// class ModelTx;
//
// class ModelCursor : public Cursor
//{
//    friend class ModelTx;
//
//    std::list<Cursor *>::iterator m_backref;
//    Cursor *const m_c;
//    const ModelTx *const m_tx;
//
//    mutable typename KVMap::iterator m_itr;
//    KVMap *m_map;
//
//    mutable std::string m_saved_key;
//    mutable std::string m_saved_val;
//    mutable bool m_saved;
//
//    auto save_position() const -> void
//    {
//        if (!m_saved && m_c->is_valid()) {
//            m_saved_key = to_string(m_c->key());
//            m_saved_val = to_string(m_c->value());
//            // The element at m_itr may have been erased. This will cause m_itr to be
//            // invalidated, but we won't be able to tell, since it probably won't equal
//            // end(*m_map). This makes sure the iterator can still be used as a hint in
//            // ModelTx::put().
//            m_itr = end(*m_map);
//            m_saved = true;
//        }
//    }
//
//    auto load_position() const -> std::pair<bool, std::string>
//    {
//        if (m_saved) {
//            m_saved = false;
//            m_itr = m_map->lower_bound(m_saved_key);
//            return {true, m_saved_key};
//        }
//        return {false, ""};
//    }
//
//    auto move_to(typename KVMap::iterator position) const -> void
//    {
//        m_saved = false;
//        m_itr = position;
//    }
//
// public:
//    explicit ModelCursor(Cursor &c, const ModelTx &tx, KVMap &map, std::list<Cursor *>::iterator backref)
//        : m_backref(backref),
//          m_c(&c),
//          m_tx(&tx),
//          m_itr(end(map)),
//          m_map(&map),
//          m_saved(false)
//    {
//    }
//
//    ~ModelCursor() override;
//
//    [[nodiscard]] auto map() -> KVMap &
//    {
//        return *m_map;
//    }
//
//    [[nodiscard]] auto handle() -> void * override
//    {
//        return m_c->handle();
//    }
//
//    [[nodiscard]] auto is_valid() const -> bool override
//    {
//        if (m_c->status().is_ok()) {
//            CHECK_EQ(m_c->is_valid(),
//                     m_itr != end(*m_map) || m_saved);
//            check_record();
//        }
//        return m_c->is_valid();
//    }
//
//    [[nodiscard]] auto status() const -> Status override
//    {
//        return m_c->status();
//    }
//
//    auto check_record() const -> void
//    {
//        if (m_c->is_valid()) {
//            const auto key = m_saved ? m_saved_key
//                                     : m_itr->first;
//            CHECK_EQ(key, to_string(m_c->key()));
//
//            const auto value = m_saved ? m_saved_val : m_itr->second;
//            CHECK_EQ(value, to_string(m_c->value()));
//        }
//    }
//
//    [[nodiscard]] auto key() const -> Slice override
//    {
//        return m_c->key();
//    }
//
//    [[nodiscard]] auto value() const -> Slice override
//    {
//        return m_c->value();
//    }
//
//    auto find(const Slice &key) -> void override
//    {
//        m_saved = false;
//        m_itr = m_map->find(to_string(key));
//        m_c->find(key);
//    }
//
//    auto seek(const Slice &key) -> void override
//    {
//        m_saved = false;
//        m_itr = m_map->lower_bound(to_string(key));
//        m_c->seek(key);
//    }
//
//    auto seek_first() -> void override
//    {
//        m_saved = false;
//        m_itr = begin(*m_map);
//        m_c->seek_first();
//    }
//
//    auto seek_last() -> void override
//    {
//        m_saved = false;
//        m_itr = end(*m_map);
//        if (!m_map->empty()) {
//            --m_itr;
//        }
//        m_c->seek_last();
//    }
//
//    auto next() -> void override
//    {
//        load_position();
//        if (m_itr != end(*m_map)) {
//            ++m_itr;
//        }
//        m_c->next();
//    }
//
//    auto previous() -> void override
//    {
//        load_position();
//        if (m_itr == begin(*m_map)) {
//            m_itr = end(*m_map);
//        } else {
//            --m_itr;
//        }
//        m_c->previous();
//    }
//
//    auto validate() const -> void
//    {
//        reinterpret_cast<const CursorImpl *>(m_c)->TEST_check_state();
//    }
//};
//
// class ModelBucket : public Tx
//{
//    friend class ModelCursor;
//
//    mutable KVStore m_temp;
//    KVStore *const m_base;
//    Tx *const m_tx;
//
//    auto use_cursor(Cursor &c) const -> ModelCursor &
//    {
//        auto &m = reinterpret_cast<ModelCursor &>(c);
//        save_cursors(&m);
//        m.load_position();
//        return m;
//    }
//
//    auto open_model_cursor(Cursor &c, KVMap &map) const -> Cursor *;
//    auto save_cursors(Cursor *exclude = nullptr) const -> void;
//    mutable std::list<Cursor *> m_cursors;
//
// public:
//    explicit ModelBucket(KVStore &store, Tx &tx)
//        : m_temp(store),
//          m_base(&store),
//          m_tx(&tx)
//    {
//    }
//
//    ~ModelBucket() override;
//
//    // WARNING: Invalidates all open cursors.
//    auto check_consistency() const -> void;
//
//    [[nodiscard]] auto status() const -> Status override
//    {
//        return m_tx->status();
//    }
//
//    auto create_bucket(const BucketOptions &options, const Slice &name, Cursor **c_out) -> Status override;
//    [[nodiscard]] auto open_bucket(const Slice &name, Cursor *&c_out) const -> Status override;
//
//    auto drop_bucket(const Slice &name) -> Status override
//    {
//        m_schema.seek(name);
//        auto s = m_tx->drop_bucket(name);
//        if (s.is_ok()) {
//            CHECK_TRUE(m_schema.m_itr != end(m_temp));
//            m_schema.move_to(m_temp.erase(m_schema.m_itr));
//        }
//        return s;
//    }
//
//    auto put(Cursor &c, const Slice &key, const Slice &value) -> Status override;
//    auto erase(Cursor &c, const Slice &key) -> Status override;
//    auto erase(Cursor &c) -> Status override;
//};
//
// class ModelTx : public Tx
//{
//    friend class ModelCursor;
//
//    mutable KVStore m_temp;
//    KVStore *const m_base;
//    Tx *const m_tx;
//
// public:
//    explicit ModelTx(KVStore &store, Tx &tx)
//        : m_temp(store),
//          m_base(&store),
//          m_tx(&tx)
//    {
//    }
//
//    ~ModelTx() override;
//
//    [[nodiscard]] auto status() const -> Status override
//    {
//        return m_tx->status();
//    }
//
//    auto vacuum() -> Status override
//    {
//        save_cursors();
//        return m_tx->vacuum();
//    }
//
//    auto commit() -> Status override
//    {
//        auto s = m_tx->commit();
//        if (s.is_ok()) {
//            *m_base = m_temp;
//        }
//        return s;
//    }
//};
//
// ModelCursor::~ModelCursor()
//{
//    m_tx->m_cursors.erase(m_backref);
//    delete m_c;
//}
//
//} // namespace calicodb
//
// #endif // CALICODB_UTILS_MODEL_H

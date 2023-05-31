// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "env_helpers.h"
#include "fuzzer.h"
#include "harness.h"
#include "model.h"
#include "tools.h"

namespace calicodb
{
using namespace tools;

class CheckedDB : public DB
{
    ModelDB m_model;
    DB *m_real;

    explicit CheckedDB(DB &db, KVStore &store)
        : m_model(&store),
          m_real(&db)
    {
    }

public:
    [[nodiscard]] static auto open(const Options &options, const std::string &filename, KVStore &store, DB *&db_out) -> Status
    {
        DB *db;
        auto s = DB::open(options, filename, db);
        if (s.is_ok()) {
            db_out = new CheckedDB(*db, store);
        } else {
            db_out = nullptr;
        }
        return s;
    }

    ~CheckedDB() override;

    auto get_property(const Slice &name, std::string *value_out) const -> bool override
    {
        return m_real->get_property(name, value_out);
    }

    [[nodiscard]] auto new_txn(bool write, Txn *&txn_out) -> Status override;

    [[nodiscard]] auto checkpoint(bool reset) -> Status override
    {
        return m_real->checkpoint(reset);
    }
};

class CheckedTxn : public Txn
{
    ModelTxn *m_model;
    Txn *m_real;

public:
    explicit CheckedTxn(Txn &real, ModelTxn &model)
        : m_model(&model),
          m_real(&real)
    {
    }

    ~CheckedTxn() override;

    [[nodiscard]] auto status() const -> Status override
    {
        return m_real->status();
    }

    [[nodiscard]] auto schema() const -> Cursor & override
    {
        return m_real->schema();
    }

    [[nodiscard]] auto create_table(const TableOptions &options, const Slice &name, Table **out) -> Status override;

    [[nodiscard]] auto drop_table(const Slice &name) -> Status override
    {
        auto s = m_model->drop_table(name);
        const auto t = m_real->drop_table(name);
        if (s.is_ok()) {
            CHECK_OK(t);
        } else {
            CHECK_TRUE(!t.is_ok());
        }
        return s;
    }

    [[nodiscard]] auto vacuum() -> Status override
    {
        return m_real->vacuum();
    }

    [[nodiscard]] auto commit() -> Status override
    {
        (void)m_model->commit();
        return m_real->commit();
    }
};

class CheckedTable : public Table
{
    Table *m_model;
    Table *m_real;

public:
    explicit CheckedTable(Table &real, Table &model)
        : m_model(&model),
          m_real(&real)
    {
    }

    ~CheckedTable() override;

    [[nodiscard]] auto new_cursor() const -> Cursor * override;

    [[nodiscard]] auto get(const Slice &key, std::string *value) const -> Status override
    {
        std::string result;
        auto s = m_model->get(key, &result);
        if (s.is_ok()) {
            CHECK_OK(m_real->get(key, value));
            CHECK_EQ(*value, result);
        } else {
            CHECK_TRUE(!m_real->get(key, value).is_ok());
        }
        return s;
    }

    [[nodiscard]] auto put(const Slice &key, const Slice &value) -> Status override
    {
        (void)m_model->put(key, value);
        return m_real->put(key, value);
    }

    [[nodiscard]] auto erase(const Slice &key) -> Status override
    {
        (void)m_model->erase(key);
        return m_real->erase(key);
    }
};

class CheckedCursor : public Cursor
{
    Cursor *m_model;
    Cursor *m_real;

public:
    explicit CheckedCursor(Cursor &real, Cursor &model)
        : m_model(&model),
          m_real(&real)
    {
    }

    ~CheckedCursor() override;

    [[nodiscard]] auto is_valid() const -> bool override
    {
        CHECK_EQ(m_model->is_valid(), m_real->is_valid());
        return m_model->is_valid();
    }

    [[nodiscard]] auto status() const -> Status override
    {
        return m_real->status();
    }

    [[nodiscard]] auto key() const -> Slice override
    {
        CHECK_TRUE(m_model->key() == m_real->key());
        return m_model->key();
    }

    [[nodiscard]] auto value() const -> Slice override
    {
        CHECK_TRUE(m_model->value() == m_real->value());
        return m_model->value();
    }

    auto seek(const Slice &key) -> void override
    {
        m_model->seek(key);
        m_real->seek(key);
    }

    auto seek_first() -> void override
    {
        m_model->seek_first();
        m_real->seek_first();
    }

    auto seek_last() -> void override
    {
        m_model->seek_last();
        m_real->seek_last();
    }

    auto next() -> void override
    {
        m_model->next();
        m_real->next();
    }

    auto previous() -> void override
    {
        m_model->previous();
        m_real->previous();
    }
};

CheckedDB::~CheckedDB()
{
    delete m_real;
}

auto CheckedDB::new_txn(bool write, Txn *&txn_out) -> Status
{
    Txn *real_txn;
    auto s = m_real->new_txn(write, real_txn);
    if (s.is_ok()) {
        Txn *model_txn;
        CHECK_OK(m_model.new_txn(write, model_txn));
        txn_out = new CheckedTxn(*real_txn, reinterpret_cast<ModelTxn &>(*model_txn));
    }
    return s;
}

CheckedTxn::~CheckedTxn()
{
    delete m_real;
    delete m_model;
}

auto CheckedTxn::create_table(const TableOptions &options, const Slice &name, Table **out) -> Status
{
    Table *real_table;
    auto s = m_real->create_table(options, name, &real_table);
    if (s.is_ok()) {
        Table *model_table;
        (void)m_model->create_table(options, name, &model_table);
        *out = new CheckedTable(*real_table, *model_table);
        m_model->set_aux_ptr(name, *out, [](auto *ptr) {
            delete reinterpret_cast<CheckedTable *>(ptr);
        });
    }
    return s;
}

CheckedTable::~CheckedTable() = default;

auto CheckedTable::new_cursor() const -> Cursor *
{
    return new CheckedCursor(
        *m_real->new_cursor(),
        *m_model->new_cursor());
}

CheckedCursor::~CheckedCursor()
{
    delete m_model;
    delete m_real;
}

class DBFuzzer
{
    Options m_options;
    std::string m_filename;
    KVStore m_store;
    DB *m_db = nullptr;
    Txn *m_tx = nullptr;
    Table *m_tb = nullptr;

    auto reopen_db() -> void
    {
        delete m_tx;
        delete m_db;
        m_tb = nullptr;
        m_tx = nullptr;
        CHECK_OK(CheckedDB::open(m_options, m_filename, m_store, m_db));
        reopen_txn();
        reopen_table();
    }

    auto reopen_txn() -> void
    {
        delete m_tx;
        m_tb = nullptr;
        CHECK_OK(m_db->new_txn(true, m_tx));
        reopen_table();
    }

    auto reopen_table() -> void
    {
        // This should be a NOOP if the table handle has already been created
        // since this transaction was started. The same exact handle is returned.
        CHECK_OK(m_tx->create_table(TableOptions(), "TABLE", &m_tb));
    }

public:
    explicit DBFuzzer(std::string filename, Options *options)
        : m_filename(std::move(filename))
    {
        if (options) {
            m_options = *options;
        }
        (void)DB::destroy(m_options, m_filename);
        reopen_db();
    }

    ~DBFuzzer()
    {
        delete m_tx;
        delete m_db;
    }

    auto fuzz(FuzzerStream &stream) -> bool;
};

auto DBFuzzer::fuzz(FuzzerStream &stream) -> bool
{
    if (stream.is_empty()) {
        return false;
    }

    const enum OperationType {
        kTablePut,
        kTableGet,
        kTableErase,
        kCursorSeek,
        kCursorIterate,
        kTxnCommit,
        kTxnVacuum,
        kReopenDB,
        kReopenTxn,
        kReopenTable,
        kOpCount
    } op_type = OperationType(U32(stream.extract_fixed(1)[0]) % kOpCount);

    Cursor *c = nullptr;
    std::string value;
    Slice key;
    Status s;

    switch (op_type) {
        case kTableGet:
            s = m_tb->get(stream.extract_random(), &value);
            break;
        case kTablePut:
            key = stream.extract_random();
            s = m_tb->put(key, stream.extract_random());
            break;
        case kTableErase:
            s = m_tb->erase(stream.extract_random());
            break;
        case kCursorSeek:
            key = stream.extract_random();
            c = m_tb->new_cursor();
            c->seek(key);
            while (c->is_valid()) {
                if (key.is_empty() || (key[0] & 1)) {
                    c->previous();
                } else {
                    c->next();
                }
            }
            break;
        case kCursorIterate:
            c = m_tb->new_cursor();
            c->seek_first();
            while (c->is_valid()) {
                c->next();
            }
            c->seek_last();
            while (c->is_valid()) {
                c->previous();
            }
            break;
        case kTxnVacuum:
            s = m_tx->vacuum();
            break;
        case kTxnCommit:
            s = m_tx->commit();
            break;
        case kReopenTxn:
            reopen_txn();
            break;
        case kReopenTable:
            reopen_table();
            break;
        default: // kReopenDB
            reopen_db();
    }
    if (c) {
        // Cursor should have been moved off the edge of the range.
        CHECK_FALSE(c->is_valid());
        CHECK_TRUE(c->status().is_not_found());
        delete c;
    }

    // All records should match between DB and ModelDB.
    c = m_tb->new_cursor();
    c->seek_first();
    while (c->is_valid()) {
        c->next();
    }
    CHECK_TRUE(c->status().is_not_found());
    delete c;

    if (s.is_not_found() || s.is_invalid_argument()) {
        // Forgive non-fatal errors.
        s = Status::ok();
    }
    CHECK_OK(s);
    CHECK_OK(m_tx->status());
    return true;
}

extern "C" int LLVMFuzzerTestOneInput(const U8 *data, std::size_t size)
{
    Options options;
    options.env = new tools::FakeEnv;
    options.cache_size = 0; // Use the smallest possible cache.

    {
        FuzzerStream stream(data, size);
        DBFuzzer fuzzer("db_fuzzer.cdb", &options);
        while (fuzzer.fuzz(stream)) {
        }
    }

    delete options.env;
    return 0;
}

} // namespace calicodb
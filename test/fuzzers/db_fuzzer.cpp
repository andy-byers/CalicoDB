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
    }
    return s;
}

CheckedTable::~CheckedTable()
{
    delete m_model;
    delete m_real;
}

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

enum OperationType {
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
};

constexpr std::size_t kMaxTableSize = 1'000;
constexpr std::size_t kMaxTables = 10;

class DBFuzzer
{
    Options m_options;
    std::string m_filename;
    KVStore m_store;
    DB *m_db = nullptr;
    Txn *m_txn = nullptr;
    Table *m_tb = nullptr;

    auto reopen_db() -> void
    {
        delete m_txn;
        delete m_db;
        m_tb = nullptr;
        m_txn = nullptr;
        CHECK_OK(CheckedDB::open(m_options, m_filename, m_store, m_db));
        reopen_txn();
        reopen_tb();
    }

    auto reopen_txn() -> void
    {
        delete m_txn;
        m_tb = nullptr;
        CHECK_OK(m_db->new_txn(true, m_txn));
        reopen_tb();
    }

    auto reopen_tb() -> void
    {
        // This should be a NOOP if the table handle has already been created
        // since this transaction was started. The same exact handle is returned.
        CHECK_OK(m_txn->create_table(TableOptions(), "TABLE", &m_tb));
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
        delete m_txn;
        delete m_db;
    }

    auto fuzz(const U8 *&data_ptr, std::size_t &size_ref) -> void;
};

auto DBFuzzer::fuzz(const U8 *&data_ptr, std::size_t &size_ref) -> void
{
    CHECK_TRUE(size_ref >= 2);
    auto operation_type = static_cast<OperationType>(*data_ptr++ % OperationType::kOpCount);
    --size_ref;

    Cursor *c = nullptr;
    std::string value;
    std::string key;
    Status s;

    switch (operation_type) {
        case kTableGet:
            s = m_tb->get(FUZZER_KEY, &value);
            if (!s.is_not_found()) {
                CHECK_OK(s);
            }
            break;
        case kTablePut:
            key = FUZZER_KEY;
            CHECK_OK(m_tb->put(key, FUZZER_VAL));
            break;
        case kTableErase:
            CHECK_OK(m_tb->erase(FUZZER_KEY));
            break;
        case kCursorSeek:
            key = FUZZER_KEY;
            c = m_tb->new_cursor();
            c->seek(key);
            while (c->is_valid()) {
                if (key.front() & 1) {
                    c->next();
                } else {
                    c->previous();
                }
            }
            break;
        case kTxnVacuum:
            CHECK_OK(m_txn->vacuum());
            break;
        case kTxnCommit:
            CHECK_OK(m_txn->commit());
            break;
        case kReopenTxn:
            reopen_txn();
            break;
        case kReopenTable:
            reopen_tb();
            break;
        default: // kReopen
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
    CHECK_OK(m_txn->status());
    delete c;
}

extern "C" int LLVMFuzzerTestOneInput(const U8 *data, std::size_t size)
{
    Options options;
    options.env = new tools::FakeEnv;
    options.cache_size = 0; // Use the smallest possible cache.

    {
        DBFuzzer fuzzer("db_fuzzer.cdb", &options);

        while (size > 1) {
            fuzzer.fuzz(data, size);
        }
    }

    delete options.env;
    return 0;
}

} // namespace calicodb
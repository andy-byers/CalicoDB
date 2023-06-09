// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "fake_env.h"
#include "fuzzer.h"
#include "model.h"

namespace calicodb
{

static auto common_status(const Status &real_s, const Status &model_s) -> Status
{
    CHECK_TRUE(real_s == model_s);
    return real_s;
}

class CheckedDB : public DB
{
    ModelDB m_model;
    DB *m_real;

    explicit CheckedDB(DB &db, KVMap &store)
        : m_model(&store),
          m_real(&db)
    {
    }

public:
    auto real() -> DB * { return m_real; }
    [[nodiscard]] static auto open(const Options &options, const std::string &filename, KVMap &store, DB *&db_out) -> Status
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

    [[nodiscard]] auto new_tx(const Tx *&tx_out) const -> Status override;
    auto new_tx(WriteTag, Tx *&tx_out) -> Status override;

    auto checkpoint(bool reset) -> Status override
    {
        return common_status(
            m_real->checkpoint(reset),
            m_model.checkpoint(reset));
    }
};

class CheckedTx : public Tx
{
    ModelTx *m_model;
    Tx *m_real;

public:
    explicit CheckedTx(Tx &real, ModelTx &model)
        : m_model(&model),
          m_real(&real)
    {
    }

    ~CheckedTx() override;

    [[nodiscard]] auto status() const -> Status override
    {
        return common_status(
            m_real->status(),
            m_model->status());
    }

    [[nodiscard]] auto schema() const -> Cursor & override
    {
        return m_real->schema();
    }

    auto create_bucket(const BucketOptions &options, const Slice &name, Bucket *tb_out) -> Status override;
    [[nodiscard]] auto open_bucket(const Slice &name, Bucket &tb_out) const -> Status override;

    auto drop_bucket(const Slice &name) -> Status override
    {
        return common_status(
            m_real->drop_bucket(name),
            m_model->drop_bucket(name));
    }

    auto vacuum() -> Status override
    {
        return common_status(
            m_real->vacuum(),
            m_model->vacuum());
    }

    auto commit() -> Status override
    {
        return common_status(
            m_real->commit(),
            m_model->commit());
    }

    [[nodiscard]] auto new_cursor(const Bucket &b) const -> Cursor * override;

    [[nodiscard]] auto get(const Bucket &b, const Slice &key, std::string *value) const -> Status override
    {
        std::string actual;
        auto s = common_status(
            m_real->get(b, key, value),
            m_model->get(b, key, &actual));
        if (s.is_ok()) {
            CHECK_EQ(*value, actual);
        } else {
            CHECK_TRUE(actual.empty());
        }
        return s;
    }

    auto put(const Bucket &b, const Slice &key, const Slice &value) -> Status override
    {
        return common_status(
            m_real->put(b, key, value),
            m_model->put(b, key, value));
    }

    auto erase(const Bucket &b, const Slice &key) -> Status override
    {
        return common_status(
            m_real->erase(b, key),
            m_model->erase(b, key));
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
        return common_status(
            m_real->status(),
            m_model->status());
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

auto CheckedDB::new_tx(WriteTag, Tx *&tx_out) -> Status
{
    Tx *real_tx, *model_tx;
    auto s = common_status(
        m_real->new_tx(WriteTag(), real_tx),
        m_model.new_tx(WriteTag(), model_tx));
    if (s.is_ok()) {
        tx_out = new CheckedTx(*real_tx, reinterpret_cast<ModelTx &>(*model_tx));
    } else {
        tx_out = nullptr;
    }
    return s;
}

auto CheckedDB::new_tx(const Tx *&) const -> Status
{
    // Only fuzzing on read-write transactions.
    return Status::not_supported();
}

CheckedTx::~CheckedTx()
{
    delete m_real;
    delete m_model;
}

auto CheckedTx::create_bucket(const BucketOptions &options, const Slice &name, Bucket *b_out) -> Status
{
    return common_status(
        m_real->create_bucket(options, name, b_out),
        m_model->create_bucket(options, name, b_out));
}

auto CheckedTx::open_bucket(const Slice &name, Bucket &b_out) const -> Status
{
    return common_status(
        m_real->open_bucket(name, b_out),
        m_model->open_bucket(name, b_out));
}

auto CheckedTx::new_cursor(const Bucket &b) const -> Cursor *
{
    return new CheckedCursor(
        *m_real->new_cursor(b),
        *m_model->new_cursor(b));
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
    KVMap m_store;
    DB *m_db = nullptr;
    Tx *m_tx = nullptr;
    Bucket m_b;

    auto reopen_db() -> void
    {
        delete m_tx;
        delete m_db;
        m_tx = nullptr;
        CHECK_OK(CheckedDB::open(m_options, m_filename, m_store, m_db));
        reopen_tx();
        reopen_bucket();
    }

    auto reopen_tx() -> void
    {
        delete m_tx;
        CHECK_OK(m_db->new_tx(WriteTag{}, m_tx));
        reopen_bucket();
    }

    auto reopen_bucket() -> void
    {
        // This should be a NOOP if the bucket handle has already been created
        // since this transaction was started. The same exact handle is returned.
        CHECK_OK(m_tx->create_bucket(BucketOptions(), "BUCKET", &m_b));
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
        kBucketPut,
        kBucketGet,
        kBucketErase,
        kCursorSeek,
        kCursorIterate,
        kTxCommit,
        kTxVacuum,
        kReopenDB,
        kReopenTx,
        kReopenBucket,
        kOpCount
    } op_type = OperationType(U32(stream.extract_fixed(1)[0]) % kOpCount);

    Cursor *c = nullptr;
    std::string value;
    Slice key;
    Status s;

    switch (op_type) {
        case kBucketGet:
            s = m_tx->get(m_b, stream.extract_random(), &value);
            break;
        case kBucketPut:
            key = stream.extract_random();
            s = m_tx->put(m_b, key, stream.extract_random());
            break;
        case kBucketErase:
            s = m_tx->erase(m_b, stream.extract_random());
            break;
        case kCursorSeek:
            key = stream.extract_random();
            c = m_tx->new_cursor(m_b);
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
            c = m_tx->new_cursor(m_b);
            c->seek_first();
            while (c->is_valid()) {
                c->next();
            }
            c->seek_last();
            while (c->is_valid()) {
                c->previous();
            }
            break;
        case kTxVacuum:
            s = m_tx->vacuum();
            break;
        case kTxCommit:
            s = m_tx->commit();
            break;
        case kReopenTx:
            reopen_tx();
            break;
        case kReopenBucket:
            reopen_bucket();
            break;
        default: // kReopenDB
            reopen_db();
    }
    if (c) {
        // Cursor should have been moved off the edge of the range.
        CHECK_FALSE(c->is_valid());
        CHECK_OK(c->status());
        delete c;
    }

    // All records should match between DB and ModelDB.
    c = m_tx->new_cursor(m_b);
    c->seek_first();
    while (c->is_valid()) {
        c->next();
    }
    CHECK_OK(c->status());
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
    options.env = new FakeEnv;
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
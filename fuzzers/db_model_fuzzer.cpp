// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "fake_env.h"
#include "fuzzer.h"
#include "logging.h"
#include "model.h"
#include "tree.h"

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
    [[nodiscard]] auto real() -> DB *
    {
        return m_real;
    }

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

    auto put(Cursor &c, const Slice &key, const Slice &value) -> Status override;

    auto erase(const Bucket &b, const Slice &key) -> Status override
    {
        return common_status(
            m_real->erase(b, key),
            m_model->erase(b, key));
    }

    auto erase(Cursor &c) -> Status override;
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

    [[nodiscard]] auto token() -> void * override
    {
        return m_real->token();
    }

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

    [[nodiscard]] auto real() -> Cursor &
    {
        return *m_real;
    }

    [[nodiscard]] auto model() -> ModelCursor &
    {
        return reinterpret_cast<ModelCursor &>(*m_model);
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

auto CheckedTx::put(Cursor &c, const Slice &key, const Slice &value) -> Status
{
    return common_status(
        m_real->put(reinterpret_cast<CheckedCursor &>(c).real(), key, value),
        m_model->put(reinterpret_cast<CheckedCursor &>(c).model(), key, value));
}

auto CheckedTx::erase(Cursor &c) -> Status
{
    return common_status(
        m_real->erase(reinterpret_cast<CheckedCursor &>(c).real()),
        m_model->erase(reinterpret_cast<CheckedCursor &>(c).model()));
}

CheckedCursor::~CheckedCursor()
{
    delete m_model;
    delete m_real;
}

class Fuzzer
{
    Options m_options;
    KVMap m_store;
    DB *m_db = nullptr;
    Tx *m_tx = nullptr;
    Cursor *m_c = nullptr;
    Bucket m_b;

    auto reopen_db() -> void
    {
        delete m_c;
        delete m_tx;
        delete m_db;
        m_c = nullptr;
        m_tx = nullptr;
        CHECK_OK(CheckedDB::open(m_options, "", m_store, m_db));
        reopen_tx();
    }

    auto reopen_tx() -> void
    {
        delete m_c;
        delete m_tx;
        m_c = nullptr;
        CHECK_OK(m_db->new_tx(WriteTag(), m_tx));
        reopen_bucket();
    }

    auto reopen_bucket() -> void
    {
        delete m_c;
        // This should be a NOOP if the bucket handle has already been created
        // since this transaction was started. The same exact handle is returned.
        CHECK_OK(m_tx->create_bucket(BucketOptions(), "BUCKET", &m_b));
        m_c = m_tx->new_cursor(m_b);
    }

public:
    explicit Fuzzer(Env &env)
    {
        env.srand(42);
        m_options.env = &env;
        m_options.cache_size = 0;
        reopen_db();
    }

    ~Fuzzer()
    {
        delete m_c;
        delete m_tx;
        delete m_db;
    }

    auto fuzz(FuzzerStream &stream) -> bool;
};

auto Fuzzer::fuzz(FuzzerStream &stream) -> bool
{
    if (stream.is_empty()) {
        return false;
    }

    const enum OperationType {
        kBucketPut,
        kBucketGet,
        kBucketErase,
        kCursorNext,
        kCursorPrevious,
        kCursorSeek,
        kCursorPut,
        kCursorErase,
        kTxCommit,
        kTxVacuum,
        kReopenDB,
        kReopenTx,
        kReopenBucket,
        kValidateDB,
        kOpCount
    } op_type = OperationType(U32(stream.extract_fixed(1)[0]) % kOpCount);

#ifdef FUZZER_TRACE
    static constexpr const char *kOperationTypeNames[kOpCount] = {
        "kBucketPut",
        "kBucketGet",
        "kBucketErase",
        "kCursorNext",
        "kCursorPrevious",
        "kCursorSeek",
        "kCursorPut",
        "kCursorErase",
        "kTxCommit",
        "kTxVacuum",
        "kReopenDB",
        "kReopenTx",
        "kReopenBucket",
        "kValidateDB",
    };
    const auto sample_len = std::min(stream.length(), 8UL);
    const auto missing_len = stream.length() - sample_len;
    const auto sample = escape_string(stream.peek(sample_len));
    std::cout << "TRACE: OpType: " << kOperationTypeNames[op_type] << R"( Input: ")"
              << sample << R"(" + <)" << missing_len << " bytes>\n";
#endif // FUZZER_TRACE

    std::string value;
    Slice key;
    Status s;

    switch (op_type) {
        case kBucketGet:
            s = m_tx->get(m_b, stream.extract_random(), &value);
            break;
        case kBucketPut:
            key = stream.extract_random();
            s = m_tx->put(m_b, key, stream.extract_fake_random());
            m_c->seek(key); // TODO
            break;
        case kBucketErase:
            s = m_tx->erase(m_b, stream.extract_random());
            m_c->seek_first(); // TODO
            break;
        case kCursorSeek:
            key = stream.extract_random();
            m_c->seek(key);
            break;
        case kCursorNext:
            if (m_c->is_valid()) {
                m_c->next();
            } else {
                m_c->seek_first();
            }
            break;
        case kCursorPrevious:
            if (m_c->is_valid()) {
                m_c->previous();
            } else {
                m_c->seek_last();
            }
            break;
        case kCursorPut:
            key = stream.extract_random();
            s = m_tx->put(*m_c, key, stream.extract_fake_random());
            break;
        case kCursorErase:
            s = m_tx->erase(*m_c);
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
        case kValidateDB:
            static_cast<const Tree *>(m_b.state)->TEST_validate();
            break;
        default: // kReopenDB
            reopen_db();
    }
    if (m_c->is_valid()) {
        [[maybe_unused]] const auto _k = m_c->key();
        [[maybe_unused]] const auto _v = m_c->value();
        [[maybe_unused]] const auto _s = m_c->status();
    }

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
    FakeEnv env;
    Fuzzer fuzzer(env);
    FuzzerStream stream(data, size);
    while (fuzzer.fuzz(stream)) {
    }
    return 0;
}

} // namespace calicodb
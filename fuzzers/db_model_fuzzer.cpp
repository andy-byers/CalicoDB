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

class Fuzzer
{
    Options m_options;
    KVStore m_store;
    DB *m_db = nullptr;
    Tx *m_tx = nullptr;
    Cursor *m_c = nullptr;

    auto reopen_db() -> void
    {
        delete m_c;
        delete m_tx;
        delete m_db;
        m_c = nullptr;
        m_tx = nullptr;
        CHECK_OK(ModelDB::open(m_options, "InMemory", m_store, m_db));
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
        CHECK_OK(m_tx->create_bucket(BucketOptions(), "BUCKET", &m_c));
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

    auto fuzz(FuzzedInputProvider &stream) -> bool;
};

auto Fuzzer::fuzz(FuzzedInputProvider &stream) -> bool
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
        kMaxValue = kValidateDB
    } op_type = stream.extract_enum<OperationType>();

#ifdef FUZZER_TRACE
    static constexpr const char *kOperationTypeNames[kMaxValue + 1] = {
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

    std::string key, value;
    Status s;

    auto &schema = m_tx->schema();
    schema.seek_first();
    CHECK_TRUE(schema.is_valid());

    switch (op_type) {
        case kBucketGet:
            m_c->find(stream.extract_random());
            s = m_c->status();
            break;
        case kBucketPut:
            key = stream.extract_random();
            s = m_tx->put(*m_c, key, stream.extract_random_record_value());
            break;
        case kBucketErase:
            s = m_tx->erase(*m_c, stream.extract_random());
            break;
        case kCursorErase:
            s = m_tx->erase(*m_c);
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
            reinterpret_cast<ModelDB *>(m_db)->check_consistency();
            reinterpret_cast<ModelTx *>(m_tx)->check_consistency();
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

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    FakeEnv env;
    Fuzzer fuzzer(env);
    FuzzedInputProvider stream(data, size);
    while (fuzzer.fuzz(stream)) {
    }
    return 0;
}

} // namespace calicodb
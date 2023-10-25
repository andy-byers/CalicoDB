// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "fake_env.h"
#include "fuzzer.h"
#include "logging.h"
#include "model.h"

namespace calicodb
{

class Fuzzer
{
    Options m_options;
    ModelStore m_store;
    DB *m_db = nullptr;
    Tx *m_tx = nullptr;
    Bucket *m_b = nullptr;
    Cursor *m_c = nullptr;

    auto reopen_db() -> void
    {
        delete m_c;
        delete m_b;
        delete m_tx;
        delete m_db;
        m_c = nullptr;
        m_b = nullptr;
        m_tx = nullptr;
        CHECK_OK(ModelDB::open(m_options, "/InMemory", m_store, m_db));
        reopen_tx();
    }

    auto reopen_tx() -> void
    {
        delete m_c;
        delete m_b;
        delete m_tx;
        m_c = nullptr;
        m_b = nullptr;
        CHECK_OK(m_db->new_writer(m_tx));
        reopen_bucket();
    }

    auto reopen_bucket() -> void
    {
        delete m_c;
        delete m_b;
        CHECK_OK(m_tx->main_bucket().create_bucket_if_missing("BUCKET", &m_b));
        m_c = m_b->new_cursor();
    }

public:
    explicit Fuzzer(Env &env)
    {
        env.srand(42);
        m_options.env = &env;
        m_options.cache_size = 0;
        m_options.page_size = kMinPageSize;
        reopen_db();
    }

    ~Fuzzer()
    {
        delete m_c;
        delete m_b;
        delete m_tx;
        delete m_db;
    }

    auto consume_input(FuzzedInputProvider &stream) -> bool
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
        String repr;
        const auto sample_len = std::min(stream.length(), 8UL);
        const auto missing_len = stream.length() - sample_len;
        CHECK_EQ(append_escaped_string(repr, stream.peek(sample_len)), 0);
        std::cout << "TRACE: OpType: " << kOperationTypeNames[op_type] << R"( Input: ")"
                  << repr.c_str() << R"(" + <)" << missing_len << " bytes>\n";
#endif // FUZZER_TRACE

        std::string str;
        std::string str2;
        Status s;

        auto toplevel = test_new_cursor(m_tx->main_bucket());
        toplevel->seek_first();
        CHECK_TRUE(toplevel->is_valid());

        switch (op_type) {
            case kBucketGet:
                str = stream.extract_random();
                m_c->find(str);
                s = m_c->status();
                break;
            case kBucketPut:
                str = stream.extract_random();
                str2 = stream.extract_random();
                s = m_b->put(str, str2);
                break;
            case kBucketErase:
                str = stream.extract_random();
                s = m_b->erase(str);
                break;
            case kCursorErase:
                s = m_b->erase(*m_c);
                break;
            case kCursorSeek:
                str = stream.extract_random();
                m_c->seek(str);
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
};

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    CHECK_OK(configure(kSetAllocator, DebugAllocator::config()));
    default_env().srand(42);
    {
        FakeEnv env;
        Fuzzer fuzzer(env);
        FuzzedInputProvider stream(data, size);
        while (fuzzer.consume_input(stream)) {
        }
    }
    CHECK_EQ(DebugAllocator::bytes_used(), 0);
    return 0;
}

} // namespace calicodb
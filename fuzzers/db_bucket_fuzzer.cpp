// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/cursor.h"
#include "calicodb/db.h"
#include "calicodb/env.h"
#include "fake_env.h"
#include "fuzzer.h"
#include "tree.h"
#include <memory>

namespace calicodb
{

class Fuzzer
{
    static constexpr size_t kMaxBuckets = 8;

    Options m_options;
    DB *m_db = nullptr;

    auto reopen_db() -> void
    {
        delete m_db;
        CHECK_OK(DB::open(m_options, "MemDB", m_db));
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
        delete m_db;
    }

    static auto check_bucket(const Bucket &b) -> void
    {
        static_cast<const Tree *>(b.state)->TEST_validate();
    }

    auto consume_input(FuzzedInputProvider &stream) -> void
    {
        enum OperationType : char {
            kOpNext,
            kOpPrevious,
            kOpSeek,
            kOpPut,
            kOpErase,
            kOpModify,
            kOpDrop,
            kOpVacuum,
            kOpSelect,
            kOpCommit,
            kOpFinish,
            kOpCheck,
            kMaxValue = kOpCheck
        };

        reopen_db();

        const auto s = m_db->update([&stream](auto &tx) {
            std::unique_ptr<Cursor> cursors[kMaxBuckets];
            Bucket buckets[kMaxBuckets];

            while (!stream.is_empty()) {
                const auto idx = stream.extract_integral_in_range<uint16_t>(0, kMaxBuckets - 1);
                if (cursors[idx] == nullptr) {
                    CHECK_OK(tx.create_bucket(BucketOptions(), std::to_string(idx), &buckets[idx]));
                    cursors[idx] = std::unique_ptr<Cursor>(tx.new_cursor(buckets[idx]));
                }
                Status s;
                auto *c = cursors[idx].get();
                switch (stream.extract_enum<OperationType>()) {
                    case kOpNext:
                        if (c->is_valid()) {
                            c->next();
                        } else {
                            c->seek_first();
                        }
                        break;
                    case kOpPrevious:
                        if (c->is_valid()) {
                            c->previous();
                        } else {
                            c->seek_last();
                        }
                        break;
                    case kOpSeek:
                        c->seek(stream.extract_random());
                        break;
                    case kOpModify:
                        if (c->is_valid()) {
                            s = tx.put(*c, c->key(), stream.extract_random_record_value());
                            break;
                        }
                        [[fallthrough]];
                    case kOpPut:
                        s = tx.put(*c, stream.extract_random(), stream.extract_random_record_value());
                        break;
                    case kOpErase:
                        s = tx.erase(*c);
                        break;
                    case kOpVacuum:
                        s = tx.vacuum();
                        break;
                    case kOpCommit:
                        s = tx.commit();
                        break;
                    case kOpDrop:
                        cursors[idx].reset();
                        s = tx.drop_bucket(std::to_string(idx));
                        c = nullptr;
                        break;
                    case kOpCheck:
                        for (size_t i = 0; i < kMaxBuckets; ++i) {
                            if (cursors[i] != nullptr) {
                                check_bucket(buckets[i]);
                            }
                        }
                        break;
                    default:
                        return Status::not_supported("ROLLBACK");
                }
                if (s.is_not_found() || s.is_invalid_argument()) {
                    // Forgive non-fatal errors.
                    s = Status::ok();
                }
                if (s.is_ok() && c) {
                    s = c->status();
                }
                CHECK_OK(s);
                CHECK_OK(tx.status());
            }
            return Status::ok();
        });
        CHECK_TRUE(s.is_ok() || s.to_string() == "not supported: ROLLBACK");
    }
};

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    FakeEnv env;
    FuzzedInputProvider stream(data, size);
    Fuzzer fuzzer(env);
    fuzzer.consume_input(stream);
    return 0;
}

} // namespace calicodb
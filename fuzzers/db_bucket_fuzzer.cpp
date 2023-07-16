// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/cursor.h"
#include "calicodb/db.h"
#include "calicodb/env.h"
#include "fuzzer.h"
#include <memory>

namespace calicodb
{

class Fuzzer
{
    static constexpr std::size_t kMaxBuckets = 8;

    Options m_options;
    std::string m_filename;
    DB *m_db = nullptr;

    auto reopen_db() -> void
    {
        delete m_db;
        CHECK_OK(DB::open(m_options, m_filename, m_db));
    }

public:
    explicit Fuzzer(std::string filename)
        : m_filename(std::move(filename))
    {
        Env::default_env().srand(42);
        (void)DB::destroy(m_options, m_filename);
        reopen_db();
    }

    ~Fuzzer()
    {
        delete m_db;
    }

    auto fuzz(FuzzerStream &stream) -> bool
    {
        static constexpr std::size_t kMinStreamLen = 2;

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
            kOpCount
        };

        reopen_db();

        const auto s = m_db->update([&stream](auto &tx) {
            std::unique_ptr<Cursor> cursors[kMaxBuckets];
            Bucket buckets[kMaxBuckets];

            while (stream.length() > kMinStreamLen) {
                const auto idx = std::size_t(stream.extract_fixed(1)[0]) % kMaxBuckets;
                if (cursors[idx] == nullptr) {
                    CHECK_OK(tx.create_bucket(BucketOptions(), std::to_string(idx), &buckets[idx]));
                    cursors[idx] = std::unique_ptr<Cursor>(tx.new_cursor(buckets[idx]));
                }
                Status s;
                auto *c = cursors[idx].get();
                switch (OperationType{stream.extract_fixed(1)[0]} % kOpCount) {
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
                            s = tx.put(*c, c->key(), stream.extract_fake_random());
                            break;
                        }
                        [[fallthrough]];
                    case kOpPut:
                        s = tx.put(*c, stream.extract_random(), stream.extract_fake_random());
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
        return stream.length() > kMinStreamLen;
    }
};

extern "C" int LLVMFuzzerTestOneInput(const U8 *data, std::size_t size)
{
    FuzzerStream stream(data, size);
    Fuzzer fuzzer("/tmp/calicodb_db_fuzzer");
    while (fuzzer.fuzz(stream)) {
    }

    return 0;
}

} // namespace calicodb
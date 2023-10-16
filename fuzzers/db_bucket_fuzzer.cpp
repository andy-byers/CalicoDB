// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/db.h"
#include "cursor_impl.h"
#include "fuzzer.h"
#include "model.h"
#include "tree.h"
#include <memory>

namespace calicodb
{

class Fuzzer
{
    static constexpr const char *kBucketNames[] = {"A", "B", "C", "D",
                                                   "E", "F", "G", "H",
                                                   "I", "J", "K", "L"};
    static constexpr auto kMaxBuckets = ARRAY_SIZE(kBucketNames);

    Options m_options;
    DB *m_db = nullptr;
    ModelStore m_store;

    auto reopen_db() -> void
    {
        delete m_db;
        CHECK_OK(ModelDB::open(m_options, "MemDB", m_store, m_db));
    }

public:
    explicit Fuzzer()
    {
        m_options.temp_database = true;
        m_options.cache_size = 0;
        m_options.page_size = kMinPageSize;
        reopen_db();
    }

    ~Fuzzer()
    {
        delete m_db;
    }

    static auto check_bucket(Cursor &c) -> void
    {
        reinterpret_cast<const ModelCursor &>(c).validate();
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
            kOpNest,
            kOpUnnest,
            kOpCommit,
            kOpFinish,
            kOpCheck,
            kMaxValue = kOpCheck
        };

        reopen_db();

        const auto s = m_db->update([&stream, &db = *m_db](auto &tx) {
            struct NestedBucket {
                TestBucket b;
                TestCursor c;
            } nested_buckets[kMaxBuckets] = {};

            auto *main_bucket = &tx.main_bucket();
            auto main_cursor = test_new_cursor(*main_bucket);
            int level = -1;

            while (!stream.is_empty()) {
                Status s;
                std::string str;
                auto *b = level >= 0 ? nested_buckets[level].b.get() : main_bucket;
                auto *c = level >= 0 ? nested_buckets[level].c.get() : main_cursor.get();
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
                            s = b->put(*c, stream.extract_random());
                            break;
                        }
                        [[fallthrough]];
                    case kOpPut:
                        str = stream.extract_random();
                        s = b->put(str, stream.extract_random());
                        break;
                    case kOpErase:
                        s = b->erase(*c);
                        break;
                    case kOpVacuum:
                        s = tx.vacuum();
                        break;
                    case kOpCommit:
                        s = tx.commit();
                        break;
                    case kOpDrop:
                        // It shouldn't matter if this ends up dropping the bucket open on the next level. All records
                        // and nested buckets should remain valid until the dropped bucket is closed.
                        s = b->drop_bucket(stream.extract_random());
                        break;
                    case kOpNest:
                        if (level < static_cast<int>(kMaxBuckets - 1)) {
                            ++level;
                            // Open or create a nested bucket rooted at the current level, if one is not already open.
                            if (!nested_buckets[level].b) {
                                str = stream.extract_random();
                                s = test_open_bucket(*b, str, nested_buckets[level].b);
                            }
                            if (s.is_invalid_argument()) {
                                CHECK_FALSE(nested_buckets[level].b); // test_open_bucket() failed
                                s = test_create_and_open_bucket(*b, str, nested_buckets[level].b);
                            }
                            if (s.is_ok()) {
                                nested_buckets[level].c = test_new_cursor(*nested_buckets[level].b);
                            }
                            break;
                        }
                        [[fallthrough]];
                    case kOpUnnest:
                        if (level >= 0) {
                            // Move up a level, but don't close the bucket at the current level. It will be closed
                            // during kOpNest if necessary.
                            --level;
                            break;
                        }
                        [[fallthrough]];
                    case kOpCheck:
                        reinterpret_cast<ModelDB &>(db).check_consistency();
                        reinterpret_cast<ModelTx &>(tx).check_consistency();
                        check_bucket(*c);
                        break;
                    default:
                        return Status::not_supported("ROLLBACK");
                }
                if (s.is_not_found() || s.is_invalid_argument() || s.is_incompatible_value()) {
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
        CHECK_TRUE(s.is_ok() || (s.is_not_supported() && 0 == std::strcmp(s.message(), "ROLLBACK")));
    }
};

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    CHECK_OK(configure(kSetAllocator, DebugAllocator::config()));
    {
        FuzzedInputProvider stream(data, size);
        Fuzzer fuzzer;
        fuzzer.consume_input(stream);
    }
    CHECK_EQ(DebugAllocator::bytes_used(), 0);
    return 0;
}

} // namespace calicodb
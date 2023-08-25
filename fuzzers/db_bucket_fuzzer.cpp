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
    static constexpr size_t kMaxBuckets = 8;

    Options m_options;
    DB *m_db = nullptr;
    KVStore m_store;

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
        reopen_db();
    }

    ~Fuzzer()
    {
        delete m_db;
    }

    static auto check_bucket(Cursor &c) -> void
    {
        reinterpret_cast<const CursorImpl &>(c).TEST_tree().TEST_validate();
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

        const auto s = m_db->run(WriteOptions(), [&stream, &db = *m_db](auto &tx) {
            std::unique_ptr<Cursor> cursors[kMaxBuckets];

            while (!stream.is_empty()) {
                const auto idx = stream.extract_integral_in_range<uint16_t>(0, kMaxBuckets - 1);
                if (cursors[idx] == nullptr) {
                    Cursor *cursor;
                    CHECK_OK(tx.create_bucket(BucketOptions(), std::to_string(idx), &cursor));
                    cursors[idx].reset(cursor);
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
                        for (const auto &to_check : cursors) {
                            if (to_check) {
                                check_bucket(*to_check);
                            }
                        }
                        reinterpret_cast<ModelDB &>(db).check_consistency();
                        reinterpret_cast<ModelTx &>(tx).check_consistency();
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
        CHECK_TRUE(s.is_ok() || (s.is_not_supported() && 0 == std::strcmp(s.message(), "ROLLBACK")));
    }
};

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size)
{
    {
        FuzzedInputProvider stream(data, size);
        Fuzzer fuzzer;
        fuzzer.consume_input(stream);
    }
    CHECK_EQ(Alloc::bytes_used(), 0);
    return 0;
}

} // namespace calicodb
// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.
//
// Runs normal database operations.

#include "ops_fuzzer.h"
#include "db_impl.h"
#include "tree.h"

namespace calicodb
{

enum OperationType {
    kPut,
    kGet,
    kErase,
    kSeekIter,
    kIterForward,
    kIterReverse,
    kBeginTxn,
    kCommitTxn,
    kRollbackTxn,
    kVacuum,
    kReopen,
    kOpCount
};

constexpr std::size_t DB_MAX_RECORDS = 5'000;

OpsFuzzer::OpsFuzzer(std::string path, Options *options)
    : DbFuzzer(std::move(path), options)
{
}

auto OpsFuzzer::step(const U8 *&data, std::size_t &size) -> Status
{
    CHECK_TRUE(size >= 2);
    auto operation_type = static_cast<OperationType>(*data++ % OperationType::kOpCount);
    --size;

    std::unique_ptr<Cursor> cursor;
    std::string value;
    std::string key;
    Status s;

    switch (operation_type) {
        case kGet:
            s = m_table->get(extract_fuzzer_key(data, size), &value);
            if (s.is_not_found()) {
                s = Status::ok();
            }
            CALICODB_TRY(s);
            break;
        case kPut:
            key = extract_fuzzer_key(data, size);
            CALICODB_TRY(m_table->put(key, extract_fuzzer_value(data, size)));
            break;
        case kErase:
            key = extract_fuzzer_key(data, size);
            cursor.reset(m_table->new_cursor());
            cursor->seek(key);
            if (cursor->is_valid()) {
                s = m_table->erase(cursor->key());
                // Cursor is valid, so the record must exist.
                CHECK_FALSE(s.is_not_found());
            }
            CALICODB_TRY(s);
            break;
        case kSeekIter:
            key = extract_fuzzer_key(data, size);
            cursor.reset(m_table->new_cursor());
            cursor->seek(key);
            while (cursor->is_valid()) {
                if (key.front() & 1) {
                    cursor->next();
                } else {
                    cursor->previous();
                }
            }
            break;
        case kIterForward:
            cursor.reset(m_table->new_cursor());
            cursor->seek_first();
            while (cursor->is_valid()) {
                cursor->next();
            }
            break;
        case kIterReverse:
            cursor.reset(m_table->new_cursor());
            cursor->seek_last();
            while (cursor->is_valid()) {
                cursor->previous();
            }
            break;
        case kVacuum:
            CALICODB_TRY(m_txn->vacuum());
            break;
        case kCommitTxn:
            s = m_txn->commit();
            if (!s.is_ok() && !s.is_invalid_argument()) {
                return s;
            }
            break;
        case kRollbackTxn:
            m_txn->rollback();
            break;
        default: // kReopen
            CALICODB_TRY(reopen());
    }
    return m_txn->status();
}

extern "C" int LLVMFuzzerTestOneInput(const U8 *data, std::size_t size)
{
    Options options;
    options.env = new tools::FakeEnv;
    options.cache_size = kPageSize * kMinFrameCount;

    {
        OpsFuzzer fuzzer("ops_db", &options);

        while (size > 1) {
            CHECK_OK(fuzzer.step(data, size));
            fuzzer.validate();
        }
    }
    delete options.env;
    return 0;
}

} // namespace calicodb
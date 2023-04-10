// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.
//
// Runs normal database operations.

#include "ops_fuzzer.h"
#include "db_impl.h"

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
            s = m_db->get(extract_fuzzer_key(data, size), &value);
            if (s.is_not_found()) {
                s = Status::ok();
            }
            CALICODB_TRY(s);
            break;
        case kPut:
            key = extract_fuzzer_key(data, size);
            CALICODB_TRY(m_db->put(key, extract_fuzzer_value(data, size)));
            break;
        case kErase:
            key = extract_fuzzer_key(data, size);
            cursor.reset(m_db->new_cursor());
            cursor->seek(key);
            if (cursor->is_valid()) {
                s = m_db->erase(cursor->key());
                if (s.is_not_found()) {
                    s = Status::ok();
                }
            }
            CALICODB_TRY(s);
            break;
        case kSeekIter:
            key = extract_fuzzer_key(data, size);
            cursor.reset(m_db->new_cursor());
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
            cursor.reset(m_db->new_cursor());
            cursor->seek_first();
            while (cursor->is_valid()) {
                cursor->next();
            }
            break;
        case kIterReverse:
            cursor.reset(m_db->new_cursor());
            cursor->seek_last();
            while (cursor->is_valid()) {
                cursor->previous();
            }
            break;
        case kVacuum:
            CALICODB_TRY(m_db->vacuum());
            break;
        case kBeginTxn:
            m_txn = m_db->begin_txn(TxnOptions());
            break;
        case kCommitTxn:
            s = m_db->commit_txn(m_txn);
            if (!s.is_ok() && !s.is_invalid_argument()) {
                return s;
            }
            break;
        case kRollbackTxn:
            s = m_db->rollback_txn(m_txn);
            if (!s.is_ok() && !s.is_invalid_argument()) {
                return s;
            }
            break;
        default: // kReopen
            CALICODB_TRY(reopen());
    }
    return m_db->status();
}

extern "C" int LLVMFuzzerTestOneInput(const U8 *data, std::size_t size)
{
    Options options;
    options.env = new tools::FakeEnv;
    options.page_size = kMinPageSize;
    options.cache_size = kMinPageSize * 16;

    {
        OpsFuzzer fuzzer {"ops_db", &options};

        while (size > 1) {
            CHECK_OK(fuzzer.step(data, size));
            fuzzer.validate();
        }
    }
    delete options.env;
    return 0;
}

} // namespace calicodb
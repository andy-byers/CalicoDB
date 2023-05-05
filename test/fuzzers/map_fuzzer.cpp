// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.
//
// Checks database consistency with a std::map.
//
// The std::map represents the records that are committed to the database. The
// contents of the std::map and the database should be exactly the same after
// (a) a transaction has finished, or (b) the database is reopened.

#include "map_fuzzer.h"
#include "db_impl.h"
#include <set>

namespace calicodb
{

using namespace calicodb::tools;

enum OperationType {
    kPut,
    kErase,
    kRollback,
    kCommit,
    kReopen,
    kVacuum,
    kOpCount
};

constexpr std::size_t DB_MAX_RECORDS = 5'000;

MapFuzzer::MapFuzzer(std::string path, Options *options)
    : DbFuzzer(std::move(path), options)
{
}

auto MapFuzzer::step(const U8 *&data, std::size_t &size) -> Status
{
    CHECK_TRUE(size >= 2);

    const auto expect_equal_contents = [this] {
        auto *cursor = m_table->new_cursor();
        cursor->seek_first();
        for (const auto &[key, value] : m_map) {
            CHECK_TRUE(cursor->is_valid());
            CHECK_EQ(cursor->key(), key);
            CHECK_EQ(cursor->value(), value);
            cursor->next();
        }
        CHECK_FALSE(cursor->is_valid());
        CHECK_TRUE(cursor->status().is_not_found());
        delete cursor;
    };

    const auto discard_pending = [this, &expect_equal_contents] {
        m_added.clear();
        m_erased.clear();
        expect_equal_contents();
    };

    const auto commit_pending = [this, &cleanup = discard_pending] {
        for (const auto &[k, v] : m_added) {
            m_map.insert_or_assign(k, v);
        }
        for (const auto &k : m_erased) {
            m_map.erase(k);
        }
        cleanup();
    };

    auto operation_type = static_cast<OperationType>(*data++ % OperationType::kOpCount);
    --size;

    std::string key;
    std::string value;
    Status s;

    // Limit memory used by the fuzzer.
    if (operation_type == kPut && m_map.size() + m_added.size() > m_erased.size() + DB_MAX_RECORDS) {
        operation_type = kErase;
    }

    switch (operation_type) {
        case kPut:
            key = extract_fuzzer_key(data, size);
            value = extract_fuzzer_value(data, size);
            CALICODB_TRY(m_table->put(key, value));
            if (const auto itr = m_erased.find(key); itr != end(m_erased)) {
                m_erased.erase(itr);
            }
            m_added.insert_or_assign(key, value);
            if (!m_txn) {
                commit_pending();
            }
            break;
        case kErase: {
            key = extract_fuzzer_key(data, size);
            auto *cursor = m_table->new_cursor();
            cursor->seek(key);
            if (cursor->is_valid()) {
                CALICODB_TRY(m_table->erase(cursor->key()));
                key = cursor->key().to_string();
                if (const auto itr = m_added.find(key); itr != end(m_added)) {
                    m_added.erase(itr);
                }
                m_erased.insert(key);
                if (!m_txn) {
                    commit_pending();
                }
            }
            delete cursor;
            break;
        }
            //        case kVacuum:
            //            CALICODB_TRY(m_txn->vacuum());
            //            break;
        case kRollback:
            if (m_txn) {
                m_txn->rollback();
                discard_pending();
                expect_equal_contents();
            }
            break;
        case kCommit:
            CALICODB_TRY(m_txn->commit());
            commit_pending();
            expect_equal_contents();
            break;
        default: // kReopen

            m_added.clear();
            m_erased.clear();
            CALICODB_TRY(reopen());
            expect_equal_contents();
    }
    return m_txn->status();
}

extern "C" int LLVMFuzzerTestOneInput(const U8 *data, std::size_t size)
{
    Options options;
    options.env = new tools::FakeEnv;
    options.cache_size = kPageSize * kMinFrameCount;

    {
        MapFuzzer fuzzer("map_db", &options);

        while (size > 1) {
            CHECK_OK(fuzzer.step(data, size));
            fuzzer.validate();
        }
    }

    delete options.env;
    return 0;
}

} // namespace calicodb
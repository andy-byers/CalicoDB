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
    kCheckpoint,
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
        auto *cursor = m_db->new_cursor();
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
            s = m_db->put(key, value);
            if (s.is_ok()) {
                if (const auto itr = m_erased.find(key); itr != end(m_erased)) {
                    m_erased.erase(itr);
                }
                m_added[key] = value;
            }
            reinterpret_cast<DBImpl &>(*m_db).TEST_tables().get(Id(2))->tree->TEST_validate();
            break;
        case kErase: {
            key = extract_fuzzer_key(data, size);
            auto *cursor = m_db->new_cursor();
            cursor->seek(key);
            if (cursor->is_valid()) {
                s = m_db->erase(cursor->key());
                if (s.is_ok()) {
                    key = cursor->key().to_string();
                    if (const auto itr = m_added.find(key); itr != end(m_added)) {
                        m_added.erase(itr);
                    }
                    m_erased.insert(key);
                } else if (s.is_not_found()) {
                    s = Status::ok();
                }
            }
            delete cursor;
            reinterpret_cast<DBImpl &>(*m_db).TEST_tables().get(Id(2))->tree->TEST_validate();
            break;
        }
        case kVacuum:
            s = m_db->vacuum();
            reinterpret_cast<DBImpl &>(*m_db).TEST_tables().get(Id(2))->tree->TEST_validate();
            break;
        case kCheckpoint:
            s = m_db->commit();
            if (s.is_ok()) {
                for (const auto &[k, v] : m_added) {
                    m_map[k] = v;
                }
                for (const auto &k : m_erased) {
                    m_map.erase(k);
                }
                m_added.clear();
                m_erased.clear();
                expect_equal_contents();
            }
            reinterpret_cast<DBImpl &>(*m_db).TEST_tables().get(Id(2))->tree->TEST_validate();
            break;
        default: // kReopen
            m_added.clear();
            m_erased.clear();
            s = reopen();
            if (s.is_ok()) {
                expect_equal_contents();
            }
    }
    if (!s.is_ok()) {
        m_added.clear();
        m_erased.clear();
        return s;
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
        MapFuzzer fuzzer {"map_db", &options};

        while (size > 1) {
            CHECK_OK(fuzzer.step(data, size));
            fuzzer.validate();
        }
    }

    delete options.env;
    return 0;
}

} // namespace calicodb
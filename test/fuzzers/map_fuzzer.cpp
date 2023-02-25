/*
 * map_fuzzer.cpp: Checks database consistency with a std::map.
 *
 * The std::map represents the records that are committed to the database. The contents of the std::map and the database should
 * be exactly the same after (a) a transaction has finished, or (b) the database is reopened.
 */

#include "map_fuzzer.h"

#include <map>
#include <set>

#include "calico/calico.h"

namespace Calico {

using namespace Calico::Tools;

enum OperationType {
    PUT,
    ERASE,
    COMMIT,
    REOPEN,
    VACUUM,
    TYPE_COUNT
};

constexpr Size DB_MAX_RECORDS {5'000};

MapFuzzer::MapFuzzer(std::string path, Options *options)
    : DbFuzzer {std::move(path), options}
{}

auto MapFuzzer::step(const std::uint8_t *&data, std::size_t &size) -> Status
{
    CHECK_TRUE(size >= 2);

    const auto expect_equal_contents = [this] {
        CHECK_EQ(m_map.size(), reinterpret_cast<const DatabaseImpl *>(m_db)->record_count());

        auto *cursor = m_db->new_cursor();
        cursor->seek_first();
        for (const auto &[key, value]: m_map) {
            CHECK_TRUE(cursor->is_valid());
            CHECK_EQ(cursor->key(), key);
            CHECK_EQ(cursor->value(), value);
            cursor->next();
        }
        CHECK_FALSE(cursor->is_valid());
        CHECK_TRUE(cursor->status().is_not_found());
        delete cursor;
    };

    auto operation_type = static_cast<OperationType>(*data++ % OperationType::TYPE_COUNT);
    size--;

    std::string key;
    std::string value;
    Status s;

    // Limit memory used by the fuzzer.
    if (operation_type == PUT && m_map.size() + m_added.size() > m_erased.size() + DB_MAX_RECORDS) {
        operation_type = ERASE;
    }

    switch (operation_type) {
        case PUT:
            key = extract_key(data, size).to_string();
            value = extract_value(data, size);
            s = m_db->put(key, value);
            if (s.is_ok()) {
                if (const auto itr = m_erased.find(key); itr != end(m_erased)) {
                    m_erased.erase(itr);
                }
                m_added[key] = value;
            }
            break;
        case ERASE:
            key = extract_key(data, size).to_string();
            s = m_db->erase(key);
            if (s.is_ok()) {
                if (const auto itr = m_added.find(key); itr != end(m_added)) {
                    m_added.erase(itr);
                }
                m_erased.insert(key);
            } else if (s.is_not_found()) {
                s = Status::ok();
            }
            break;
        case VACUUM:
            s = m_db->vacuum();
            break;
        case COMMIT:
            s = m_db->commit();
            if (s.is_ok()) {
                for (const auto &[k, v]: m_added) {
                    m_map[k] = v;
                }
                for (const auto &k: m_erased) {
                    m_map.erase(k);
                }
                m_added.clear();
                m_erased.clear();
                expect_equal_contents();
            }
            break;
        default: // REOPEN
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

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t *data, std::size_t size)
{
    Options options;
    options.storage = new Tools::DynamicMemory;

    {
        MapFuzzer fuzzer {"map_fuzzer", &options};

        while (size > 1) {
            CHECK_OK(fuzzer.step(data, size));
        }
    }

    delete options.storage;
    return 0;
}

} // namespace
/*
 * ops_fuzzer.cpp: Runs normal database operations.
 */

#include "ops_fuzzer.h"

namespace Calico {

enum OperationType {
    PUT,
    GET,
    ERASE,
    SEEK_ITER,
    ITER_FORWARD,
    ITER_REVERSE,
    COMMIT,
    VACUUM,
    REOPEN,
    TYPE_COUNT
};

constexpr Size DB_MAX_RECORDS {5'000};

OpsFuzzer::OpsFuzzer(std::string path, Options *options)
    : DbFuzzer {std::move(path), options}
{}

auto OpsFuzzer::step(const std::uint8_t *&data, std::size_t &size) -> Status
{
    std::string prop;
    const auto record_count = reinterpret_cast<const DatabaseImpl *>(m_db)->record_count();
    auto operation_type = static_cast<OperationType>(*data++ % OperationType::TYPE_COUNT);
    size--;

    std::string key;
    std::string value;
    Cursor *cursor;

    if (record_count > DB_MAX_RECORDS) {
        operation_type = ERASE;
    }
    switch (operation_type) {
        case GET:
            Tools::expect_non_error(m_db->get(extract_key(data, size), value));
            break;
        case PUT:
            key = extract_key(data, size).to_string();
            CHECK_OK(m_db->put(key, extract_value(data, size)));
            break;
        case ERASE:
            Tools::expect_non_error(m_db->erase(extract_key(data, size)));
            break;
        case SEEK_ITER:
            key = extract_key(data, size).to_string();
            cursor = m_db->new_cursor();
            cursor->seek(key);
            while (cursor->is_valid()) {
                if (key.front() & 1) {
                    cursor->next();
                } else {
                    cursor->previous();
                }
            }
            CHECK_TRUE(cursor->status().is_not_found());
            delete cursor;
            break;
        case ITER_FORWARD:
            cursor = m_db->new_cursor();
            cursor->seek_first();
            CHECK_EQ(cursor->is_valid(), record_count != 0);
            while (cursor->is_valid()) {
                cursor->next();
            }
            CHECK_TRUE(cursor->status().is_not_found());
            delete cursor;
            break;
        case ITER_REVERSE:
            cursor = m_db->new_cursor();
            cursor->seek_last();
            CHECK_EQ(cursor->is_valid(), record_count != 0);
            while (cursor->is_valid()) {
                cursor->previous();
            }
            CHECK_TRUE(cursor->status().is_not_found());
            delete cursor;
            break;
        case VACUUM:
            if (auto s = m_db->commit(); s.is_ok()) {
                return m_db->vacuum();
            }
            break;
        case COMMIT:
            return m_db->commit();
            break;
        default: // REOPEN
            return reopen();
    }
}

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t *data, std::size_t size)
{
    OpsFuzzer fuzzer {"__ops_fuzzer"};
    while (size > 1) {
        fuzzer.step(data, size);
    }
    return 0;
}

} // namespace
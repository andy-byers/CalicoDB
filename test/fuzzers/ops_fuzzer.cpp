/*
 * ops_fuzzer.cpp: Runs normal database operations.
 */

#include "fuzzer.h"

namespace {

using namespace Calico;

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

constexpr auto DB_PATH = "__ops_fuzzer";
constexpr Size DB_MAX_RECORDS {5'000};

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t *data, Size size)
{
    Database *db;
    CHECK_OK(Database::open(DB_PATH, DB_OPTIONS, &db));

    while (size > 1) {
        std::string prop;
        CHECK_TRUE(db->get_property("calico.counts", prop));
        const auto counts = Tools::parse_db_counts(prop);
        auto operation_type = static_cast<OperationType>(*data++ % OperationType::TYPE_COUNT);
        size--;

        std::string key;
        std::string value;
        Cursor *cursor;

        if (counts.records > DB_MAX_RECORDS) {
            operation_type = ERASE;
        }
        switch (operation_type) {
            case GET:
                Tools::expect_non_error(db->get(extract_key(data, size), value));
                break;
            case PUT:
                key = extract_key(data, size).to_string();
                CHECK_OK(db->put(key, extract_value(data, size)));
                break;
            case ERASE:
                Tools::expect_non_error(db->erase(extract_key(data, size)));
                break;
            case SEEK_ITER:
                key = extract_key(data, size).to_string();
                cursor = db->new_cursor();
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
                cursor = db->new_cursor();
                cursor->seek_first();
                CHECK_EQ(cursor->is_valid(), counts.records != 0);
                while (cursor->is_valid()) {
                    cursor->next();
                }
                CHECK_TRUE(cursor->status().is_not_found());
                delete cursor;
                break;
            case ITER_REVERSE:
                cursor = db->new_cursor();
                cursor->seek_last();
                CHECK_EQ(cursor->is_valid(), counts.records != 0);
                while (cursor->is_valid()) {
                    cursor->previous();
                }
                CHECK_TRUE(cursor->status().is_not_found());
                delete cursor;
                break;
            case VACUUM:
                CHECK_OK(db->commit());
                CHECK_OK(db->vacuum());
                break;
            case COMMIT:
                CHECK_OK(db->commit());
                break;
            default: // REOPEN
                delete db;
                CHECK_OK(Database::open(DB_PATH, DB_OPTIONS, &db));
        }
        CHECK_OK(db->status());
        Tools::validate_db(*db);
    }
    Tools::validate_db(*db);
    delete db;
    CHECK_OK(Database::destroy(DB_PATH, DB_OPTIONS));
    return 0;
}

} // namespace
/*
 * db_fuzzer.cpp: Checks the happy path.
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
    ABORT,
    REOPEN,
    TYPE_COUNT
};

constexpr auto DB_PATH = "/tmp/_db_fuzzer";

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t *data, Size size)
{
    Database *db;
    Tools::expect_ok(Database::open(DB_PATH, DB_OPTIONS, &db));

    while (size > 1) {
        const auto record_count = std::stoi(db->get_property("calico.count.records"));
        const auto operation_type = static_cast<OperationType>(*data++ % OperationType::TYPE_COUNT);
        size--;

        std::string key;
        std::string value;
        Cursor *cursor;

        switch (operation_type) {
            case GET:
                Tools::expect_non_error(db->get(extract_key(data, size), value));
                break;
            case PUT:
                key = extract_key(data, size).to_string();
                Tools::expect_ok(db->put(key, extract_value(data, size)));
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
                assert(cursor->status().is_not_found());
                delete cursor;
                break;
            case ITER_FORWARD:
                cursor = db->new_cursor();
                cursor->seek_first();
                assert(cursor->is_valid() == (record_count != 0));
                while (cursor->is_valid()) {
                    cursor->next();
                }
                assert(cursor->status().is_not_found());
                delete cursor;
                break;
            case ITER_REVERSE:
                cursor = db->new_cursor();
                cursor->seek_last();
                assert(cursor->is_valid() == (record_count != 0));
                while (cursor->is_valid()) {
                    cursor->previous();
                }
                assert(cursor->status().is_not_found());
                delete cursor;
                break;
            case COMMIT:
                Tools::expect_ok(db->commit());
                break;
            case ABORT:
                Tools::expect_ok(db->abort());
                break;
            default: // REOPEN
                delete db;
                Tools::expect_ok(Database::open(DB_PATH, DB_OPTIONS, &db));
        }
        Tools::expect_ok(db->status());
    }
    delete db;
    Tools::expect_ok(Database::destroy(DB_PATH, DB_OPTIONS));
    return 0;
}

} // namespace
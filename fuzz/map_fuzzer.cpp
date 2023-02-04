#include "fuzzer.h"
#include <map>
#include <set>
#include <calico/calico.h>

#include "../test/tools/fakes.h"
#include "../test/tools/tools.h"

namespace {

using namespace Calico;

enum OperationType {
    PUT,
    ERASE,
    COMMIT,
    ABORT,
    REOPEN,
    FAIL,
    TYPE_COUNT
};

enum FailureTarget {
    DATA_READ,
    DATA_WRITE,
    WAL_READ,
    WAL_WRITE,
    WAL_UNLINK,
    WAL_OPEN,
    TARGET_COUNT
};

static constexpr auto DB_PATH = "test";
static constexpr auto DB_DATA_PATH = "test/data";
static constexpr auto DB_WAL_PREFIX = "test/wal";

using Set = std::set<std::string>;
using Map = std::map<std::string, std::string>;

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t *data, Size size)
{
    Storage *storage;
    storage = new(std::nothrow) HeapStorage;
    assert(storage != nullptr);

    auto options = DB_OPTIONS;
    options.storage = storage;

    Database *db;
    assert_ok(Database::open(DB_PATH, options, &db));

    Set erased;
    Map added;
    Map map;

    const auto reopen_db = [&]
    {
        delete db;

        Interceptors::reset();
        assert_ok(Database::open(DB_PATH, options, &db));

        added.clear();
        erased.clear();
    };

    while (size > 1) {
        const auto operation_type = static_cast<OperationType>(*data++ % OperationType::TYPE_COUNT);
        size--;

        if (operation_type == FAIL) {
            const auto failure_target = static_cast<FailureTarget>(*data++ % FailureTarget::TARGET_COUNT);
            size--;
            
            switch (failure_target) {
                case DATA_READ:
                    Interceptors::set_read(FailOnce<0> {DB_DATA_PATH});
                    break;
                case DATA_WRITE:
                    Interceptors::set_write(FailOnce<0> {DB_DATA_PATH});
                    break;
                case WAL_READ:
                    Interceptors::set_read(FailOnce<0> {DB_WAL_PREFIX});
                    break;
                case WAL_WRITE:
                    Interceptors::set_write(FailOnce<0> {DB_WAL_PREFIX});
                    break;
                case WAL_UNLINK:
                    Interceptors::set_unlink(FailOnce<0> {DB_WAL_PREFIX});
                    break;
                default:
                    Interceptors::set_open(FailOnce<0> {DB_WAL_PREFIX});
            }
            continue;
        }
        
        std::string key;
        std::string value;

        switch (operation_type) {
            case PUT:
                key = extract_key(data, size).to_string();
                value = extract_value(data, size).to_string();
                if (db->put(key, value).is_ok()) {
                    if (const auto itr = erased.find(key); itr != end(erased)) {
                        erased.erase(itr);
                    }
                    added.emplace(key, value);
                } else {
                    reopen_db();
                }
                break;
            case ERASE:
                if (const auto s = db->erase(extract_key(data, size)); s.is_ok()) {
                    if (const auto itr = added.find(key); itr != end(added)) {
                        added.erase(itr);
                    }
                    erased.emplace(key);
                } else if (!s.is_not_found()) {
                    reopen_db();
                }
                break;
            case COMMIT:
                if (db->commit().is_ok()) {
                    map.insert(begin(added), end(added));
                    for (const auto &k: erased) {
                        map.erase(k);
                    }
                    added.clear();
                    erased.clear();
                } else {
                    reopen_db();
                }
                break;
            case ABORT:
                if (db->abort().is_ok()) {
                    added.clear();
                    erased.clear();
                } else {
                    reopen_db();
                }
                break;
            default: // REOPEN
                reopen_db();
        }
        assert_ok(db->status());
    }
    reopen_db();

    // Ensure that the database and the std::map have identical contents.
    assert(map.size() == std::stoi(db->get_property("calico.count.records")));
    auto *cursor = db->new_cursor();
    cursor->seek_first();
    for (const auto &[key, value]: map) {
        assert(cursor->is_valid());
        assert(cursor->key() == key);
        assert(cursor->value() == value);
    }
    assert(not cursor->is_valid());
    delete cursor;
    delete db;
    return 0;
}

} // namespace
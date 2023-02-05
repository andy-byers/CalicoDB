#include "fuzzer.h"
#include <map>
#include <set>
#include <calico/calico.h>

namespace {

using namespace Calico;
using namespace Calico::Tools;

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

constexpr auto DB_PATH = "test";
constexpr auto DB_DATA_PATH = "test/data";
constexpr auto DB_WAL_PREFIX = "test/wal";

using Set = std::set<std::string>;
using Map = std::map<std::string, std::string>;

auto storage_base(Storage *storage) -> DynamicMemory &
{
    return reinterpret_cast<DynamicMemory &>(*storage);
}

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t *data, Size size)
{
    auto options = DB_OPTIONS;
    options.storage = new(std::nothrow) DynamicMemory;
    assert(options.storage != nullptr);

    Database *db;
    expect_ok(Database::open(DB_PATH, options, &db));

    Set erased;
    Map added;
    Map map;

    const auto reopen_and_clear_pending = [&added, &db, &erased, options]
    {
        delete db;

        storage_base(options.storage).clear_interceptors();
        expect_ok(Database::open(DB_PATH, options, &db));

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
                    storage_base(options.storage).add_interceptor(Interceptor {DB_DATA_PATH, Interceptor::READ, [] {
                        return Status::system_error(std::string {"READ "} + DB_DATA_PATH);
                    }});
                    break;
                case DATA_WRITE:
                    storage_base(options.storage).add_interceptor(Interceptor {DB_DATA_PATH, Interceptor::WRITE, [] {
                        return Status::system_error(std::string {"WRITE "} + DB_DATA_PATH);
                    }});
                    break;
                case WAL_READ:
                    storage_base(options.storage).add_interceptor(Interceptor {DB_WAL_PREFIX, Interceptor::READ, [] {
                        return Status::system_error(std::string {"READ "} + DB_WAL_PREFIX);
                    }});
                    break;
                case WAL_WRITE:
                    storage_base(options.storage).add_interceptor(Interceptor {DB_WAL_PREFIX, Interceptor::WRITE, [] {
                        return Status::system_error(std::string {"WRITE "} + DB_WAL_PREFIX);
                    }});
                    break;
                case WAL_UNLINK:
                    storage_base(options.storage).add_interceptor(Interceptor {DB_WAL_PREFIX, Interceptor::UNLINK, [] {
                        return Status::system_error(std::string {"UNLINK "} + DB_WAL_PREFIX);
                    }});
                    break;
                default:
                    storage_base(options.storage).add_interceptor(Interceptor {DB_WAL_PREFIX, Interceptor::OPEN, [] {
                        return Status::system_error(std::string {"OPEN "} + DB_WAL_PREFIX);
                    }});
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
                    reopen_and_clear_pending();
                }
                break;
            case ERASE:
                if (const auto s = db->erase(extract_key(data, size)); s.is_ok()) {
                    if (const auto itr = added.find(key); itr != end(added)) {
                        added.erase(itr);
                    }
                    erased.emplace(key);
                } else if (!s.is_not_found()) {
                    reopen_and_clear_pending();
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
                    reopen_and_clear_pending();
                }
                break;
            case ABORT:
                if (db->abort().is_ok()) {
                    added.clear();
                    erased.clear();
                } else {
                    reopen_and_clear_pending();
                }
                break;
            default: // REOPEN
                reopen_and_clear_pending();
        }
        expect_ok(db->status());
    }
    reopen_and_clear_pending();

    // Ensure that the database and the std::map have identical contents.
    assert(map.size() == std::stoi(db->get_property("calico.count.records")));
    auto *cursor = db->new_cursor();
    cursor->seek_first();
    for (const auto &[key, value]: map) {
        assert(cursor->is_valid());
        assert(cursor->key() == key);
        assert(cursor->value() == value);
    }
    Tools::expect_non_error(cursor->status());
    delete cursor;
    delete db;
    delete options.storage;
    return 0;
}

} // namespace
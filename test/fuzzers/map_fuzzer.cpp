/*
 * map_fuzzer.cpp: Checks database consistency with a std::map. This fuzzer will inject faults, unless NO_FAILURES is defined.
 *
 * The std::map represents the records that are committed to the database. The contents of the std::map and the database should
 * be exactly the same after (a) a transaction has finished, or (b) the database is reopened.
 */

#include "fuzzer.h"

#include <map>
#include <set>

#include "calico/calico.h"

#define NO_FAILURES 0

namespace {

using namespace Calico;
using namespace Calico::Tools;

enum OperationType {
    PUT,
    ERASE,
    COMMIT,
    ABORT,
    REOPEN,
    VACUUM,
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
constexpr Size DB_MAX_RECORDS {5'000};

using Set = std::set<std::string>;
using Map = std::map<std::string, std::string>;

auto storage_base(Storage *storage) -> DynamicMemory &
{
    return reinterpret_cast<DynamicMemory &>(*storage);
}

auto handle_failure() -> void
{
#ifdef NO_FAILURES
    std::fputs("error: unexpected failure\n", stderr);
    std::abort();
#endif // NO_FAILURES
}

auto translate_op(std::uint8_t code) -> OperationType
{
    auto type = static_cast<OperationType>(code % OperationType::TYPE_COUNT);

#ifdef NO_FAILURES
    if (type == FAIL) {
        type = REOPEN;
    }
#endif // NO_FAILURES

    return type;
}

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t *data, Size size)
{
    auto options = DB_OPTIONS;
    options.storage = new(std::nothrow) DynamicMemory;
    CHECK_TRUE(options.storage != nullptr);

    Database *db;
    expect_ok(Database::open(DB_PATH, options, &db));

    Set erased;
    Map added;
    Map map;

    const auto reopen_and_clear_pending = [&added, &db, &erased, options] {
        delete db;

        storage_base(options.storage).clear_interceptors();
        expect_ok(Database::open(DB_PATH, options, &db));

        added.clear();
        erased.clear();
    };

    const auto expect_equal_sizes = [&db, &map] {
        std::string prop;
        CHECK_TRUE(db->get_property("calico.counts", prop));
        const auto counts = Tools::parse_db_counts(prop);
        CHECK_EQ(map.size(), counts.records);
    };

    const auto expect_equal_contents = [&db, &map] {
        auto *cursor = db->new_cursor();
        cursor->seek_first();
        for (const auto &[key, value]: map) {
            CHECK_TRUE(cursor->is_valid());
            CHECK_EQ(cursor->key(), key);
            CHECK_EQ(cursor->value(), value);
            cursor->next();
        }
        CHECK_FALSE(cursor->is_valid());
        CHECK_TRUE(cursor->status().is_not_found());
        delete cursor;
    };

    const auto commit = [&] {
        if (db->commit().is_ok()) {
            for (const auto &[k, v]: added) {
                map[k] = v;
            }
            for (const auto &k: erased) {
                map.erase(k);
            }
            added.clear();
            erased.clear();
        } else {
            handle_failure();
            reopen_and_clear_pending();
        }
    };

    while (size > 1) {
        auto operation_type = translate_op(*data++);
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
                default: // WAL_OPEN
                    storage_base(options.storage).add_interceptor(Interceptor {DB_WAL_PREFIX, Interceptor::OPEN, [] {
                        return Status::system_error(std::string {"OPEN "} + DB_WAL_PREFIX);
                    }});
            }
            continue;
        }

        std::string key;
        std::string value;

        // Limit memory used by the fuzzer.
        if (operation_type == PUT && map.size() + added.size() > erased.size() + DB_MAX_RECORDS) {
            operation_type = ERASE;
        }

        switch (operation_type) {
            case PUT:
                key = extract_key(data, size).to_string();
                value = extract_value(data, size);
                if (db->put(key, value).is_ok()) {
                    if (const auto itr = erased.find(key); itr != end(erased)) {
                        erased.erase(itr);
                    }
                    added[key] = value;
                } else {
                    handle_failure();
                    reopen_and_clear_pending();
                }
                break;
            case ERASE:
                key = extract_key(data, size).to_string();
                if (const auto s = db->erase(key); s.is_ok()) {
                    if (const auto itr = added.find(key); itr != end(added)) {
                        added.erase(itr);
                    }
                    erased.insert(key);
                } else if (!s.is_not_found()) {
                    handle_failure();
                    reopen_and_clear_pending();
                }
                break;
            case COMMIT:
                commit();
                expect_equal_sizes();
                break;
            case ABORT:
                if (db->abort().is_ok()) {
                    added.clear();
                    erased.clear();
                } else {
                    handle_failure();
                    reopen_and_clear_pending();
                }
                expect_equal_sizes();
                break;
            case VACUUM:
                commit();
                if (auto s = db->vacuum(); !s.is_ok()) {
                    CHECK_FALSE(s.is_logic_error());
                    handle_failure();
                    reopen_and_clear_pending();
                }
                expect_equal_sizes();
                break;
            default: // REOPEN
                reopen_and_clear_pending();
                expect_equal_contents();
        }
        if (!db->status().is_ok()) {
            handle_failure();
            reopen_and_clear_pending();
            expect_equal_contents();
        }
    }
    reopen_and_clear_pending();
    expect_equal_sizes();
    expect_equal_contents();

    delete db;
    delete options.storage;
    return 0;
}

} // namespace
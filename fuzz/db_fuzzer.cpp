
#include <filesystem>
#include <calico/calico.h>

namespace {

using namespace Calico;

enum OperationGroup {
    XACT_WITH_COMMIT,
    XACT_WITH_ABORT,
    REOPEN,
    GROUP_COUNT
};

enum OperationType {
    PUT,
    GET,
    ERASE,
    ITERATE_FORWARD_FULL,
    ITERATE_REVERSE_FULL,
    ITERATE_FORWARD_PARTIAL,
    ITERATE_REVERSE_PARTIAL,
    TYPE_COUNT
};

static constexpr auto PATH = "__db_fuzzer__";
static constexpr Size MAX_KEY_SIZE {12};
static constexpr Size MAX_VALUE_SIZE {0x200};

static auto expect_ok(const Status &s)
{
    if (!s.is_ok()) {
        std::fprintf(stderr, "error: %s\n", s.what().data());
        std::abort();
    }
}

template<class ...Set>
static auto expect_one_of_type(const Status &target, const Status &first, const Set &...rest) -> bool
{
    return expect_one_of_type(target, first) && expect_one_of_type(target, rest...);
}

static auto extract_payload(const std::uint8_t *&data, Size &size, Size max_size)
{
    if (size == 0) {
        return Slice {};
    }
    Size actual {1};

    // If possible, use the first byte to denote the payload size.
    if (size > 1) {
        const auto requested = std::min<Size>(data[0], max_size);
        actual = std::min(requested + !requested, size - 1);
        data++;
        size--;
    }
    const Slice payload {reinterpret_cast<const Byte *>(data), actual};
    data += actual;
    size -= actual;
    return payload;
}

static auto extract_key(const std::uint8_t *&data, Size &size)
{
    assert(size != 0);
    return extract_payload(data, size, MAX_KEY_SIZE);
}

static auto extract_value(const std::uint8_t *&data, Size &size)
{
    return extract_payload(data, size, MAX_VALUE_SIZE);
}

static auto iterate_forward(Cursor &cursor)
{
    while (cursor.is_valid()) {
        cursor.next();
    }
}

static auto iterate_reverse(Cursor &cursor)
{
    while (cursor.is_valid()) {
        cursor.previous();
    }
}

static auto run_operations(Database &db, const std::uint8_t *data, Size size)
{
    while (size) {
        const auto operation_type = static_cast<OperationType>(data[0] % OperationType::TYPE_COUNT);
        data++;
        size--;

        if (size == 0) {
            break;
        }

        Slice key;
        std::string value;

        const auto stat = db.statistics();

        switch (operation_type) {
            case PUT:
                key = extract_key(data, size);
                expect_ok(db.put(key, extract_value(data, size)));
                break;
            case GET: {
                const auto s = db.get(extract_key(data, size), value);
                assert(s.is_ok() || s.is_not_found());
                break;
            }
            case ITERATE_FORWARD_FULL: {
                auto cursor = db.cursor();
                cursor.seek_first();
                assert(cursor.is_valid() == (stat.record_count() != 0));
                iterate_forward(cursor);
                assert(cursor.status().is_not_found());
                break;
            }
            case ITERATE_REVERSE_FULL: {
                auto cursor = db.cursor();
                cursor.seek_last();
                assert(cursor.is_valid() == (stat.record_count() != 0));
                iterate_reverse(cursor);
                assert(cursor.status().is_not_found());
                break;
            }
            case ITERATE_FORWARD_PARTIAL: {
                auto cursor = db.cursor();
                cursor.seek(extract_key(data, size));
                assert(cursor.is_valid() || cursor.status().is_not_found());
                iterate_forward(cursor);
                assert(cursor.status().is_not_found());
                break;
            }
            case ITERATE_REVERSE_PARTIAL: {
                auto cursor = db.cursor();
                cursor.seek(extract_key(data, size));
                assert(cursor.is_valid() || cursor.status().is_not_found());
                iterate_reverse(cursor);
                assert(cursor.status().is_not_found());
                break;
            }
            default: { // ERASE
                const auto s = db.erase(extract_key(data, size));
                assert(s.is_ok() || s.is_not_found());
            }
        }
    }
}

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t *data, Size size)
{
    {
        std::error_code code;
        std::filesystem::remove_all(PATH, code);
    }

    Options options;
    options.page_size = 0x400;
    options.page_cache_size = options.page_size * 32;
    options.wal_buffer_size = options.page_size * 32;

    Database db;
    expect_ok(db.open(PATH, options));

    while (size > 3) {
        const auto group_type = static_cast<OperationGroup>(data[0] % OperationGroup::GROUP_COUNT);
        const Size group_size = static_cast<std::uint16_t>(data[1] | data[2] << 8);
        data += 3;
        size -= 3;

        if (size < group_size) {
            break;
        }

        switch (group_type) {
            case XACT_WITH_COMMIT: {
                auto xact = db.start();
                run_operations(db, data, group_size);
                expect_ok(xact.commit());
                break;
            }
            case XACT_WITH_ABORT: {
                auto xact = db.start();
                run_operations(db, data, group_size);
                expect_ok(xact.abort());
                break;
            }
            default: // REOPEN
                expect_ok(db.close());
                expect_ok(db.open(PATH, options));
        }
        expect_ok(db.status());

        data += group_size;
        size -= group_size;
    }
    expect_ok(std::move(db).destroy());
    return 0;
}

} // namespace
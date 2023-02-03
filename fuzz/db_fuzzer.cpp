#include <calico/calico.h>

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

static constexpr auto DB_PATH = "/tmp/_db_fuzzer";
static constexpr Size MAX_KEY_SIZE {12};
static constexpr Size MAX_VALUE_SIZE {0x200};
static constexpr Options DB_OPTIONS {
    0x400,
    0x400 * 32,
    0x400 * 32,
    {},
    0,
    0,
    LogLevel::OFF,
    {},
    nullptr,
};

static auto expect_ok(const Status &s)
{
    if (!s.is_ok()) {
        std::fprintf(stderr, "error: %s\n", s.what().data());
        std::abort();
    }
}

static auto expect_non_error(const Status &s)
{
    if (!s.is_ok() && !s.is_not_found()) {
        std::fprintf(stderr, "error: %s\n", s.what().data());
        std::abort();
    }
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

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t *data, Size size)
{
    Database *db;
    expect_ok(Database::open(DB_PATH, DB_OPTIONS, &db));

    while (size > 1) {
        const auto record_count = std::stoi(db->get_property("record_count"));
        const auto operation_type = static_cast<OperationType>(*data % OperationType::TYPE_COUNT);
        data++;
        size--;

        std::string key;
        std::string value;
        Cursor *cursor;

        switch (operation_type) {
            case GET:
                expect_non_error(db->get(extract_key(data, size), value));
                break;
            case PUT:
                key = extract_key(data, size).to_string();
                expect_ok(db->put(key, extract_value(data, size)));
                break;
            case ERASE:
                expect_non_error(db->erase(extract_key(data, size)));
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
                expect_ok(db->commit());
                break;
            case ABORT:
                expect_ok(db->abort());
                break;
            default: // REOPEN
                delete db;
                expect_ok(Database::open(DB_PATH, DB_OPTIONS, &db));
        }
        expect_ok(db->status());
    }
    delete db;
    expect_ok(Database::destroy(DB_PATH, DB_OPTIONS));
    return 0;
}

} // namespace
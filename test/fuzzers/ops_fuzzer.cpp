/*
 * ops_fuzzer.cpp: Runs normal database operations.
 */

#include "ops_fuzzer.h"

namespace calicodb
{

enum OperationType {
    kPut,
    kGet,
    kErase,
    kSeekIter,
    kIterForward,
    kIterReverse,
    kCheckpoint,
    kVacuum,
    kReopen,
    kOpCount
};

constexpr std::size_t DB_MAX_RECORDS {5'000};

OpsFuzzer::OpsFuzzer(std::string path, Options *options)
    : DbFuzzer {std::move(path), options}
{
}

auto OpsFuzzer::step(const std::uint8_t *&data, std::size_t &size) -> Status
{
    CHECK_TRUE(size >= 2);

    const auto record_count = reinterpret_cast<const DBImpl *>(m_db)->record_count();
    auto operation_type = static_cast<OperationType>(*data++ % OperationType::kOpCount);
    --size;

    std::unique_ptr<Cursor> cursor;
    std::string value;
    std::string key;
    Status s;

    if (record_count > DB_MAX_RECORDS) {
        operation_type = kErase;
    }
    switch (operation_type) {
    case kGet:
        s = m_db->get(extract_fuzzer_key(data, size), &value);
        if (s.is_not_found()) {
            s = Status::ok();
        }
        CDB_TRY(s);
        break;
    case kPut:
        key = extract_fuzzer_key(data, size);
        CDB_TRY(m_db->put(key, extract_fuzzer_value(data, size)));
        break;
    case kErase:
        key = extract_fuzzer_key(data, size);
        cursor.reset(m_db->new_cursor());
        cursor->seek(key);
        if (cursor->is_valid()) {
            s = m_db->erase(cursor->key());
            if (s.is_not_found()) {
                s = Status::ok();
            }
        }
        CDB_TRY(s);
        break;
    case kSeekIter:
        key = extract_fuzzer_key(data, size);
        cursor.reset(m_db->new_cursor());
        cursor->seek(key);
        while (cursor->is_valid()) {
            if (key.front() & 1) {
                cursor->next();
            } else {
                cursor->previous();
            }
        }
        break;
    case kIterForward:
        cursor.reset(m_db->new_cursor());
        cursor->seek_first();
        while (cursor->is_valid()) {
            cursor->next();
        }
        break;
    case kIterReverse:
        cursor.reset(m_db->new_cursor());
        cursor->seek_last();
        while (cursor->is_valid()) {
            cursor->previous();
        }
        break;
    case kVacuum:
        CDB_TRY(m_db->vacuum());
        break;
    case kCheckpoint:
        CDB_TRY(m_db->checkpoint());
        break;
    default: // kReopen
        CDB_TRY(reopen());
    }
    return m_db->status();
}

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t *data, std::size_t size)
{
    Options options;
    options.env = new tools::FakeEnv;
    options.page_size = kMinPageSize;
    options.cache_size = kMinPageSize * 16;

    {
        OpsFuzzer fuzzer {"ops_db", &options};

        while (size > 1) {
            CHECK_OK(fuzzer.step(data, size));
            fuzzer.validate();
        }
    }
    delete options.env;
    return 0;
}

} // namespace calicodb
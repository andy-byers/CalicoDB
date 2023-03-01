/*
 * ops_fuzzer.cpp: Runs normal database operations.
 */

#include "ops_fuzzer.h"

namespace calicodb
{

enum OperationType {
    OT_Put,
    OT_Get,
    OT_Erase,
    OT_SeekIter,
    OT_IterForward,
    OT_IterReverse,
    OT_Commit,
    OT_Vacuum,
    OT_Reopen,
    OT_OpCount
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
    auto operation_type = static_cast<OperationType>(*data++ % OperationType::OT_OpCount);
    --size;

    std::unique_ptr<Cursor> cursor;
    std::string value;
    std::string key;
    Status s;

    if (record_count > DB_MAX_RECORDS) {
        operation_type = OT_Erase;
    }
    switch (operation_type) {
    case OT_Get:
        s = m_db->get(extract_fuzzer_key(data, size), value);
        if (s.is_not_found()) {
            s = Status::ok();
        }
        CDB_TRY(s);
        break;
    case OT_Put:
        key = extract_fuzzer_key(data, size);
        CDB_TRY(m_db->put(key, extract_fuzzer_value(data, size)));
        break;
    case OT_Erase:
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
    case OT_SeekIter:
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
    case OT_IterForward:
        cursor.reset(m_db->new_cursor());
        cursor->seek_first();
        while (cursor->is_valid()) {
            cursor->next();
        }
        break;
    case OT_IterReverse:
        cursor.reset(m_db->new_cursor());
        cursor->seek_last();
        while (cursor->is_valid()) {
            cursor->previous();
        }
        break;
    case OT_Vacuum:
        CDB_TRY(m_db->vacuum());
        break;
    case OT_Commit:
        CDB_TRY(m_db->commit());
        break;
    default: // OT_Reopen
        CDB_TRY(reopen());
    }
    return m_db->status();
}

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t *data, std::size_t size)
{
    Options options;
    options.env = new tools::DynamicMemory;
    {
        OpsFuzzer fuzzer {"ops_fuzzer", &options};

        while (size > 1) {
            CHECK_OK(fuzzer.step(data, size));
            fuzzer.validate();
        }
    }
    delete options.env;
    return 0;
}

} // namespace calicodb
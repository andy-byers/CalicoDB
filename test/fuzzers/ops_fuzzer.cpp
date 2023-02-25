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
    CHECK_TRUE(size >= 2);

    const auto record_count = reinterpret_cast<const DatabaseImpl *>(m_db)->record_count();
    auto operation_type = static_cast<OperationType>(*data++ % OperationType::TYPE_COUNT);
    size--;

    std::string key;
    std::string value;
    Cursor *cursor {};
    Status s;

    if (record_count > DB_MAX_RECORDS) {
        operation_type = ERASE;
    }
    switch (operation_type) {
        case GET:
            s = m_db->get(extract_key(data, size), value);
            if (s.is_not_found()) {
                s = Status::ok();
            }
            Calico_Try(s);
            break;
        case PUT:
            key = extract_key(data, size).to_string();
            Calico_Try(m_db->put(key, extract_value(data, size)));
            break;
        case ERASE:
            s = m_db->erase(extract_key(data, size));
            if (s.is_not_found()) {
                s = Status::ok();
            }
            Calico_Try(s);
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
            break;
        case ITER_FORWARD:
            cursor = m_db->new_cursor();
            cursor->seek_first();
            while (cursor->is_valid()) {
                cursor->next();
            }
            break;
        case ITER_REVERSE:
            cursor = m_db->new_cursor();
            cursor->seek_last();
            while (cursor->is_valid()) {
                cursor->previous();
            }
            break;
        case VACUUM:
            Calico_Try(m_db->vacuum());
            break;
        case COMMIT:
            Calico_Try(m_db->commit());
            break;
        default: // REOPEN
            Calico_Try(reopen());
    }
    delete cursor;
    return m_db->status();
}

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t *data, std::size_t size)
{
    Options options;
    options.storage = new Tools::DynamicMemory;
    {
        OpsFuzzer fuzzer {"ops_fuzzer", &options};

        while (size > 1) {
            CHECK_OK(fuzzer.step(data, size));
        }
    }
    delete options.storage;
    return 0;
}

} // namespace
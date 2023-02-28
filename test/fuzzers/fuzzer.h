#ifndef CALICODB_FUZZERS_FUZZER_H
#define CALICODB_FUZZERS_FUZZER_H

#include "calicodb/calicodb.h"
#include "encoding.h"
#include "tools.h"
#include <iostream>

namespace calicodb
{

inline auto extract_key(const std::uint8_t *&data, std::size_t &size)
{
    CHECK_TRUE(size != 0);
    if (size == 1) {
        return Slice {reinterpret_cast<const char *>(data), size};
    }
    std::size_t actual {2};
    if (size > 2) {
        const auto requested = std::min<std::size_t>(data[0] << 8 | data[1], MAXIMUM_PAGE_SIZE);
        actual = std::min(requested + !requested, size - 2);
        data += 2;
        size -= 2;
    }
    const Slice payload {reinterpret_cast<const char *>(data), actual};
    data += actual;
    size -= actual;
    return payload;
}

inline auto extract_value(const std::uint8_t *&data, std::size_t &size)
{
    // Allow zero-length values.
    if (size == 0) {
        return std::string {};
    }
    const auto needed_size = std::min<std::size_t>(size, 2);
    std::size_t result_size;

    if (needed_size == 1) {
        result_size = data[0];
    } else {
        result_size = data[0] << 8 | data[1];
    }
    result_size %= MAXIMUM_PAGE_SIZE;
    data += needed_size;
    size -= needed_size;

    std::string result(result_size, '_');
    if (result_size) {
        result.append(std::to_string(result_size));
    }
    return result;
}

class DbFuzzer
{
public:
    virtual ~DbFuzzer();
    explicit DbFuzzer(std::string path, Options *options = nullptr);
    [[nodiscard]] virtual auto step(const std::uint8_t *&data, std::size_t &size) -> Status = 0;
    [[nodiscard]] virtual auto reopen() -> Status;
    virtual auto validate() -> void;

protected:
    std::string m_path;
    Options m_options;
    DB *m_db {};
};

} // namespace calicodb

#endif // CALICODB_FUZZERS_FUZZER_H

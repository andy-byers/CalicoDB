#ifndef CALICODB_FUZZERS_FUZZER_H
#define CALICODB_FUZZERS_FUZZER_H

#include "calicodb/calicodb.h"
#include "encoding.h"
#include "logging.h"
#include "tools.h"
#include <iostream>

namespace calicodb
{

inline auto extract_fuzzer_value(const std::uint8_t *&data, std::size_t &size) -> std::string
{
    static constexpr auto max_fuzzer_value_size = MINIMUM_PAGE_SIZE * 2;

    const auto extract = [&data, &size] {
        std::size_t result {};
        if (size == 1) {
            result = data[0];
            ++data;
            --size;
        } else if (size >= 2) {
            result = data[0] << 8 | data[1];
            data += 2;
            size -= 2;
        }
        result %= max_fuzzer_value_size;
        return result + !result;
    };

    if (size == 0) {
        return "";
    }
    const auto result_size = extract();
    const auto result_data = extract();

    std::string result;
    if (result_size) {
        append_number(result, result_data);
        result.append(std::string(result_size, '0'));
    }
    return result;
}

inline auto extract_fuzzer_key(const std::uint8_t *&data, std::size_t &size) -> std::string
{
    if (size == 0) {
        return "0";
    }
    return extract_fuzzer_value(data, size);
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

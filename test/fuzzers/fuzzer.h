// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_FUZZERS_FUZZER_H
#define CALICODB_FUZZERS_FUZZER_H

#include "calicodb/db.h"
#include "encoding.h"
#include "logging.h"
#include "tools.h"
#include <iostream>

namespace calicodb
{

inline auto extract_fuzzer_value(const std::uint8_t *&data, std::size_t &size) -> std::string
{
    static constexpr auto max_fuzzer_value_size = kMinPageSize * 2;

    const auto extract = [&data, &size] {
        std::size_t result = 0;
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
    DB *m_db = nullptr;
};

} // namespace calicodb

#endif // CALICODB_FUZZERS_FUZZER_H

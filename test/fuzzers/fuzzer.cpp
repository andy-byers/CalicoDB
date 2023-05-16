// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "fuzzer.h"

namespace calicodb
{

auto extract_fuzzer_value(const U8 *&data_ptr, std::size_t &size_ref) -> std::string
{
    static constexpr auto kMaxValueSize = kPageSize * 2;

    const auto extract = [&data_ptr, &size_ref] {
        std::size_t result = 0;
        if (size_ref == 1) {
            result = data_ptr[0];
            ++data_ptr;
            --size_ref;
        } else if (size_ref >= 2) {
            result = data_ptr[0] << 8 | data_ptr[1];
            data_ptr += 2;
            size_ref -= 2;
        }
        result %= kMaxValueSize;
        return result + !result;
    };

    if (size_ref == 0) {
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

auto extract_fuzzer_key(const U8 *&data_ptr, std::size_t &size_ref) -> std::string
{
    if (size_ref) {
        return extract_fuzzer_value(data_ptr, size_ref);
    }
    return "0";
}

} // namespace calicodb

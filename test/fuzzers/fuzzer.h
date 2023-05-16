// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_FUZZERS_FUZZER_H
#define CALICODB_FUZZERS_FUZZER_H

#include "tools.h"

namespace calicodb
{

#ifndef FUZZER_DATA_NAME
#define FUZZER_DATA_NAME data_ptr
#endif // FUZZER_DATA_NAME
#ifndef FUZZER_SIZE_NAME
#define FUZZER_SIZE_NAME size_ref
#endif // FUZZER_SIZE_NAME

#define FUZZER_KEY extract_fuzzer_key(FUZZER_DATA_NAME, FUZZER_SIZE_NAME)
#define FUZZER_VAL extract_fuzzer_value(FUZZER_DATA_NAME, FUZZER_SIZE_NAME)

auto extract_fuzzer_key(const U8 *&data, std::size_t &size) -> std::string;
auto extract_fuzzer_value(const U8 *&data, std::size_t &size) -> std::string;

} // namespace calicodb

#endif // CALICODB_FUZZERS_FUZZER_H

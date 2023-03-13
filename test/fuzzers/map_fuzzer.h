// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_FUZZERS_MAP_FUZZER_H
#define CALICODB_FUZZERS_MAP_FUZZER_H

#include "fuzzer.h"
#include <map>
#include <set>

namespace calicodb
{

class MapFuzzer : public DbFuzzer
{
public:
    ~MapFuzzer() override = default;
    explicit MapFuzzer(std::string path, Options *options = nullptr);
    auto step(const std::uint8_t *&data, std::size_t &size) -> Status override;

private:
    std::map<std::string, std::string> m_map;
    std::map<std::string, std::string> m_added;
    std::set<std::string> m_erased;
};

} // namespace calicodb

#endif // CALICODB_FUZZERS_MAP_FUZZER_H

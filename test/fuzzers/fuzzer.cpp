// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for contributor names.

#include "fuzzer.h"

namespace calicodb
{

DbFuzzer::DbFuzzer(std::string path, Options *options)
    : m_path {std::move(path)}
{
    if (options != nullptr) {
        m_options = *options;
    }
    CHECK_OK(DB::open(m_options, m_path, &m_db));
}

DbFuzzer::~DbFuzzer()
{
    tools::validate_db(*m_db);

    delete m_db;

    CHECK_OK(DB::destroy(m_options, m_path));
}

auto DbFuzzer::reopen() -> Status
{
    delete m_db;
    m_db = nullptr;

    return DB::open(m_options, m_path, &m_db);
}

auto DbFuzzer::validate() -> void
{
    tools::validate_db(*m_db);
}

} // namespace calicodb
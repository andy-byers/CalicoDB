// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "fuzzer.h"

namespace calicodb
{

DbFuzzer::DbFuzzer(std::string path, Options *options)
    : m_path(std::move(path))
{
    if (options != nullptr) {
        m_options = *options;
    }
    CHECK_OK(reopen_impl());
    // Commit creation of the table so rollback() doesn't cause it to get invalidated.
    // Normally, this problem is mitigated by using the view()/update() API.
    CHECK_OK(m_txn->commit());
}

DbFuzzer::~DbFuzzer()
{
    CHECK_TRUE(reinterpret_cast<const DBImpl &>(*m_db).TEST_pager().assert_state());

    delete m_table;
    delete m_txn;
    delete m_db;

    CHECK_OK(DB::destroy(m_options, m_path));
}

auto DbFuzzer::reopen_impl() -> Status
{
    delete m_table;
    delete m_txn;
    delete m_db;

    m_table = nullptr;
    m_txn = nullptr;
    m_db = nullptr;

    CALICODB_TRY(DB::open(m_options, m_path, m_db));
    CALICODB_TRY(m_db->new_txn(true, m_txn));
    return m_txn->new_table(TableOptions(), "default", m_table);
}

auto DbFuzzer::reopen() -> Status
{
    return reopen_impl();
}

auto DbFuzzer::validate() -> void
{
    CHECK_TRUE(reinterpret_cast<const DBImpl &>(*m_db).TEST_pager().assert_state());
}

} // namespace calicodb
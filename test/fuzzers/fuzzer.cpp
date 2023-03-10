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
    CHECK_OK(m_db->create_table({}, "fuzz", &m_table));
}

DbFuzzer::~DbFuzzer()
{
    tools::validate_db(*m_db);

    delete m_table;
    delete m_db;

    CHECK_OK(DB::destroy(m_options, m_path));
}

auto DbFuzzer::reopen() -> Status
{
    delete m_table;
    m_table = nullptr;

    delete m_db;
    m_db = nullptr;

    auto s = DB::open(m_options, m_path, &m_db);
    if (s.is_ok()) {
        s = m_db->create_table({}, "fuzz", &m_table);
        if (s.is_ok()) {
            validate();
        }
    }
    return s;
}

auto DbFuzzer::validate() -> void
{
    tools::validate_db(*m_db);
}

} // namespace calicodb
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

    auto s = DB::open(m_options, m_path, &m_db);
    if (s.is_ok()) {
        tools::validate_db(*m_db);
    }
    return s;
}

auto DbFuzzer::validate() -> void
{
    tools::validate_db(*m_db);
}

} // namespace calicodb
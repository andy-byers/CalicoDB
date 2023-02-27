#include "fuzzer.h"

namespace Calico {

DbFuzzer::DbFuzzer(std::string path, Options *options)
    : m_path {std::move(path)}
{
    if (options != nullptr) {
        m_options = *options;
    }
    CHECK_OK(Database::open(m_path, m_options, &m_db));
}

DbFuzzer::~DbFuzzer()
{
    Tools::validate_db(*m_db);

    delete m_db;

    CHECK_OK(Database::destroy(m_path, m_options));
}

auto DbFuzzer::reopen() -> Status
{
    delete m_db;
    m_db = nullptr;

    auto s = Database::open(m_path, m_options, &m_db);
    if (s.is_ok()) {
        Tools::validate_db(*m_db);
    }
    return s;
}

auto DbFuzzer::validate() -> void
{
    Tools::validate_db(*m_db);
}

} // namespace Calico
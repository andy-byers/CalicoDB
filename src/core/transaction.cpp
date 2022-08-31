
#include "calico/transaction.h"
#include "utils/expect.h"
#include "utils/logging.h"
#include "core.h"

namespace calico {

static auto already_completed_error(const std::string &action) -> Status
{
    ThreePartMessage message;
    message.set_primary("cannot {} transaction", action);
    message.set_detail("transaction is already finished");
    message.set_hint("start a new transaction and try again");
    return message.logic_error();
}

Transaction::~Transaction()
{
    if (m_core) (void)m_core->abort();
}

Transaction::Transaction(Core &core)
    : m_core {&core}
{}

Transaction::Transaction(Transaction &&rhs) noexcept
    : m_core {std::exchange(rhs.m_core, nullptr)}
{}

auto Transaction::operator=(Transaction &&rhs) noexcept -> Transaction&
{
    if (this != &rhs)
        m_core = std::exchange(rhs.m_core, nullptr);
    return *this;
}

auto Transaction::commit() -> Status
{
    if (!m_core) return already_completed_error("commit");
    auto s = m_core->commit();
    if (s.is_ok()) m_core = nullptr;
    return s;
}

auto Transaction::abort() -> Status
{
    if (!m_core) return already_completed_error("abort");
    auto s = m_core->abort();
    if (s.is_ok()) m_core = nullptr;
    return s;
}

} // namespace calico
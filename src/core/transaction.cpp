#include "calico/transaction.h"
#include "database_impl.h"
#include "utils/expect.h"
#include "utils/system.h"

namespace Calico {

static auto already_completed_error(const std::string &action) -> Status
{
    return logic_error(
        "cannot {} transaction: transaction is already completed (start a new transaction and try again)", action);
}

Transaction::~Transaction()
{
    // If m_core is not nullptr, then we must have failed during a commit. Try to
    // fix the database by rolling back this transaction.
    if (m_impl) {
        (void)m_impl->abort();
    }
}

Transaction::Transaction(DatabaseImpl &core)
    : m_impl {&core}
{}

Transaction::Transaction(Transaction &&rhs) noexcept
    : m_impl {std::exchange(rhs.m_impl, nullptr)}
{}

auto Transaction::operator=(Transaction &&rhs) noexcept -> Transaction &
{
    if (this != &rhs) {
        m_impl = std::exchange(rhs.m_impl, nullptr);
    }
    return *this;
}

auto Transaction::commit() -> Status
{
    if (!m_impl) {
        return already_completed_error("commit");
    }
    return std::exchange(m_impl, nullptr)->commit();
}

auto Transaction::abort() -> Status
{
    if (!m_impl) {
        return already_completed_error("abort");
    }
    return std::exchange(m_impl, nullptr)->abort();
}

} // namespace Calico
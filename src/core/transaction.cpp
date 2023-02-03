#include "calico/transaction.h"
#include "database_impl.h"
#include "utils/expect.h"
#include "utils/system.h"

namespace Calico {

[[nodiscard]]
static auto already_completed_error(const std::string &action) -> Status
{
    return logic_error(
        "cannot {} transaction: transaction is already completed (start a new transaction and try again)", action);
}

Transaction::Transaction(DatabaseImpl &core)
    : m_impl {&core}
{}

Transaction::~Transaction()
{
    (void)abort();
}

auto Transaction::commit() -> Status
{
    if (auto ptr = std::exchange(m_impl, nullptr)) {
        return ptr->commit();
    }
    return already_completed_error("commit");
}

auto Transaction::abort() -> Status
{
    if (auto ptr = std::exchange(m_impl, nullptr)) {
        return ptr->abort();
    }
    return already_completed_error("abort");
}

} // namespace Calico
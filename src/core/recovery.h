#ifndef CALICO_CORE_TRANSACTION_LOG_H
#define CALICO_CORE_TRANSACTION_LOG_H

#include "calico/slice.h"
#include "utils/encoding.h"
#include "utils/system.h"
#include "utils/types.h"
#include "wal/wal.h"
#include <optional>
#include <variant>
#include <vector>

namespace Calico {

class Pager;
class WriteAheadLog;

class Recovery {
public:
    Recovery(Pager &pager, WriteAheadLog &wal, System &system)
        : m_pager {&pager},
          m_wal {&wal},
          m_system {&system},
          m_log {system.create_log("recovery")}
    {}

    [[nodiscard]] auto start_abort() -> Status;
    [[nodiscard]] auto finish_abort() -> Status;
    [[nodiscard]] auto start_recovery() -> Status;
    [[nodiscard]] auto finish_recovery() -> Status;

private:
    Pager *m_pager {};
    WriteAheadLog *m_wal {};
    System *m_system {};
    LogPtr m_log;
};

} // namespace Calico

#endif // CALICO_CORE_TRANSACTION_LOG_H


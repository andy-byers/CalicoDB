#ifndef CALICO_CORE_TRANSACTION_LOG_H
#define CALICO_CORE_TRANSACTION_LOG_H

#include "wal/wal.h"
#include <optional>
#include <variant>
#include <vector>

namespace Calico {

class Pager;
class WriteAheadLog;

class Recovery {
public:
    System *system {};

    Recovery(Pager &pager, WriteAheadLog &wal, System &sys)
        : system {&sys},
          m_pager {&pager},
          m_wal {&wal},
          m_log {sys.create_log("recovery")}
    {}

    [[nodiscard]] auto start_abort() -> Status;
    [[nodiscard]] auto finish_abort() -> Status;
    [[nodiscard]] auto start_recovery() -> Status;
    [[nodiscard]] auto finish_recovery() -> Status;

private:
    Pager *m_pager {};
    WriteAheadLog *m_wal {};
    LogPtr m_log;
};

} // namespace Calico

#endif // CALICO_CORE_TRANSACTION_LOG_H


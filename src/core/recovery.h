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

    Recovery(Pager &pager, WriteAheadLog &wal, System &sys, Lsn &commit_lsn)
        : system {&sys},
          m_pager {&pager},
          m_wal {&wal},
          m_commit_lsn {&commit_lsn},
          m_log {sys.create_log("recovery")}
    {}

    [[nodiscard]] auto start_abort() -> Status;
    [[nodiscard]] auto finish_abort() -> Status;
    [[nodiscard]] auto start_recovery() -> Status;
    [[nodiscard]] auto finish_recovery() -> Status;

private:
    Pager *m_pager {};
    WriteAheadLog *m_wal {};
    Lsn *m_commit_lsn {};
    LogPtr m_log;
};

} // namespace Calico

#endif // CALICO_CORE_TRANSACTION_LOG_H


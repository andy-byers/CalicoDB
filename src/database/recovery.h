#ifndef CALICO_DATABASE_RECOVERY_H
#define CALICO_DATABASE_RECOVERY_H

#include "wal/wal.h"
#include <optional>
#include <variant>
#include <vector>

namespace Calico {

class Pager;
class WriteAheadLog;

class Recovery {
public:
    Recovery(Pager &pager, WriteAheadLog &wal, Lsn &commit_lsn)
        : m_pager {&pager},
          m_wal {&wal},
          m_commit_lsn {&commit_lsn}
    {}

    [[nodiscard]] auto start_abort() -> Status;
    [[nodiscard]] auto finish_abort() -> Status;
    [[nodiscard]] auto start_recovery() -> Status;
    [[nodiscard]] auto finish_recovery() -> Status;

private:
    Pager *m_pager {};
    WriteAheadLog *m_wal {};
    Lsn *m_commit_lsn {};
};

} // namespace Calico

#endif // CALICO_DATABASE_RECOVERY_H


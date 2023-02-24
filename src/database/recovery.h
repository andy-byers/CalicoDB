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
    explicit Recovery(Pager &pager, WriteAheadLog &wal, Lsn &commit_lsn);
    [[nodiscard]] auto recover() -> Status;

private:
    [[nodiscard]] auto open_reader(Id segment, std::unique_ptr<Reader> &out) -> Status;
    [[nodiscard]] auto recover_phase_1() -> Status;
    [[nodiscard]] auto recover_phase_2() -> Status;

    std::string m_reader_data;
    std::string m_reader_tail;
    Pager *m_pager {};
    Storage *m_storage {};
    WalSet *m_set {};
    WriteAheadLog *m_wal {};
    Lsn *m_commit_lsn {};
};

} // namespace Calico

#endif // CALICO_DATABASE_RECOVERY_H

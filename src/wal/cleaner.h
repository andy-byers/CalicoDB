#ifndef CALICO_WAL_CLEANER_H
#define CALICO_WAL_CLEANER_H

#include "helpers.h"
#include "utils/worker.h"
#include <thread>

namespace Calico {

class WalCleanupTask {
public:
    struct Parameters {
        Slice prefix;
        Storage *storage {};
        System *system {};
        WalSet *set {};
        Size capacity {};
    };

    explicit WalCleanupTask(const Parameters &param)
        : m_work {param.capacity},
          m_prefix {param.prefix.to_string()},
          m_storage {param.storage},
          m_system {param.system},
          m_set {param.set}
    {
        CALICO_EXPECT_FALSE(m_prefix.empty());
        CALICO_EXPECT_NE(m_storage, nullptr);
        CALICO_EXPECT_NE(m_system, nullptr);
        CALICO_EXPECT_NE(m_set, nullptr);
    }

    auto remove_before(Id lsn) -> void
    {
        m_work.enqueue(lsn);
    }

    auto operator()() -> void;

private:
    Queue<Id> m_work;
    std::string m_prefix;
    Storage *m_storage {};
    System *m_system {};
    WalSet *m_set {};
};

} // namespace Calico

#endif // CALICO_WAL_CLEANER_H

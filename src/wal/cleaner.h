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
        std::atomic<Id> *limit {};
        Storage *storage {};
        System *system {};
        WalSet *set {};
    };

    explicit WalCleanupTask(const Parameters &param)
        : m_prefix {param.prefix.to_string()},
          m_limit {param.limit},
          m_storage {param.storage},
          m_system {param.system},
          m_set {param.set}
    {
        CALICO_EXPECT_FALSE(m_prefix.empty());
        CALICO_EXPECT_NE(m_storage, nullptr);
        CALICO_EXPECT_NE(m_system, nullptr);
        CALICO_EXPECT_NE(m_set, nullptr);
    }

    auto cleanup() -> void;

private:
    std::string m_prefix;
    std::atomic<Id> *m_limit {};
    Storage *m_storage {};
    System *m_system {};
    WalSet *m_set {};
};

} // namespace Calico

#endif // CALICO_WAL_CLEANER_H

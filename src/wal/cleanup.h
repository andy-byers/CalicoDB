#ifndef CALICO_WAL_CLEANER_H
#define CALICO_WAL_CLEANER_H

#include "helpers.h"
#include "utils/worker.h"
#include <thread>

namespace Calico {

class WalCleanup {
public:
    struct Parameters {
        Slice prefix;
        std::atomic<Id> *limit {};
        Storage *storage {};
        System *system {};
        WalSet *set {};
    };

    explicit WalCleanup(const Parameters &param);

    /*
     * Delete the oldest WAL segment file if it has become obsolete.
     */
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

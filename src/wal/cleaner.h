#ifndef CALICO_WAL_CLEANER_H
#define CALICO_WAL_CLEANER_H

#include "helpers.h"
#include "utils/worker.h"
#include <thread>

namespace calico {

/*
 * WAL component that handles cleanup of obsolete segment files in the background.
 */
class WalCleaner {
public:
    static constexpr Size WORKER_CAPACITY {16};

    WalCleaner(Storage &store, std::string prefix, WalCollection &collection)
        : m_worker {WORKER_CAPACITY, [this](const auto &event) {
              return on_event(event);
          }},
          m_prefix {std::move(prefix)},
          m_store {&store},
          m_collection {&collection}
    {}

    [[nodiscard]]
    auto status() const -> Status
    {
        return m_worker.status();
    }

    auto remove_before(SequenceId lsn, bool should_wait = false) -> void
    {
        m_worker.dispatch(lsn, should_wait);
    }

    [[nodiscard]]
    auto destroy() && -> Status
    {
        return std::move(m_worker).destroy();
    }

private:
    [[nodiscard]] auto on_event(const SequenceId &limit) -> Status;

    Worker<SequenceId> m_worker;
    std::string m_prefix;
    Storage *m_store {};
    WalCollection *m_collection {};
};

} // namespace calico

#endif // CALICO_WAL_CLEANER_H

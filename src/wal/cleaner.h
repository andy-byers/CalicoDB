#ifndef CALICO_WAL_CLEANER_H
#define CALICO_WAL_CLEANER_H

#include "helpers.h"
#include "utils/worker.h"
#include <thread>

namespace Calico {

/*
 * WAL component that handles cleanup of obsolete segment files in the background.
 */
class WalCleaner {
public:
    WalCleaner(Storage &store, std::string prefix, WalSet &set)
        : m_worker {16, [this](const auto &event) {
              return on_event(event);
          }},
          m_prefix {std::move(prefix)},
          m_store {&store},
          m_set {&set}
    {}

    [[nodiscard]]
    auto status() const -> Status
    {
        return m_worker.status();
    }

    auto remove_before(Id lsn, bool should_wait = false) -> void
    {
        m_worker.dispatch(lsn, should_wait);
    }

    [[nodiscard]]
    auto destroy() && -> Status
    {
        return std::move(m_worker).destroy();
    }

private:
    [[nodiscard]] auto on_event(const Id &limit) -> Status;

    Worker<Id> m_worker;
    std::string m_prefix;
    Storage *m_store {};
    WalSet *m_set {};
};

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

    auto operator()() -> void
    {
        const auto limit = m_work.try_dequeue();

        if (!limit.has_value())
            return;

        const auto first = m_set->first();
        if (first.is_null() || first <= *limit)
            return;

        const auto second = m_set->id_after(first);
        if (second.is_null() || second < *limit)
            return;

        auto s = m_storage->remove_file(m_prefix + first.to_name());
        if (s.is_ok()) {
            m_set->remove_before(second);
        } else {
            CALICO_ERROR(s);
        }
    }

private:
    Queue<Id> m_work;
    std::string m_prefix;
    Storage *m_storage {};
    System *m_system {};
    WalSet *m_set {};
};

} // namespace Calico

#endif // CALICO_WAL_CLEANER_H

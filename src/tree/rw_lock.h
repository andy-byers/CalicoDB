
#ifndef CUB_TREE_RW_LOCK_H
#define CUB_TREE_RW_LOCK_H

#include <condition_variable>
#include <mutex>

#include "common.h"
#include "utils/types.h"

namespace cub {

class RWLock {
public:
    struct RToken {
        ~RToken()
        {
            if (parent.value)
                parent.value->do_r_unlock();
        }

        RToken(RToken&&) = default;
        auto operator=(RToken&&) -> RToken& = default;

    private:
        friend class RWLock;

        explicit RToken(RWLock *p)
            : parent {p} {}

        Unique<RWLock*> parent;
    };

    struct WToken {
        ~WToken()
        {
            if (parent.value)
                parent.value->do_w_unlock();
        }

        WToken(WToken&&) = default;
        auto operator=(WToken&&) -> WToken& = default;

    private:
        friend class RWLock;

        explicit WToken(RWLock *p)
            : parent {p} {}

        Unique<RWLock*> parent;
    };

    auto r_lock() -> RToken
    {
        std::unique_lock lock {m_mutex};
        m_cond.wait(lock, [this] {return !m_has_writer;});
        m_reader_count++;
        return RToken {this};
    }

    auto w_lock() -> WToken
    {
        std::unique_lock lock {m_mutex};
        m_cond.wait(lock, [this] {return !m_has_writer;});
        m_has_writer = true;
        m_cond.wait(lock, [this] {return m_reader_count == 0;});
        return WToken {this};
    }

private:
    auto do_r_unlock() -> void
    {
        std::lock_guard lock {m_mutex};
        if (--m_reader_count == 0)
            m_cond.notify_all();
    }

    auto do_w_unlock() -> void
    {
        std::lock_guard lock {m_mutex};
        m_has_writer = false;
        m_cond.notify_all();
    }

    std::mutex m_mutex;
    std::condition_variable m_cond;
    size_t m_reader_count {};
    bool m_has_writer {};
};

} // cub

#endif // CUB_TREE_RW_LOCK_H

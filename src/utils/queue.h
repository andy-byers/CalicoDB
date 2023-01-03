/*
 * Based off of RocksDB's WorkQueue class.
 */
#ifndef CALICO_UTILS_QUEUE_H
#define CALICO_UTILS_QUEUE_H

#include <chrono>
#include <condition_variable>
#include <deque>
#include <mutex>
#include <optional>
#include "calico/common.h"

namespace Calico {

template<class T>
class Queue final {
public:
    explicit Queue(Size capacity = 0)
        : m_capacity {capacity}
    {}

    template<class U>
    auto enqueue(U &&u) -> bool
    {
        std::unique_lock lock {m_mu};
        m_full_cv.wait(lock, [this] {
            return !is_full() || m_is_finished;
        });
        if (m_is_finished) return false;
        m_queue.push_back(std::forward<U>(u));
        lock.unlock();

        m_empty_cv.notify_one();
        return true;
    }

    [[nodiscard]]
    auto dequeue() -> std::optional<T>
    {
        std::unique_lock lock {m_mu};
        m_empty_cv.wait(lock, [this] {
            return !m_queue.empty() || m_is_finished;
        });
        if (m_queue.empty()) {
            CALICO_EXPECT_TRUE(m_is_finished);
            return std::nullopt;
        }
        auto t = std::move(m_queue.front());
        m_queue.pop_front();
        lock.unlock();

        m_full_cv.notify_one();
        return t;
    }

    auto finish() -> void
    {
        {
            std::lock_guard lock {m_mu};
            m_is_finished = true;
        }
        m_empty_cv.notify_all();
        m_full_cv.notify_all();
    }

private:
    [[nodiscard]]
    auto is_full() const -> bool
    {
        if (m_capacity)
            return m_queue.size() >= m_capacity;
        return false;
    }

    std::condition_variable m_empty_cv;
    std::condition_variable m_full_cv;
    mutable std::mutex m_mu;
    std::deque<T> m_queue;
    Size m_capacity {};
    bool m_is_finished {};
};

} // namespace Calico

#endif // CALICO_UTILS_QUEUE_H

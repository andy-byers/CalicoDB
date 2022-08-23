#ifndef CALICO_UTILS_QUEUE_H
#define CALICO_UTILS_QUEUE_H

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <optional>
#include <vector>
#include "calico/common.h"

namespace calico {

/**
 * A simple queue with internal synchronization.
 *
 * Modified from RocksDB's WorkQueue class.
 *
 * @tparam T Must be CopyConstructible!
 */
template<class T>
class Queue final {
public:
    Queue() = default;
    ~Queue() = default;

    auto enqueue(T &&t) -> void
    {
        std::unique_lock lock {m_mu};
        m_queue.push_back(std::forward<T>(t));
        m_empty_cv.notify_one();
        m_is_finished = false;
    }

    [[nodiscard]]
    auto dequeue() -> std::optional<T>
    {
        std::unique_lock lock {m_mu};
        m_empty_cv.wait(lock, [this] {
            return !m_queue.empty() && !m_is_finished;
        });
        if (m_queue.empty()) {
            CALICO_EXPECT_TRUE(m_is_finished);
            return std::nullopt;
        }
        auto t = std::move(m_queue.front());
        m_queue.pop_front();
        return t;
    }

    [[nodiscard]]
    auto peek() const -> std::optional<T>
    {
        std::lock_guard lock {m_mu};
        if (m_queue.empty())
            return std::nullopt;
        return m_queue.front();
    }

    auto finish() -> void
    {
        {
            std::lock_guard lock {m_mu};
            m_is_finished = true;
        }
        m_empty_cv.notify_all();
    }

    auto wait_until_finish() -> void
    {
        std::unique_lock lock {m_mu};
        m_finish_cv.wait(lock, [this] {
            return m_is_finished;
        });
    }

private:
    std::condition_variable m_empty_cv;
    std::condition_variable m_finish_cv;
    mutable std::mutex m_mu;
    std::deque<T> m_queue;
    bool m_is_finished {};
};

} // namespace calico

#endif // CALICO_UTILS_QUEUE_H

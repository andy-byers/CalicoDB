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
 *
 * @tparam T Must be CopyConstructible!
 */
template<class T>
class Queue {
public:

    [[nodiscard]]
    auto is_empty() const -> bool
    {
        std::lock_guard lock {m_mutex};
        return size() == 0;
    }

    [[nodiscard]]
    auto size() const -> Size
    {
        std::lock_guard lock {m_mutex};
        return m_queue.size();
    }

    [[nodiscard]]
    auto to_vector() const -> std::vector<T>
    {
        std::vector<T> result;
        std::lock_guard lock {m_mutex};
        result.reserve(m_queue.size());
        std::copy(cbegin(m_queue), cend(m_queue), back_inserter(result));
        return result;
    }

    auto clear() -> void
    {
        // Careful, this won't wake up any waiting threads.
        std::lock_guard lock {m_mutex};
        m_queue.clear();
    }

    auto enqueue(T t) -> void
    {
        std::lock_guard lock {m_mutex};
        m_queue.push_back(t);
        m_cond.notify_one();
    }

    [[nodiscard]]
    auto dequeue() -> T
    {
        std::unique_lock lock {m_mutex};
        m_cond.wait(lock, &Queue<T>::wait_until);
        auto t = m_queue.front();
        m_queue.pop_front();
        return t;
    }

    [[nodiscard]]
    auto peek() const -> std::optional<T>
    {
        std::lock_guard lock {m_mutex};
        return m_queue.empty() ? std::nullopt : std::optional {m_queue.front()};
    }

    [[nodiscard]]
    auto try_dequeue(std::chrono::microseconds micros) -> std::optional<T>
    {
        std::unique_lock lock {m_mutex};
        if (m_cond.wait_for(lock, micros, &Queue<T>::wait_until)) {
            auto t = std::move(m_queue.front());
            m_queue.pop_front();
            return t;
        }
        return std::nullopt;
    }

private:
    [[nodiscard]]
    auto wait_until() const -> bool
    {
        return !m_queue.empty();
    }

    std::vector<T> m_queue;
    mutable std::mutex m_mutex;
    std::condition_variable m_cond;
};

} // namespace cco

#endif // CALICO_UTILS_QUEUE_H

#ifndef CCO_UTILS_QUEUE_H
#define CCO_UTILS_QUEUE_H

#include <condition_variable>
#include <mutex>
#include <queue>
#include "calico/common.h"

namespace cco {

template<class T>
class Queue {
public:

    [[nodiscard]]
    auto is_empty() const -> bool
    {
        return size() == 0;
    }

    [[nodiscard]]
    auto size() const -> Size
    {
        return m_queue.size();
    }

    auto enqueue(T &&t) -> void
    {
        std::lock_guard lock {m_mutex};
        m_queue.push(std::forward<T>(t));
        m_cond.notify_one();
    }

    [[nodiscard]]
    auto dequeue() -> T
    {
        std::unique_lock lock {m_mutex};
        m_cond.wait(lock, [this]() {return !is_empty();});
        auto t = std::move(m_queue.top());
        m_queue.pop();
        return t;
    }

private:
    std::queue<T> m_queue;
    mutable std::mutex m_mutex;
    std::condition_variable m_cond;
};

} // namespace cco

#endif // CCO_UTILS_QUEUE_H

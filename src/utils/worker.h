#ifndef CALICO_UTILS_WORKER_H
#define CALICO_UTILS_WORKER_H

#include "utils/queue.h"
#include "wal/helpers.h"
#include "wal/wal.h"
#include <optional>
#include <thread>

namespace Calico {

template<class T>
class Worker final {
public:
    using Task = std::function<void(T)>;

    Worker(Task task, Size capacity)
        : m_state {std::move(task), capacity},
          m_thread {run, &m_state}
    {}

    ~Worker()
    {
        m_state.queue.finish();
        m_thread.join();
    }

    auto dispatch(T t, bool should_wait = false) -> void
    {
        // Note that we can only wait on a single event at a time.
        if (should_wait) {
            dispatch_and_wait(std::move(t));
        } else {
            dispatch_and_return(std::move(t));
        }
    }

private:
    struct EventWrapper {
        T event;
        bool needs_wait {};
    };

    struct State {
        State(Task task_, Size capacity)
            : task {std::move(task_)},
              queue {capacity}
        {}

        Task task;
        Queue<EventWrapper> queue;
        std::atomic<bool> is_waiting {};
        mutable std::mutex mu;
        std::condition_variable cv;
    };

    static auto run(State *state) -> void
    {
        for (; ; ) {
            auto event = state->queue.dequeue();
            if (!event.has_value())
                break;

            state->task(std::move(event->event));

            if (event->needs_wait) {
                state->is_waiting.store(false);
                state->cv.notify_one();
            }
        }
    }

    auto dispatch_and_return(T t) -> void
    {
        m_state.queue.enqueue(EventWrapper {std::move(t), false});
    }

    auto dispatch_and_wait(T t) -> void
    {
        m_state.is_waiting.store(true);
        m_state.queue.enqueue(EventWrapper {std::move(t), true});
        std::unique_lock lock {m_state.mu};
        m_state.cv.wait(lock, [this] {
            return !m_state.is_waiting.load();
        });
    }

    State m_state;
    std::thread m_thread;
};

} // namespace Calico

#endif // CALICO_UTILS_WORKER_H

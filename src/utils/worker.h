#ifndef CALICO_UTILS_WORKER_H
#define CALICO_UTILS_WORKER_H

#include "utils/queue.h"
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
    struct Event {
        T value;
        bool needs_wait {};
    };

    struct State {
        State(Task task_, Size capacity)
            : task {std::move(task_)},
              queue {capacity}
        {}

        Task task;
        Queue<Event> queue;
        mutable std::mutex mu;
        std::condition_variable cv;
        bool is_waiting {};
    };

    static auto run(State *state) -> void
    {
        for (; ; ) {
            auto event = state->queue.dequeue();
            if (!event.has_value()) {
                break;
            }

            state->task(std::move(event->value));

            if (event->needs_wait) {
                std::lock_guard lock {state->mu};
                state->is_waiting = false;
                state->cv.notify_one();
            }
        }
    }

    auto dispatch_and_return(T t) -> void
    {
        m_state.queue.enqueue(Event {std::move(t), false});
    }

    auto dispatch_and_wait(T t) -> void
    {
        {
            std::lock_guard lock {m_state.mu};
            m_state.is_waiting = true;
            m_state.queue.enqueue(Event {std::move(t), true});
        }
        std::unique_lock lock {m_state.mu};
        m_state.cv.wait(lock, [this] {
            return !m_state.is_waiting;
        });
    }

    State m_state;
    std::thread m_thread;
};

} // namespace Calico

#endif // CALICO_UTILS_WORKER_H

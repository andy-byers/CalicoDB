#ifndef CALICO_WAL_WORKER_H
#define CALICO_WAL_WORKER_H

#include "utils/queue.h"
#include "wal/helpers.h"
#include "wal/wal.h"
#include <optional>
#include <thread>

namespace Calico {

template<class Event>
class Worker final {
public:
    using Action = std::function<Status(const Event&)>;

    Worker(Size capacity, Action action)
        : m_action {std::move(action)},
          m_events {capacity},
          m_thread {[this] {worker();}}
    {}

    ~Worker()
    {
        if (m_thread.joinable())
            (void)std::move(*this).destroy();
    }

    [[nodiscard]]
    auto status() const -> Status
    {
        if (m_is_ok.load(std::memory_order_acquire))
            return ok();
        // If m_is_ok is false, the background thread must already be finished
        // setting the error status.
        return m_status;
    }

    template<class E>
    auto dispatch(E &&event, bool should_wait = false) -> void
    {
        // Note that we can only wait on a single event at a time.
        if (should_wait) {
            dispatch_and_wait(std::forward<E>(event));
        } else {
            dispatch_and_return(std::forward<E>(event));
        }
    }

    [[nodiscard]]
    auto destroy() && -> Status
    {
        m_events.finish();
        m_thread.join();
        return status();
    }

private:
    auto worker() -> void
    {
        for (; ; ) {
            auto event = m_events.dequeue();
            if (!event.has_value())
                break;
            
            if (m_is_ok.load()) {
                auto s = m_action(event->event);
                maybe_store_error(std::move(s));
            }
            if (event->needs_wait) {
                m_is_waiting.store(false);
                m_cv.notify_one();
            }
        }
    }

    auto maybe_store_error(Status s) -> void
    {
        if (!s.is_ok()) {
            m_status = std::move(s);
            
            // We won't check status unless m_is_ok is false. This makes sure
            // m_status is set before that happens.
            m_is_ok.store(false, std::memory_order_release);
        }
    }

    template<class E>
    auto dispatch_and_return(E &&event) -> void
    {
        m_events.enqueue(EventWrapper {std::forward<E>(event), false});
    }

    template<class E>
    auto dispatch_and_wait(E &&event) -> void
    {
        m_is_waiting.store(true);
        m_events.enqueue(EventWrapper {std::forward<E>(event), true});
        std::unique_lock lock {m_mu};
        m_cv.wait(lock, [this] {
            return !m_is_waiting.load();
        });
    }

    struct EventWrapper {
        Event event;
        bool needs_wait {};
    };

    Action m_action;
    std::atomic<bool> m_is_ok {true};
    std::atomic<bool> m_is_waiting {};
    Queue<EventWrapper> m_events;
    Status m_status {ok()};

    mutable std::mutex m_mu;
    std::condition_variable m_cv;
    std::thread m_thread;
};

class TaskManager final {
public:
    using Task = std::function<void()>;
    using Interval = std::chrono::duration<double>;

    explicit TaskManager(Interval interval)
        : m_thread {[interval](auto *state) {
              for (; ; ) {
                  std::unique_lock lock {state->mutex};
                  state->cond.wait_for(lock, interval, [state] {
                      return !state->enabled;
                  });

                  if (!state->enabled)
                      break;

                  for (const auto &task: state->tasks)
                      task();
              }
        }, &m_state}
    {}

    ~TaskManager()
    {
        {
            std::lock_guard lock {m_state.mutex};
            m_state.enabled = false;
        }
        m_thread.join();
    }

    auto add(Task task) -> void
    {
        std::lock_guard lock {m_state.mutex};
        m_state.tasks.emplace_back(std::move(task));
    }

private:
    struct State {
        std::vector<Task> tasks;
        mutable std::mutex mutex;
        std::condition_variable cond;
        bool enabled {true};
    };

    State m_state;
    std::thread m_thread;
};

} // namespace Calico

#endif // CALICO_WAL_WORKER_H

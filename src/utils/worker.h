#ifndef CALICO_WAL_WORKER_H
#define CALICO_WAL_WORKER_H

#include "utils/queue.h"
#include "wal/helpers.h"
#include "wal/wal.h"
#include <optional>
#include <thread>

namespace calico {

template<class Event>
class BackgroundWorker final {
public:
    using OnEvent = std::function<Status(const Status&, const Event&)>;
    using OnCleanup = std::function<void(const Status&)>;

    BackgroundWorker(OnEvent on_event, OnCleanup on_cleanup)
        : m_on_event {std::move(on_event)},
          m_on_cleanup {std::move(on_cleanup)},
          m_thread {[this] {worker();}}
    {}

    ~BackgroundWorker() = default;

    [[nodiscard]]
    auto status() const -> Status
    {
        if (m_is_ok.load())
            return Status::ok();
        std::lock_guard lock {m_mu};
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
            if (!event.has_value()) {
                m_on_cleanup(m_status);
                break;
            }

            auto s = m_on_event(m_status, event->event);
            if (!s.is_ok()) {
                std::lock_guard lock {m_mu};
                m_status = std::move(s);
                m_is_ok.store(false);
            }
            if (event->needs_wait) {
                m_is_waiting.store(false);
                m_cv.notify_one();
            }
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

    OnEvent m_on_event;
    OnCleanup m_on_cleanup;
    std::atomic<bool> m_is_ok {true};
    std::atomic<bool> m_is_waiting {};
    Queue<EventWrapper> m_events;
    Status m_status {Status::ok()};

    mutable std::mutex m_mu;
    std::condition_variable m_cv;
    std::thread m_thread;
};

} // namespace calico

#endif // CALICO_WAL_WORKER_H

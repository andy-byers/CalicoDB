//
//#ifndef CCO_WAL_EVENT_QUEUE_H
//#define CCO_WAL_EVENT_QUEUE_H
//
//#include <queue>
//#include <tuple>
//#include <variant>
//#include "wal_record.h"
//#include "page/update.h"
//
//namespace cco {
//
//template<Index Tag, class ...Data>
//struct Event {
//    template<class ...Param>
//    explicit constexpr Event(Param &&...param)
//        : data {std::forward<Param>(param)...}
//    {}
//
//    static constexpr Index tag = Tag;
//    std::tuple<Data...> data;
//};
//
//template<class ...Events>
//class EventQueue {
//public:
//    using EventWrapper = std::variant<Events...>;
//
//    [[nodiscard]]
//    auto is_empty() -> bool
//    {
//        return m_events.empty();
//    }
//
//    template<class E>
//    auto enqueue(E &&event) -> void
//    {
//        m_events.push(std::forward<E>(event));
//    }
//
//    [[nodiscard]]
//    auto dequeue() -> EventWrapper
//    {
//        CCO_EXPECT_FALSE(is_empty());
//        const auto event = std::move(m_events.front());
//        m_events.pop();
//        return event;
//    }
//
//private:
//    std::queue<EventWrapper> m_events;
//};
//
//} // namespace cco
//
//#endif // CCO_WAL_EVENT_QUEUE_H

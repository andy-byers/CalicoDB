#ifndef CUB_UTILS_LOGGER_H
#define CUB_UTILS_LOGGER_H

#include <fstream>
#include <mutex>
#include <fmt/format.h>
#include "file/file.h"

namespace cub {

class LogSink {
public:
    LogSink(std::string name, std::streambuf *stream, std::mutex &mutex)
        : m_name {std::move(name)}
        , m_mutex {&mutex}
        , m_os {stream} {}

    template<class First, class ...Rest> auto append(First &&first, Rest &&...rest) -> void
    {
        std::lock_guard lock {*m_mutex};
        append_aux(header(), std::forward<First>(first), std::forward<Rest>(rest)...);
    }

    template<class First, class ...Rest> auto append_line(First &&first, Rest &&...rest) -> void
    {
        append(std::forward<First>(first), std::forward<Rest>(rest)..., '\n');
    }

    auto flush() -> void
    {
        m_os.flush();
    }

private:
    template<class Message, class ...Rest> auto append_aux(Message &&message, Rest &&...rest) -> void
    {
        m_os << message;
        append_aux(std::forward<Rest>(rest)...);
    }

    // Base case.
    auto append_aux() -> void {}

    auto header() -> std::string
    {
        return fmt::format("[{}] ", m_name);
    }

    std::string m_name;
    std::mutex *m_mutex;
    std::ostream m_os;
};

class Logger {
public:
    static auto instance() -> Logger&;
    auto get_sink(const std::string&) -> std::unique_ptr<LogSink>;
    auto set_target(const std::string&) -> void;
    auto set_target(int) -> void;

private:
    Logger() = default;
    mutable std::mutex m_mutex;
    std::ofstream m_ofs;
    std::streambuf *m_stream {};
};

} // cub

#endif // CUB_UTILS_LOGGER_H

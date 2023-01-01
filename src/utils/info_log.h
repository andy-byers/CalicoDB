#ifndef CALICO_UTILS_LOGGING_H
#define CALICO_UTILS_LOGGING_H

#include <numeric>
#include <optional>
#include <spdlog/spdlog.h>
#include "expect.h"
#include "calico/options.h"
#include "calico/status.h"

namespace calico {

constexpr auto LOG_FILENAME = "log";

using Log = spdlog::logger;
using LogPtr = std::shared_ptr<spdlog::logger>;
using LogSink = spdlog::sink_ptr;

auto create_log(LogSink, const std::string_view &) -> LogPtr;
auto create_sink(const std::string_view &, LogLevel) -> LogSink;
auto create_sink() -> LogSink;


struct Error {
    enum Level: Size {
        WARN,
        ERROR,

        // Should be used for internal errors.
        PANIC,
    };

    Status status;
    Level priority {};
};

class System {
public:
    // Number of critical sections we are currently in.
    std::atomic<int> critical {};

    //
    std::atomic<bool> fragile {};

    System(const std::string_view &base, LogLevel log_level, LogTarget log_target);
    auto create_log(const std::string_view &name) const -> LogPtr;
    auto push_error(Error::Level level, Status status) -> Size;
    auto pop_error() -> std::optional<Error>;

private:
    mutable std::mutex m_mutex;
    std::vector<Error> m_errors;
    LogSink m_sink;
    LogPtr m_log;
};

class ThreePartMessage {
public:
    ThreePartMessage() = default;

    template<class... Args>
    auto set_primary(const std::string_view &format, Args &&...args) -> void
    {
        return set_text(PRIMARY, format, std::forward<Args>(args)...);
    }

    template<class... Args>
    auto set_detail(const std::string_view &format, Args &&...args) -> void
    {
        return set_text(DETAIL, format, std::forward<Args>(args)...);
    }

    template<class... Args>
    auto set_hint(const std::string_view &format, Args &&...args) -> void
    {
        return set_text(HINT, format, std::forward<Args>(args)...);
    }

    [[nodiscard]] auto system_error() const -> Status;
    [[nodiscard]] auto invalid_argument() const -> Status;
    [[nodiscard]] auto logic_error() const -> Status;
    [[nodiscard]] auto corruption() const -> Status;
    [[nodiscard]] auto not_found() const -> Status;
    [[nodiscard]] auto text() const -> std::string;

private:
    static constexpr Size PRIMARY {0};
    static constexpr Size DETAIL {1};
    static constexpr Size HINT {2};

    auto set_text(Size, const char *) -> void;

    template<class... Args>
    auto set_text(Size index, const std::string_view &format, Args &&...args) -> void
    {
        set_text(index, fmt::format(fmt::runtime(format), std::forward<Args>(args)...).c_str());
    }

    std::string m_text[3];
};

class LogMessage {
public:
    explicit LogMessage(Log &logger)
        : m_logger {&logger}
    {}

    template<class... Args>
    auto set_primary(const std::string_view &format, Args &&...args) -> void
    {
        return m_message.set_primary(format, std::forward<Args>(args)...);
    }

    template<class... Args>
    auto set_detail(const std::string_view &format, Args &&...args) -> void
    {
        return m_message.set_detail(format, std::forward<Args>(args)...);
    }

    template<class... Args>
    auto set_hint(const std::string_view &format, Args &&...args) -> void
    {
        return m_message.set_hint(format, std::forward<Args>(args)...);
    }

    [[nodiscard]] auto system_error(spdlog::level::level_enum = spdlog::level::err) const -> Status;
    [[nodiscard]] auto invalid_argument(spdlog::level::level_enum = spdlog::level::err) const -> Status;
    [[nodiscard]] auto corruption(spdlog::level::level_enum = spdlog::level::err) const -> Status;
    [[nodiscard]] auto logic_error(spdlog::level::level_enum = spdlog::level::err) const -> Status;
    [[nodiscard]] auto not_found(spdlog::level::level_enum = spdlog::level::err) const -> Status;
    auto log(spdlog::level::level_enum = spdlog::level::err) const -> std::string;

private:
    ThreePartMessage m_message;
    Log *m_logger {};
};

} // namespace calico

#endif // CALICO_UTILS_LOGGING_H

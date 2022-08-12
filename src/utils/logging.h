#ifndef CCO_UTILS_LOGGING_H
#define CCO_UTILS_LOGGING_H

#include "calico/options.h"
#include "calico/status.h"
#include "expect.h"
#include <numeric>
#include <spdlog/spdlog.h>

namespace cco {

constexpr auto LOG_FILENAME = "log";

#define CCO_STRINGIFY_(x) #x
#define CCO_STRINGIFY(x) CCO_STRINGIFY_(x)
#define CCO_LOG_FORMAT(s) ("[" CCO_STRINGIFY(__FILE__) ":" CCO_STRINGIFY(__LINE__) "] "(s))

auto create_logger(spdlog::sink_ptr, const std::string &) -> std::shared_ptr<spdlog::logger>;
auto create_sink(const std::string &, spdlog::level::level_enum) -> spdlog::sink_ptr;
auto create_sink() -> spdlog::sink_ptr;

class ThreePartMessage {
public:
    ThreePartMessage() = default;

    template<class... Args>
    auto set_primary(const std::string &format, Args &&...args) -> void
    {
        return set_text(PRIMARY, format, std::forward<Args>(args)...);
    }

    template<class... Args>
    auto set_detail(const std::string &format, Args &&...args) -> void
    {
        return set_text(DETAIL, format, std::forward<Args>(args)...);
    }

    template<class... Args>
    auto set_hint(const std::string &format, Args &&...args) -> void
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
    static constexpr Index PRIMARY = 0;
    static constexpr Index DETAIL = 1;
    static constexpr Index HINT = 2;

    auto set_text(Index, const char *) -> void;

    template<class... Args>
    auto set_text(Index index, const std::string &format, Args &&...args) -> void
    {
        set_text(index, fmt::format(format, std::forward<Args>(args)...).c_str());
    }

    std::string m_text[3];
};

class LogMessage {
public:
    explicit LogMessage(spdlog::logger &logger)
        : m_logger {&logger}
    {}

    template<class... Args>
    auto set_primary(const std::string &format, Args &&...args) -> void
    {
        return m_message.set_primary(format, std::forward<Args>(args)...);
    }

    template<class... Args>
    auto set_detail(const std::string &format, Args &&...args) -> void
    {
        return m_message.set_detail(format, std::forward<Args>(args)...);
    }

    template<class... Args>
    auto set_hint(const std::string &format, Args &&...args) -> void
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
    spdlog::logger *m_logger {};
};

} // namespace cco

#endif // CCO_UTILS_LOGGING_H

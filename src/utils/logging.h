#ifndef CCO_UTILS_LOGGING_H
#define CCO_UTILS_LOGGING_H

#include <numeric>
#include <spdlog/spdlog.h>
#include "calico/error.h"
#include "calico/options.h"
#include "expect.h"

namespace cco::utils {

constexpr auto LOG_NAME = "log";

#define CCO_STRINGIFY_(x) #x
#define CCO_STRINGIFY(x) CCO_STRINGIFY_(x)
#define CCO_LOG_FORMAT(s) ("[" CCO_STRINGIFY(__FILE__) ":" CCO_STRINGIFY(__LINE__) "] " (s))

auto create_logger(spdlog::sink_ptr, const std::string&) -> std::shared_ptr<spdlog::logger>;
auto create_sink(const std::string&, spdlog::level::level_enum) -> spdlog::sink_ptr;

class ThreePartMessage {
public:
    ThreePartMessage() = default;

    template<class ...Args>
    auto set_primary(const char *format, Args &&...args) -> void
    {
        return set_text(PRIMARY, format, std::forward<Args>(args)...);
    }

    template<class ...Args>
    auto set_detail(const char *format, Args &&...args) -> void
    {
        return set_text(DETAIL, format, std::forward<Args>(args)...);
    }

    template<class ...Args>
    auto set_hint(const char *format, Args &&...args) -> void
    {
        return set_text(HINT, format, std::forward<Args>(args)...);
    }

    [[nodiscard]] auto system_error() const -> Error;
    [[nodiscard]] auto invalid_argument() const -> Error;
    [[nodiscard]] auto logic_error() const -> Error;
    [[nodiscard]] auto corruption() const -> Error;
    [[nodiscard]] auto not_found() const -> Error;
    [[nodiscard]] auto text() const -> std::string;

private:
    static constexpr Index PRIMARY = 0;
    static constexpr Index DETAIL = 1;
    static constexpr Index HINT = 2;

    auto set_text(Index, const char*) -> void;

    template<class ...Args>
    auto set_text(Index index, const char *format, Args &&...args) -> void
    {
        set_text(index, fmt::format(format, std::forward<Args>(args)...).c_str());
    }

    std::string m_text[3];
};

class LogMessage {
public:
    explicit LogMessage(spdlog::logger &logger):
          m_logger {&logger} {}

    template<class ...Args>
    auto set_primary(const char *format, Args &&...args) -> void
    {
        return m_message.set_primary(format, std::forward<Args>(args)...);
    }

    template<class ...Args>
    auto set_detail(const char *format, Args &&...args) -> void
    {
        return m_message.set_detail(format, std::forward<Args>(args)...);
    }

    template<class ...Args>
    auto set_hint(const char *format, Args &&...args) -> void
    {
        return m_message.set_hint(format, std::forward<Args>(args)...);
    }

    [[nodiscard]] auto system_error(spdlog::level::level_enum = spdlog::level::err) const -> Error;
    [[nodiscard]] auto invalid_argument(spdlog::level::level_enum = spdlog::level::err) const -> Error;
    [[nodiscard]] auto logic_error(spdlog::level::level_enum = spdlog::level::err) const -> Error;
    [[nodiscard]] auto corruption(spdlog::level::level_enum = spdlog::level::err) const -> Error;
    [[nodiscard]] auto not_found(spdlog::level::level_enum = spdlog::level::err) const -> Error;
    auto log(spdlog::level::level_enum = spdlog::level::err) const -> std::string;

private:
    ThreePartMessage m_message;
    spdlog::logger *m_logger {};
};

class MessageGroup {
public:
    explicit MessageGroup(spdlog::logger &logger):
          m_logger {&logger} {}

    template<class ...Args>
    auto set_primary(const char *format, Args &&...args) -> void
    {
        m_primary = fmt::format(format, std::forward<Args>(args)...);
    }

    template<class ...Args>
    auto push(Args &&...args) -> void
    {
        m_text.emplace_back(fmt::format(std::forward<Args>(args)...));
    }

    auto log(spdlog::level::level_enum = spdlog::level::err) const -> void;

private:
    std::string m_primary;
    std::vector<std::string> m_text;
    spdlog::logger *m_logger {};
};

} // calico::utils

#endif // CCO_UTILS_LOGGING_H

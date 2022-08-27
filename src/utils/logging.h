#ifndef CALICO_UTILS_LOGGING_H
#define CALICO_UTILS_LOGGING_H

#include "calico/options.h"
#include "calico/status.h"
#include "expect.h"
#include <numeric>
#include <spdlog/spdlog.h>

namespace calico {

constexpr auto LOG_FILENAME = "log";

#define CALICO_STRINGIFY_(x) #x
#define CALICO_STRINGIFY(x) CALICO_STRINGIFY_(x)
#define CALICO_LOG_FORMAT(s) ("[" CALICO_STRINGIFY(__FILE__) ":" CALICO_STRINGIFY(__LINE__) "] "(s))

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
    static constexpr Size PRIMARY {0};
    static constexpr Size DETAIL {1};
    static constexpr Size HINT {2};

    auto set_text(Size, const char *) -> void;

    template<class... Args>
    auto set_text(Size index, const std::string &format, Args &&...args) -> void
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

} // namespace calico

#endif // CALICO_UTILS_LOGGING_H

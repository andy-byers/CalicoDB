#ifndef CCO_UTILS_LOGGING_H
#define CCO_UTILS_LOGGING_H

#include <numeric>
#include <spdlog/spdlog.h>
#include "calico/error.h"
#include "calico/options.h"
#include "expect.h"

namespace cco::utils {

constexpr auto LOG_NAME = "log";

auto create_logger(spdlog::sink_ptr, const std::string&) -> std::shared_ptr<spdlog::logger>;
auto create_sink(const std::string&, spdlog::level::level_enum) -> spdlog::sink_ptr;


class LogMessage {
public:
    explicit LogMessage(spdlog::logger &logger):
          m_logger {&logger} {}

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

    auto system_error(spdlog::level::level_enum level = spdlog::level::err) -> Error
    {
        return Error::system_error(log(level));
    }

    auto invalid_argument(spdlog::level::level_enum level = spdlog::level::err) -> Error
    {
        return Error::invalid_argument(log(level));
    }

    auto logic_error(spdlog::level::level_enum level = spdlog::level::err) -> Error
    {
        return Error::logic_error(log(level));
    }

    auto corruption(spdlog::level::level_enum level = spdlog::level::err) -> Error
    {
        return Error::corruption(log(level));
    }

    auto not_found(spdlog::level::level_enum level = spdlog::level::err) -> Error
    {
        return Error::not_found(log(level));
    }

    auto log(spdlog::level::level_enum level = spdlog::level::err) -> std::string
    {
        auto message = assemble_message();
        m_logger->log(level, message);
        return message;
    }

private:
    static constexpr Index PRIMARY = 0;
    static constexpr Index DETAIL = 1;
    static constexpr Index HINT = 2;

    template<class ...Args>
    auto set_text(Index index, const char *format, Args &&...args) -> void
    {
        set_text(index, fmt::format(format, std::forward<Args>(args)...).c_str());
    }

    auto set_text(Index index, const char *text) -> void
    {
        m_text[index] = text;
    }

    [[nodiscard]] auto assemble_message() const -> std::string
    {
        CCO_EXPECT_FALSE(m_text[PRIMARY].empty());
        std::string message {m_text[PRIMARY]};

        if (!m_text[DETAIL].empty())
            message = fmt::format("{}: {}", message, m_text[DETAIL]);

        if (!m_text[HINT].empty())
            message = fmt::format("{} ({})", message, m_text[HINT]);

        return message;
    }

    std::string m_text[3];
    spdlog::logger *m_logger {};
};

// TODO: Move to LogMessage and get rid of this!
class ErrorMessage {
public:
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

    auto warn(spdlog::logger &logger) -> std::string
    {
        auto message = assemble_message();
        logger.warn(message);
        return message;
    }

    auto error(spdlog::logger &logger) -> std::string
    {
        auto message = assemble_message();
        logger.error(message);
        return message;
    }

    auto log(spdlog::logger &logger, spdlog::level::level_enum level = spdlog::level::trace) const -> void
    {
        logger.log(level, assemble_message());
    }

    auto system_error(spdlog::logger &logger) -> Error
    {
        auto message = assemble_message();
        logger.error(message);
        return Error::system_error(message);
    }

    auto invalid_argument(spdlog::logger &logger) -> Error
    {
        auto message = assemble_message();
        logger.error(message);
        return Error::invalid_argument(message);
    }

    auto logic_error(spdlog::logger &logger) -> Error
    {
        auto message = assemble_message();
        logger.error(message);
        return Error::logic_error(message);
    }

    auto corruption(spdlog::logger &logger) -> Error
    {
        auto message = assemble_message();
        logger.error(message);
        return Error::corruption(message);
    }

private:
    static constexpr Index PRIMARY = 0;
    static constexpr Index DETAIL = 1;
    static constexpr Index HINT = 2;

    template<class ...Args>
    auto set_text(Index index, const char *format, Args &&...args) -> void
    {
        set_text(index, fmt::format(format, std::forward<Args>(args)...).c_str());
    }

    auto set_text(Index index, const char *text) -> void
    {
        m_text[index] = text;
    }

    [[nodiscard]] auto assemble_message() const -> std::string
    {
        CCO_EXPECT_FALSE(m_text[PRIMARY].empty());
        std::string message {m_text[PRIMARY]};

        if (!m_text[DETAIL].empty())
            message = fmt::format("{}: {}", message, m_text[DETAIL]);

        if (!m_text[HINT].empty())
            message = fmt::format("{} ({})", message, m_text[HINT]);

        return message;
    }

    std::string m_text[3];
};


class NumberedGroup {
public:
    template<class ...Args>
    auto push_line(Args &&...args) -> void
    {
        m_text.emplace_back(fmt::format(std::forward<Args>(args)...));
    }

    auto log(spdlog::logger &logger, spdlog::level::level_enum level = spdlog::level::trace) const -> void
    {
        Index i {};
        for (const auto &line: m_text)
            logger.log(level, "({}/{}):", ++i, m_text.size(), line);
    }

private:
    std::vector<std::string> m_text;
};

} // calico::utils

#endif // CCO_UTILS_LOGGING_H

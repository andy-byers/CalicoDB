#ifndef CALICO_UTILS_LOGGING_H
#define CALICO_UTILS_LOGGING_H

#include <numeric>
#include <spdlog/spdlog.h>
#include "calico/options.h"
#include "expect.h"

namespace calico::logging {

auto create_logger(spdlog::sink_ptr, const std::string&) -> std::shared_ptr<spdlog::logger>;
auto create_sink(const std::string&, unsigned) -> spdlog::sink_ptr;

class MessageGroup {
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

    template<class Exception>
    auto log_and_throw(spdlog::logger &logger) -> void
    {
        auto message = assemble_message();
        logger.error(message);
        throw Exception {message};
    }

    auto log(spdlog::logger &logger, spdlog::level::level_enum level = spdlog::level::trace) const -> void
    {
        logger.log(level, assemble_message());
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
        CALICO_EXPECT_FALSE(m_text[PRIMARY].empty());
        std::string message {m_text[PRIMARY]};

        if (!m_text[DETAIL].empty())
            message = fmt::format("{}: {}", message, m_text[DETAIL]);

        if (!m_text[HINT].empty())
            message = fmt::format("{} ({})", message, m_text[HINT]);

        return message;
    }

    std::string m_text[3];
};

} // calico::logging

#endif // CALICO_UTILS_LOGGING_H

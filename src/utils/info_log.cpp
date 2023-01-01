#include "info_log.h"
#include "expect.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/sinks/stdout_sinks.h"
#include "spdlog/sinks/null_sink.h"
#include <filesystem>

namespace calico {

namespace fs = std::filesystem;

static constexpr auto to_spdlog_level(Error::Level level)
{
    switch (level) {
        case Error::Level::WARN:
            return spdlog::level::warn;
        case Error::Level::ERROR:
            return spdlog::level::err;
        default:
            return spdlog::level::critical;
    }
}

static constexpr auto to_spdlog_level(LogLevel level)
{
    switch (level) {
        case LogLevel::INFO:
            return spdlog::level::info;
        case LogLevel::WARN:
            return spdlog::level::warn;
        case LogLevel::ERROR:
            return spdlog::level::err;
        default:
            return spdlog::level::off;
    }
}

System::System(const std::string_view &base, LogLevel log_level, LogTarget log_target)
{
    spdlog::level::level_enum level;
    switch (log_level) {
        case LogLevel::INFO:
            level = spdlog::level::trace;
            break;
        case LogLevel::WARN:
            level = spdlog::level::warn;
            break;
        case LogLevel::ERROR:
            level = spdlog::level::err;
            break;
        case LogLevel::OFF:
            level = spdlog::level::off;
            m_sink = std::make_shared<spdlog::sinks::null_sink_mt>();
            break;
        default:
            push_error(Error::ERROR, Status::invalid_argument("unrecognized log level"));
    }
    if (level != spdlog::level::off) {
        switch (log_target) {
            case LogTarget::FILE:
                CALICO_EXPECT_FALSE(base.empty());
                m_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(fs::path {base} / LOG_FILENAME);
                break;
            case LogTarget::STDOUT:
                m_sink = std::make_shared<spdlog::sinks::stdout_sink_mt>();
                break;
            case LogTarget::STDERR:
                m_sink = std::make_shared<spdlog::sinks::stderr_sink_mt>();
                break;
            case LogTarget::STDOUT_COLOR:
                m_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
                break;
            case LogTarget::STDERR_COLOR:
                m_sink = std::make_shared<spdlog::sinks::stderr_color_sink_mt>();
                break;
            default:
                push_error(Error::ERROR, Status::invalid_argument("unrecognized log target"));
        }
    }
    m_sink->set_level(level);
    m_log = create_log("system");
}

auto System::create_log(const std::string_view &name) const -> LogPtr
{
    CALICO_EXPECT_FALSE(name.empty());
    return std::make_shared<Log>(std::string {name}, m_sink);
}

auto System::push_error(Error::Level level, Status status) -> Size
{
    m_log->log(to_spdlog_level(level), status.what());

    std::lock_guard lock {m_mutex};
    if (level >= Error::ERROR)
        m_errors.emplace_back(Error {std::move(status), level});
    return m_errors.size();
}

auto System::pop_error() -> std::optional<Error>
{
    std::lock_guard lock {m_mutex};
    if (m_errors.empty())
        return std::nullopt;
    auto error = std::move(m_errors.back());
    m_errors.pop_back();
    return error;
}

auto ThreePartMessage::system_error() const -> Status
{
    return Status::system_error(text());
}

auto ThreePartMessage::invalid_argument() const -> Status
{
    return Status::invalid_argument(text());
}

auto ThreePartMessage::logic_error() const -> Status
{
    return Status::logic_error(text());
}

auto ThreePartMessage::corruption() const -> Status
{
    return Status::corruption(text());
}

auto ThreePartMessage::not_found() const -> Status
{
    return Status::not_found(text());
}

auto ThreePartMessage::text() const -> std::string
{
    CALICO_EXPECT_FALSE(m_text[PRIMARY].empty());
    std::string message {m_text[PRIMARY]};

    if (!m_text[DETAIL].empty())
        message = fmt::format("{}: {}", message, m_text[DETAIL]);

    if (!m_text[HINT].empty())
        message = fmt::format("{} ({})", message, m_text[HINT]);

    return message;
}

auto ThreePartMessage::set_text(Size index, const char *text) -> void
{
    m_text[index] = text;
}

auto LogMessage::system_error(spdlog::level::level_enum level) const -> Status
{
    return Status::system_error(log(level));
}

auto LogMessage::invalid_argument(spdlog::level::level_enum level) const -> Status
{
    return Status::invalid_argument(log(level));
}

auto LogMessage::logic_error(spdlog::level::level_enum level) const -> Status
{
    return Status::logic_error(log(level));
}

auto LogMessage::not_found(spdlog::level::level_enum level) const -> Status
{
    return Status::not_found(log(level));
}

auto LogMessage::corruption(spdlog::level::level_enum level) const -> Status
{
    return Status::corruption(log(level));
}

auto LogMessage::log(spdlog::level::level_enum level) const -> std::string
{
    auto message = m_message.text();
    m_logger->log(level, message);
    return message;
}

} // namespace calico
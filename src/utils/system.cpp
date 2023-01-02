#include "expect.h"
#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/sinks/null_sink.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/sinks/stdout_sinks.h"
#include "system.h"
#include <filesystem>

namespace Calico {

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
    spdlog::level::level_enum level {spdlog::level::off};

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
            push_error(Error::ERROR, invalid_argument("unrecognized log level"));
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
                push_error(Error::ERROR, invalid_argument("unrecognized log target"));
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

auto System::push_error(Error::Level level, Status status) -> void
{
    // All errors get logged.
    m_log->log(to_spdlog_level(level), status.what());

    // Only severe errors get saved.
    if (level >= Error::ERROR) {
        std::lock_guard lock {m_mutex};
        m_errors.emplace_back(Error {std::move(status), level});
        m_has_error.store(true);
    }
}

auto System::original_error() const -> Error
{
    std::lock_guard lock {m_mutex};
    CALICO_EXPECT_FALSE(m_errors.empty());
    return m_errors.front();
}

auto System::pop_error() -> Error
{
    std::lock_guard lock {m_mutex};
    CALICO_EXPECT_FALSE(m_errors.empty());
    auto error = std::move(m_errors.back());
    m_errors.pop_back();
    if (m_errors.empty())
        m_has_error.store(false);
    return error;
}

auto System::has_error() const -> bool
{
    return m_has_error.load();
}

} // namespace Calico
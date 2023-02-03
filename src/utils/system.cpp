#include "spdlog/sinks/rotating_file_sink.h"
#include "spdlog/sinks/null_sink.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/sinks/stdout_sinks.h"
#include "system.h"
#include <filesystem>

namespace Calico {

namespace fs = std::filesystem;

System::System(const std::string &prefix, const Options &options)
{
    spdlog::level::level_enum level;
    auto s = ok();

    switch (options.log_level) {
        case LogLevel::TRACE:
            level = spdlog::level::trace;
            break;
        case LogLevel::INFO:
            level = spdlog::level::info;
            break;
        case LogLevel::WARN:
            level = spdlog::level::warn;
            break;
        case LogLevel::ERROR:
            level = spdlog::level::err;
            break;
        default:
            level = spdlog::level::off;
            m_sink = std::make_shared<spdlog::sinks::null_sink_mt>();
    }

    if (level != spdlog::level::off) {
        switch (options.log_target) {
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
                CALICO_EXPECT_FALSE(prefix.empty());
                m_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(prefix + LOG_FILENAME, options.max_log_size, options.max_log_files);
        }
    }
    m_sink->set_level(level);
}

auto System::create_log(const std::string &name) const -> LogPtr
{
    CALICO_EXPECT_FALSE(name.empty());
    return std::make_shared<Log>(name, m_sink);
}

} // namespace Calico
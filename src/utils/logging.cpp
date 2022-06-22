#include "logging.h"
#include "expect.h"
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/null_sink.h>

namespace calico::logging {

auto create_logger(spdlog::sink_ptr sink, const std::string &name) -> std::shared_ptr<spdlog::logger>
{
    CALICO_EXPECT_FALSE(name.empty());
    return std::make_shared<spdlog::logger>(name, std::move(sink));
}

auto create_sink(const std::string &path, unsigned level) -> spdlog::sink_ptr
{
    spdlog::sink_ptr sink;
    if (path.empty()) {
        sink = std::make_shared<spdlog::sinks::null_sink_mt>();
    } else {
        sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(path);
    }
    sink->set_level(static_cast<spdlog::level::level_enum>(level));
    return sink;
}

} // calico::logging
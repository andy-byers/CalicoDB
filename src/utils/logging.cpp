#include "logging.h"
#include <filesystem>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/null_sink.h>
#include "expect.h"

namespace calico::utils {

namespace fs = std::filesystem;

auto create_logger(spdlog::sink_ptr sink, const std::string &name) -> std::shared_ptr<spdlog::logger>
{
    CALICO_EXPECT_FALSE(name.empty());
    return std::make_shared<spdlog::logger>(name, std::move(sink));
}

auto create_sink(const std::string &base, spdlog::level::level_enum level) -> spdlog::sink_ptr
{
    spdlog::sink_ptr sink;
    if (base.empty()) {
        sink = std::make_shared<spdlog::sinks::null_sink_mt>();
    } else {
        sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(fs::path {base} / LOG_NAME);
    }
    sink->set_level(level);
    return sink;
}

} // calico::utils
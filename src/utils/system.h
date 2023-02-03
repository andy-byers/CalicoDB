#ifndef CALICO_UTILS_LOGGING_H
#define CALICO_UTILS_LOGGING_H

#include <numeric>
#include <optional>
#include <spdlog/spdlog.h>
#include "types.h"
#include "calico/database.h"
#include "calico/status.h"

namespace Calico {

constexpr auto LOG_FILENAME = "log";

using Log = spdlog::logger;
using LogPtr = std::shared_ptr<spdlog::logger>;
using LogSink = spdlog::sink_ptr;

#define Calico_Trace m_log->trace
#define Calico_Info m_log->info
#define Calico_Warn m_log->warn
#define Calico_Error m_log->err

class ErrorBuffer {
public:
    [[nodiscard]]
    auto is_ok() const -> bool
    {
        std::lock_guard lock {m_mutex};
        return m_status.is_ok();
    }

    [[nodiscard]]
    auto get() const -> const Status &
    {
        std::lock_guard lock {m_mutex};
        return m_status;
    }

    auto set(Status status) -> void
    {
        CALICO_EXPECT_FALSE(status.is_ok());
        std::lock_guard lock {m_mutex};
        if (m_status.is_ok()) {
            m_status = std::move(status);
        }
    }

private:
    mutable std::mutex m_mutex;
    Status m_status {ok()};
};

class System {
public:
    System(const std::string &prefix, const Options &options);
    [[nodiscard]] auto create_log(const std::string &name) const -> LogPtr;

private:
    LogSink m_sink;
};

} // namespace Calico

#endif // CALICO_UTILS_LOGGING_H
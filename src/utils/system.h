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

namespace Calico {

inline auto append_number(std::string &out, Size value) -> void
{
    Byte buffer[30];
    std::snprintf(buffer, sizeof(buffer), "%llu", static_cast<unsigned long long>(value));
    out.append(buffer);
}

inline auto append_escaped_string(std::string &out, const Slice &value) -> void
{
    for (Size i {}; i < value.size(); ++i) {
        const auto chr = value[i];
        if (chr >= ' ' && chr <= '~') {
            out.push_back(chr);
        } else {
            char buffer[10];
            std::snprintf(buffer, sizeof(buffer), "\\x%02x", static_cast<unsigned>(chr) & 0xFF);
            out.append(buffer);
        }
    }
}

inline auto number_to_string(Size value) -> std::string
{
    std::string out;
    append_number(out, value);
    return out;
}

inline auto escape_string(const Slice &value) -> std::string
{
    std::string out;
    append_escaped_string(out, value);
    return out;
}

} // namespace Calico

#endif // CALICO_UTILS_LOGGING_H
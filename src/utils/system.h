#ifndef CALICO_UTILS_LOGGING_H
#define CALICO_UTILS_LOGGING_H

#include <numeric>
#include <optional>
#include <spdlog/spdlog.h>
#include "expect.h"
#include "types.h"
#include "calico/options.h"
#include "calico/status.h"

namespace Calico {

constexpr auto LOG_FILENAME = "log";

using Log = spdlog::logger;
using LogPtr = std::shared_ptr<spdlog::logger>;
using LogSink = spdlog::sink_ptr;

struct Error {
    enum Level {
        WARN,
        ERROR,

        // Should be used for internal errors.
        PANIC,
    };

    Status status;
    Level priority {};
};

#define CALICO_WARN(s) system->push_error(Error::WARN, s)
#define CALICO_ERROR(s) system->push_error(Error::ERROR, s)
#define CALICO_PANIC(s) system->push_error(Error::PANIC, s)
#define CALICO_WARN_IF(expr) do {if (auto calico_warn_s = (expr); !calico_warn_s.is_ok()) CALICO_WARN(calico_warn_s);} while (0)
#define CALICO_ERROR_IF(expr) do {if (auto calico_error_s = (expr); !calico_error_s.is_ok()) CALICO_ERROR(calico_error_s);} while (0)
#define CALICO_PANIC_IF(expr) do {if (auto calico_panic_s = (expr); !calico_panic_s.is_ok() CALICO_PANIC(calico_panic_s);} while (0)

class System {
public:
    // True if we are currently in a transaction, false otherwise.
    bool has_xact {};

    // LSN of the last commit record written to the WAL.
    Lsn commit_lsn {};

    System(const std::string &prefix, const Options &options);
    auto create_log(const std::string_view &name) const -> LogPtr;
    auto push_error(Error::Level level, Status status) -> void;
    auto original_error() const -> Error;
    auto pop_error() -> Error;

    // Check if the system has an error. We should only pop errors from one thread, so if this returns true, it is
    // safe to call pop_error() and top_error().
    [[nodiscard]] auto has_error() const -> bool;

private:
    mutable std::mutex m_mutex;
    std::atomic<bool> m_has_error {};
    std::vector<Error> m_errors;
    LogSink m_sink;
    LogPtr m_log;
};

} // namespace Calico

#endif // CALICO_UTILS_LOGGING_H
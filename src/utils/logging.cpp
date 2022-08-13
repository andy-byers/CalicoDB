#include "logging.h"
#include "expect.h"
#include <filesystem>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/null_sink.h>

namespace calico {

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
        sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(fs::path {base} / LOG_FILENAME);
    }
    sink->set_level(level);
    return sink;
}

auto create_sink() -> spdlog::sink_ptr
{
    return create_sink("", spdlog::level::off);
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

} // namespace cco
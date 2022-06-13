#include "logger.h"
#include <iostream>

namespace cub {

auto Logger::instance() -> Logger&
{
    static Logger logger;
    return logger;
}

auto Logger::get_sink(const std::string &name) -> std::unique_ptr<LogSink>
{
    return std::make_unique<LogSink>(name, m_stream, m_mutex);
}

auto Logger::set_target(const std::string &path) -> void
{
    m_ofs.open(path, std::ios::app);
    if (!m_ofs.is_open())
        throw std::runtime_error {"Unable to open log file"};
    m_stream = m_ofs.rdbuf();
}

auto Logger::set_target(int std_stream) -> void
{
    if (std_stream == STDOUT_FILENO) {
        m_stream = std::cout.rdbuf();
    } else if (std_stream == STDERR_FILENO) {
        m_stream = std::cerr.rdbuf();
    } else {
        throw std::runtime_error {"Invalid file descriptor for standard stream"};
    }
}

} // cub
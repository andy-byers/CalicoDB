// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/env.h"

namespace calicodb
{

Env::Env() = default;

Env::~Env() = default;

File::File() = default;

File::~File() = default;

Logger::Logger() = default;

Logger::~Logger() = default;

auto File::read_exact(size_t offset, size_t size, char *scratch) -> Status
{
    Slice slice;
    auto s = read(offset, size, scratch, &slice);
    if (s.is_ok() && slice.size() != size) {
        return Status::io_error("incomplete read");
    }
    return s;
}

EnvWrapper::EnvWrapper(Env &target)
    : m_target{&target}
{
}

EnvWrapper::~EnvWrapper() = default;

auto EnvWrapper::target() -> Env *
{
    return m_target;
}

auto EnvWrapper::target() const -> const Env *
{
    return m_target;
}

auto EnvWrapper::new_file(const char *filename, OpenMode mode, File *&out) -> Status
{
    return m_target->new_file(filename, mode, out);
}

auto EnvWrapper::new_logger(const char *filename, Logger *&out) -> Status
{
    return m_target->new_logger(filename, out);
}

auto EnvWrapper::file_exists(const char *filename) const -> bool
{
    return m_target->file_exists(filename);
}

auto EnvWrapper::file_size(const char *filename, size_t &out) const -> Status
{
    return m_target->file_size(filename, out);
}

auto EnvWrapper::remove_file(const char *filename) -> Status
{
    return m_target->remove_file(filename);
}

auto EnvWrapper::srand(unsigned seed) -> void
{
    m_target->srand(seed);
}

auto EnvWrapper::rand() -> unsigned
{
    return m_target->rand();
}

auto EnvWrapper::sleep(unsigned micros) -> void
{
    m_target->sleep(micros);
}

auto log(Logger *logger, const char *fmt, ...) -> void
{
    if (logger) {
        std::va_list args;
        va_start(args, fmt);
        logger->logv(fmt, args);
        va_end(args);
    }
}

} // namespace calicodb
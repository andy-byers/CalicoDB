// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/env.h"
#include "env_posix.h"
#include "utils.h"

namespace calicodb
{

File::~File() = default;

auto File::read_exact(std::size_t offset, std::size_t size, char *scratch) -> Status
{
    Slice slice;
    auto s = read(offset, size, scratch, &slice);
    if (s.is_ok() && slice.size() != size) {
        return Status::io_error("incomplete read");
    }
    return s;
}

LogFile::~LogFile() = default;

Env::~Env() = default;

auto Env::default_env() -> Env *
{
    return new EnvPosix;
}

EnvWrapper::~EnvWrapper() = default;

EnvWrapper::EnvWrapper(Env &env)
    : m_target {&env}
{
}

auto EnvWrapper::target() -> Env *
{
    return m_target;
}

auto EnvWrapper::target() const -> const Env *
{
    return m_target;
}

auto EnvWrapper::new_file(const std::string &filename, File *&out) -> Status
{
    return m_target->new_file(filename, out);
}

auto EnvWrapper::new_log_file(const std::string &filename, LogFile *&out) -> Status
{
    return m_target->new_log_file(filename, out);
}

auto EnvWrapper::file_exists(const std::string &filename) const -> bool
{
    return m_target->file_exists(filename);
}

auto EnvWrapper::resize_file(const std::string &filename, std::size_t size) -> Status
{
    return m_target->resize_file(filename, size);
}

auto EnvWrapper::file_size(const std::string &filename, std::size_t &out) const -> Status
{
    return m_target->file_size(filename, out);
}

auto EnvWrapper::remove_file(const std::string &filename) -> Status
{
    return m_target->remove_file(filename);
}

auto EnvWrapper::lock(File &file, LockMode mode) -> Status
{
    return m_target->lock(file, mode);
}

auto EnvWrapper::unlock(File &file) -> Status
{
    return m_target->unlock(file);
}

auto EnvWrapper::srand(unsigned seed) -> void
{
    m_target->srand(seed);
}

auto EnvWrapper::rand() -> unsigned
{
    return m_target->rand();
}

} // namespace calicodb
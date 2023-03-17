// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/env.h"
#include "env_posix.h"

namespace calicodb
{

Editor::~Editor() = default;

InfoLogger::~InfoLogger() = default;

Logger::~Logger() = default;

Reader::~Reader() = default;

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

auto EnvWrapper::new_reader(const std::string &filename, Reader *&out) -> Status
{
    return m_target->new_reader(filename, out);
}

auto EnvWrapper::new_editor(const std::string &filename, Editor *&out) -> Status
{
    return m_target->new_editor(filename, out);
}

auto EnvWrapper::new_logger(const std::string &filename, Logger *&out) -> Status
{
    return m_target->new_logger(filename, out);
}

auto EnvWrapper::new_info_logger(const std::string &filename, InfoLogger *&out) -> Status
{
    return m_target->new_info_logger(filename, out);
}

auto EnvWrapper::get_children(const std::string &dirname, std::vector<std::string> &out) const -> Status
{
    return m_target->get_children(dirname, out);
}

auto EnvWrapper::rename_file(const std::string &old_filename, const std::string &new_filename) -> Status
{
    return m_target->rename_file(old_filename, new_filename);
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

auto EnvWrapper::sync_directory(const std::string &dirname) -> Status
{
    return m_target->remove_file(dirname);
}

} // namespace calicodb
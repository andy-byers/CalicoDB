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

auto EnvWrapper::new_reader(const std::string &path, Reader *&out) -> Status
{
    return m_target->new_reader(path, out);
}

auto EnvWrapper::new_editor(const std::string &path, Editor *&out) -> Status
{
    return m_target->new_editor(path, out);
}

auto EnvWrapper::new_logger(const std::string &path, Logger *&out) -> Status
{
    return m_target->new_logger(path, out);
}

auto EnvWrapper::new_info_logger(const std::string &path, InfoLogger *&out) -> Status
{
    return m_target->new_info_logger(path, out);
}

auto EnvWrapper::get_children(const std::string &path, std::vector<std::string> &out) const -> Status
{
    return m_target->get_children(path, out);
}

auto EnvWrapper::rename_file(const std::string &old_path, const std::string &new_path) -> Status
{
    return m_target->rename_file(old_path, new_path);
}

auto EnvWrapper::file_exists(const std::string &path) const -> bool
{
    return m_target->file_exists(path);
}

auto EnvWrapper::resize_file(const std::string &path, std::size_t size) -> Status
{
    return m_target->resize_file(path, size);
}

auto EnvWrapper::file_size(const std::string &path, std::size_t &out) const -> Status
{
    return m_target->file_size(path, out);
}

auto EnvWrapper::remove_file(const std::string &path) -> Status
{
    return m_target->remove_file(path);
}

} // namespace calicodb
// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#include "calicodb/env.h"
#include "env_posix.h"
#include "utils.h"

namespace calicodb
{

auto Env::default_env() -> Env *
{
    return new PosixEnv;
}

Env::Env() = default;

Env::~Env() = default;

File::File() = default;

File::~File() = default;

Sink::Sink() = default;

Sink::~Sink() = default;

auto File::read_exact(std::size_t offset, std::size_t size, char *scratch) -> Status
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

auto EnvWrapper::new_file(const std::string &filename, OpenMode mode, File *&out) -> Status
{
    return m_target->new_file(filename, mode, out);
}

auto EnvWrapper::new_sink(const std::string &filename, Sink *&out) -> Status
{
    return m_target->new_sink(filename, out);
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

FileWrapper::FileWrapper(File &target)
    : m_target{&target}
{
}

FileWrapper::~FileWrapper() = default;

auto FileWrapper::target() -> File *
{
    return m_target;
}

auto FileWrapper::target() const -> const File *
{
    return m_target;
}

auto FileWrapper::read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status
{
    return m_target->read(offset, size, scratch, out);
}

auto FileWrapper::read_exact(std::size_t offset, std::size_t size, char *scratch) -> Status
{
    return m_target->read_exact(offset, size, scratch);
}

auto FileWrapper::write(std::size_t offset, const Slice &in) -> Status
{
    return m_target->write(offset, in);
}

auto FileWrapper::sync() -> Status
{
    return m_target->sync();
}

auto FileWrapper::file_lock(FileLockMode mode) -> Status
{
    return m_target->file_lock(mode);
}

auto FileWrapper::file_unlock() -> void
{
    return m_target->file_unlock();
}

auto FileWrapper::shm_map(std::size_t r, volatile void *&out) -> Status
{
    return m_target->shm_map(r, out);
}

auto FileWrapper::shm_lock(std::size_t r, std::size_t n, ShmLockFlag flags) -> Status
{
    return m_target->shm_lock(r, n, flags);
}

auto FileWrapper::shm_unmap(bool unlink) -> void
{
    return m_target->shm_unmap(unlink);
}

auto FileWrapper::shm_barrier() -> void
{
    return m_target->shm_barrier();
}

SinkWrapper::SinkWrapper(Sink &target)
    : m_target{&target}
{
}

SinkWrapper::~SinkWrapper() = default;

auto SinkWrapper::target() -> Sink *
{
    return m_target;
}

auto SinkWrapper::target() const -> const Sink *
{
    return m_target;
}

auto SinkWrapper::sink(const Slice &in) -> void
{
    return m_target->sink(in);
}

} // namespace calicodb
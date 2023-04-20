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

Env::~Env() = default;

File::~File() = default;

Sink::~Sink() = default;

Shm::~Shm() = default;

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

auto EnvWrapper::open_file(const std::string &filename, OpenMode mode, File *&out) -> Status
{
    return m_target->open_file(filename, mode, out);
}

auto EnvWrapper::close_file(File *&file) -> Status
{
    return m_target->close_file(file);
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

auto FileWrapper::lock(LockMode mode) -> Status
{
    return m_target->lock(mode);
}

auto FileWrapper::lock_mode() const -> LockMode
{
    return m_target->lock_mode();
}

auto FileWrapper::unlock(LockMode mode) -> Status
{
    return m_target->unlock(mode);
}

auto EnvWrapper::open_shm(const std::string &filename, OpenMode mode, Shm *&out) -> Status
{
    return m_target->open_shm(filename, mode, out);
}

auto EnvWrapper::close_shm(Shm *&shm) -> Status
{
    return m_target->close_shm(shm);
}

ShmWrapper::ShmWrapper(Shm &target)
    : m_target{&target}
{
}

ShmWrapper::~ShmWrapper() = default;

auto ShmWrapper::target() -> Shm *
{
    return m_target;
}

auto ShmWrapper::target() const -> const Shm *
{
    return m_target;
}

auto ShmWrapper::map(std::size_t pgno, volatile void *&out) -> Status
{
    return m_target->map(pgno, out);
}

auto ShmWrapper::lock(std::size_t start, std::size_t n, LockFlag flags) -> Status
{
    return m_target->lock(start, n, flags);
}

auto ShmWrapper::barrier() -> void
{
    return m_target->barrier();
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
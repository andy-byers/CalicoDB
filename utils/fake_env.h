// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_UTILS_FAKE_ENV_H
#define CALICODB_UTILS_FAKE_ENV_H

#include <unordered_map>
#include <vector>
#include "calicodb/env.h"

namespace calicodb
{

class FakeEnv : public Env
{
public:
    [[nodiscard]] virtual auto clone() const -> Env *;
    [[nodiscard]] virtual auto get_file_contents(const std::string &filename) const -> std::string;
    virtual auto put_file_contents(const std::string &filename, std::string contents) -> void;

    ~FakeEnv() override = default;
    auto new_logger(const std::string &filename, Logger *&out) -> Status override;
    auto new_file(const std::string &filename, OpenMode mode, File *&out) -> Status override;
    [[nodiscard]] auto file_exists(const std::string &filename) const -> bool override;
    auto resize_file(const std::string &filename, std::size_t size) -> Status override;
    [[nodiscard]] auto file_size(const std::string &filename, std::size_t &out) const -> Status override;
    auto remove_file(const std::string &filename) -> Status override;

    auto srand(unsigned seed) -> void override;
    [[nodiscard]] auto rand() -> unsigned override;

    auto sleep(unsigned) -> void override {}

protected:
    friend class FakeFile;
    friend class TestEnv;

    struct FileState {
        std::string buffer;
        bool created = false;
    };

    auto read_file_at(const FileState &mem, std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status;
    auto write_file_at(FileState &mem, std::size_t offset, const Slice &in) -> Status;

    mutable std::unordered_map<std::string, FileState> m_state;
};

class FakeFile : public File
{
public:
    FakeFile(std::string filename, FakeEnv &env, FakeEnv::FileState &mem)
        : m_state(&mem),
          m_env(&env),
          m_filename(std::move(filename))
    {
    }

    ~FakeFile() override = default;
    auto read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status override;
    auto write(std::size_t offset, const Slice &in) -> Status override;
    auto sync() -> Status override;
    auto file_lock(FileLockMode) -> Status override { return Status::ok(); }
    auto shm_map(std::size_t r, bool extend, volatile void *&out) -> Status override;
    auto shm_lock(std::size_t, std::size_t, ShmLockFlag) -> Status override { return Status::ok(); }
    auto shm_unmap(bool unlink) -> void override;
    auto shm_barrier() -> void override {}
    auto file_unlock() -> void override {}

    [[nodiscard]] auto env() -> FakeEnv &
    {
        return *m_env;
    }

    [[nodiscard]] auto env() const -> const FakeEnv &
    {
        return *m_env;
    }

    [[nodiscard]] auto filename() -> const std::string &
    {
        return m_filename;
    }

protected:
    FakeEnv::FileState *m_state = nullptr;
    FakeEnv *m_env = nullptr;
    std::string m_filename;
    std::vector<std::string> m_shm;
};

} // namespace calicodb

#endif // CALICODB_UTILS_FAKE_ENV_H

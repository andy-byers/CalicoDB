// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_UTILS_FAKE_ENV_H
#define CALICODB_UTILS_FAKE_ENV_H

#include "calicodb/env.h"
#include <string>
#include <unordered_map>
#include <vector>

namespace calicodb
{

class FakeEnv : public Env
{
public:
    [[nodiscard]] virtual auto clone() const -> Env *;
    [[nodiscard]] virtual auto get_file_contents(const char *filename) const -> std::string *;

    ~FakeEnv() override = default;

    auto max_filename() const -> size_t override
    {
        return default_env().max_filename();
    }

    auto full_filename(const char *filename, char *out, size_t out_size) -> Status override
    {
        return default_env().full_filename(filename, out, out_size);
    }

    auto new_logger(const char *filename, Logger *&out) -> Status override;
    auto new_file(const char *filename, OpenMode mode, File *&out) -> Status override;
    [[nodiscard]] auto file_exists(const char *filename) const -> bool override;
    auto remove_file(const char *filename) -> Status override;

    void srand(unsigned seed) override;
    [[nodiscard]] auto rand() -> unsigned override;

    void sleep(unsigned) override {}

protected:
    friend class FakeFile;
    friend class TestEnv;

    struct FileState {
        std::string buffer;
        bool created = false;
    };

    auto read_file_at(const FileState &mem, size_t offset, size_t size, char *scratch, Slice *out) -> Status;
    auto write_file_at(FileState &mem, size_t offset, const Slice &in) -> Status;

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
    auto read(uint64_t offset, size_t size, char *scratch, Slice *out) -> Status override;
    auto write(uint64_t offset, const Slice &in) -> Status override;
    auto get_size(uint64_t &size_out) const -> Status override;
    auto resize(uint64_t size) -> Status override;
    auto sync() -> Status override;
    auto file_lock(FileLockMode) -> Status override { return Status::ok(); }
    auto shm_map(size_t r, bool extend, volatile void *&out) -> Status override;
    auto shm_lock(size_t, size_t, ShmLockFlag) -> Status override { return Status::ok(); }
    void shm_unmap(bool unlink) override;
    void shm_barrier() override {}
    void file_unlock() override {}

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

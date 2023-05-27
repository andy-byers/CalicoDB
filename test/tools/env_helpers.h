// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_TOOLS_TEST_ENV_H
#define CALICODB_TOOLS_TEST_ENV_H

#include "tools.h"

namespace calicodb::tools
{

class StreamSink : public Sink
{
    mutable std::mutex *m_mu;
    std::ostream *m_os;

    auto sink_and_flush(const Slice &in) -> void
    {
        m_os->write(in.data(), static_cast<std::streamsize>(in.size()));
        m_os->flush();
    }

public:
    explicit StreamSink(std::ostream &os, std::mutex *mu = nullptr)
        : m_os(&os),
          m_mu(mu)
    {
    }

    auto sink(const Slice &in) -> void override
    {
        if (m_mu) {
            std::lock_guard lock(*m_mu);
            sink_and_flush(in);
        } else {
            sink_and_flush(in);
        }
    }
};

class FakeEnv : public Env
{
public:
    [[nodiscard]] virtual auto clone() const -> Env *;
    [[nodiscard]] virtual auto get_file_contents(const std::string &filename) const -> std::string;
    virtual auto put_file_contents(const std::string &filename, std::string contents) -> void;

    ~FakeEnv() override = default;
    [[nodiscard]] auto new_sink(const std::string &filename, Sink *&out) -> Status override;
    [[nodiscard]] auto new_file(const std::string &filename, OpenMode mode, File *&out) -> Status override;
    [[nodiscard]] auto file_exists(const std::string &filename) const -> bool override;
    [[nodiscard]] auto resize_file(const std::string &filename, std::size_t size) -> Status override;
    [[nodiscard]] auto file_size(const std::string &filename, std::size_t &out) const -> Status override;
    [[nodiscard]] auto remove_file(const std::string &filename) -> Status override;

    auto srand(unsigned seed) -> void override;
    [[nodiscard]] auto rand() -> unsigned override;

    auto sleep(unsigned) -> void override {}

protected:
    friend class FakeFile;
    friend class FakeSink;
    friend class TestEnv;

    struct FileState {
        std::string buffer;
        bool created = false;
    };

    [[nodiscard]] auto open_or_create_file(const std::string &filename) const -> FileState &;
    [[nodiscard]] auto read_file_at(const FileState &mem, std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status;
    [[nodiscard]] auto write_file_at(FileState &mem, std::size_t offset, const Slice &in) -> Status;

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
    [[nodiscard]] auto read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status override;
    [[nodiscard]] auto write(std::size_t offset, const Slice &in) -> Status override;
    [[nodiscard]] auto sync() -> Status override;
    [[nodiscard]] auto file_lock(FileLockMode) -> Status override { return Status::ok(); }
    [[nodiscard]] auto shm_map(std::size_t r, bool extend, volatile void *&out) -> Status override;
    [[nodiscard]] auto shm_lock(std::size_t, std::size_t, ShmLockFlag) -> Status override { return Status::ok(); }
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

using SyscallType = U64;
static constexpr SyscallType kSyscallRead = 1;
static constexpr SyscallType kSyscallWrite = kSyscallRead << 1;
static constexpr SyscallType kSyscallOpen = kSyscallWrite << 1;
static constexpr SyscallType kSyscallSync = kSyscallOpen << 1;
static constexpr SyscallType kSyscallUnlink = kSyscallSync << 1;
static constexpr SyscallType kSyscallResize = kSyscallUnlink << 1;
static constexpr SyscallType kSyscallFileLock = kSyscallResize << 1;
static constexpr SyscallType kSyscallShmMap = kSyscallFileLock << 1;
static constexpr SyscallType kSyscallShmLock = kSyscallShmMap << 1;
static constexpr std::size_t kNumSyscalls = 9;

using Callback = std::function<Status()>;

struct Interceptor {
    explicit Interceptor(SyscallType t, Callback c)
        : callback(std::move(c)),
          type(t)
    {
    }

    [[nodiscard]] auto operator()() const -> Status
    {
        return callback();
    }

    Callback callback;
    SyscallType type;
};

struct FileCounters {
    std::size_t values[kNumSyscalls] = {};
};

class TestEnv : public EnvWrapper
{
public:
    explicit TestEnv();
    explicit TestEnv(Env &env);
    ~TestEnv() override;

    // NOTE: clone() always clones files into a FakeEnv, and only works properly if
    //       the wrapped Env was empty when passed to the constructor.
    [[nodiscard]] virtual auto clone() -> Env *;

    virtual auto copy_file(const std::string &source, const std::string &target) -> void;

    // The TestFile wrapper reads the whole file and saves it in memory after a
    // successful call to sync().
    virtual auto drop_after_last_sync(const std::string &filename) -> void;
    virtual auto drop_after_last_sync() -> void;

    virtual auto add_interceptor(const std::string &filename, Interceptor interceptor) -> void;
    virtual auto clear_interceptors() -> void;
    virtual auto clear_interceptors(const std::string &filename) -> void;

    [[nodiscard]] auto new_file(const std::string &filename, OpenMode mode, File *&out) -> Status override;
    [[nodiscard]] auto resize_file(const std::string &filename, std::size_t file_size) -> Status override;
    [[nodiscard]] auto remove_file(const std::string &filename) -> Status override;

    [[nodiscard]] auto find_counters(const std::string &filename) -> const FileCounters *;

private:
    friend class TestFile;

    struct FileState {
        std::list<Interceptor> interceptors;
        FileCounters counters;
        std::string saved_state;
        bool unlinked = false;
    };

    mutable std::unordered_map<std::string, FileState> m_state;
    mutable std::mutex m_mutex;

    [[nodiscard]] auto try_intercept_syscall(SyscallType type, const std::string &filename) -> Status;
    auto get_state(const std::string &filename) -> FileState &;
    auto save_file_contents(const std::string &filename) -> void;
    auto overwrite_file(const std::string &filename, const std::string &contents) -> void;
};

class TestFile : public FileWrapper
{
    friend class TestEnv;

    std::string m_filename;
    TestEnv *m_env;
    TestEnv::FileState *m_state;

    explicit TestFile(std::string filename, File &file, TestEnv &env, TestEnv::FileState &state);

public:
    ~TestFile() override;
    [[nodiscard]] auto read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status override;
    [[nodiscard]] auto read_exact(std::size_t offset, std::size_t size, char *out) -> Status override;
    [[nodiscard]] auto write(std::size_t offset, const Slice &in) -> Status override;
    [[nodiscard]] auto sync() -> Status override;
    [[nodiscard]] auto file_lock(FileLockMode mode) -> Status override;
    [[nodiscard]] auto shm_map(std::size_t r, bool extend, volatile void *&ptr_out) -> Status override;
    [[nodiscard]] auto shm_lock(std::size_t r, std::size_t n, ShmLockFlag flag) -> Status override;
};

} // namespace calicodb::tools

#endif // CALICODB_TOOLS_TEST_ENV_H

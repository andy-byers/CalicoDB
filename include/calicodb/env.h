// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_ENV_H
#define CALICODB_ENV_H

#include "status.h"
#include <cstdarg>

namespace calicodb
{

class File;
class Logger;

// CalicoDB storage environment
// Handles platform-specific filesystem manipulations and file locking.
class Env
{
public:
    // Return a handle to this platform's default `Env` implementation
    // The returned handle belongs to CalicoDB and must not be `delete`d. Note that this method
    // always returns the same address for a given process.
    static auto default_env() -> Env *;

    explicit Env();
    virtual ~Env();

    Env(Env &) = delete;
    void operator=(Env &) = delete;

    enum OpenMode : int {
        kCreate = 1,
        kReadOnly = 2,
        kReadWrite = 4,
    };
    [[nodiscard]] virtual auto new_file(const std::string &filename, OpenMode mode, File *&out) -> Status = 0;
    [[nodiscard]] virtual auto new_logger(const std::string &filename, Logger *&out) -> Status = 0;
    [[nodiscard]] virtual auto resize_file(const std::string &filename, std::size_t size) -> Status = 0;
    [[nodiscard]] virtual auto file_size(const std::string &filename, std::size_t &out) const -> Status = 0;
    [[nodiscard]] virtual auto remove_file(const std::string &filename) -> Status = 0;
    [[nodiscard]] virtual auto file_exists(const std::string &filename) const -> bool = 0;

    virtual auto srand(unsigned seed) -> void = 0;
    [[nodiscard]] virtual auto rand() -> unsigned = 0;

    virtual auto sleep(unsigned micros) -> void = 0;
};

// Available modes for the file locking API
// NOTE: File locking modes and semantics are from SQLite.
enum FileLockMode : int {
    kLockShared = 1,    // Any number of threads can hold a kShared lock
    kLockExclusive = 2, // Excludes all other locks
};

// Available flags for the shared memory locking API
// NOTE: shm locking modes and semantics are from SQLite.
enum ShmLockFlag : int {
    kShmUnlock = 1,
    kShmLock = 2,
    kShmReader = 4,
    kShmWriter = 8,
};

class File
{
public:
    explicit File();
    virtual ~File();

    File(File &) = delete;
    void operator=(File &) = delete;

    // Attempt to read `size` bytes from the file at the given offset.
    //
    // Reads into `scratch`, which must point to at least `size` bytes of available
    // memory. On success, sets "*out" to point to the data that was read, which may
    // be less than what was requested, but only if `out` is not nullptr.
    [[nodiscard]] virtual auto read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status = 0;

    // Read exactly `size` bytes from the file at the given offset.
    //
    // Return a "not found" status if there is not enough data remaining in the file.
    [[nodiscard]] virtual auto read_exact(std::size_t offset, std::size_t size, char *scratch) -> Status;

    // Write `in` to the file at the given offset.
    [[nodiscard]] virtual auto write(std::size_t offset, const Slice &in) -> Status = 0;

    // Synchronize with the underlying filesystem.
    [[nodiscard]] virtual auto sync() -> Status = 0;

    // Take or upgrade a lock on the file
    [[nodiscard]] virtual auto file_lock(FileLockMode mode) -> Status = 0;

    // Release a lock on the file
    virtual auto file_unlock() -> void = 0;

    // Size of a shared memory region, i.e. the number of bytes pointed to by `out`
    // when `shm_map()` returns successfully.
    static constexpr std::size_t kShmRegionSize = 1'024 * 32;
    static constexpr std::size_t kShmLockCount = 8;

    [[nodiscard]] virtual auto shm_map(std::size_t r, bool extend, volatile void *&out) -> Status = 0;
    [[nodiscard]] virtual auto shm_lock(std::size_t r, std::size_t n, ShmLockFlag flags) -> Status = 0;
    virtual auto shm_unmap(bool unlink) -> void = 0;
    virtual auto shm_barrier() -> void = 0;
};

class Logger
{
public:
    explicit Logger();
    virtual ~Logger();

    Logger(Logger &) = delete;
    void operator=(Logger &) = delete;

    virtual auto logv(const char *fmt, std::va_list args) -> void = 0;
};

auto log(Logger *sink, const char *fmt, ...) -> void;

class EnvWrapper : public Env
{
public:
    explicit EnvWrapper(Env &target);
    [[nodiscard]] auto target() -> Env *;
    [[nodiscard]] auto target() const -> const Env *;

    ~EnvWrapper() override;

    [[nodiscard]] auto new_logger(const std::string &filename, Logger *&out) -> Status override;
    [[nodiscard]] auto new_file(const std::string &filename, OpenMode mode, File *&out) -> Status override;
    [[nodiscard]] auto resize_file(const std::string &filename, std::size_t size) -> Status override;
    [[nodiscard]] auto file_size(const std::string &filename, std::size_t &out) const -> Status override;
    [[nodiscard]] auto remove_file(const std::string &filename) -> Status override;
    [[nodiscard]] auto file_exists(const std::string &filename) const -> bool override;

    auto srand(unsigned seed) -> void override;
    [[nodiscard]] auto rand() -> unsigned override;

    auto sleep(unsigned micros) -> void override;

private:
    Env *m_target;
};

// Allow composition of flags with OR
inline constexpr auto operator|(Env::OpenMode lhs, Env::OpenMode rhs) -> Env::OpenMode
{
    return Env::OpenMode{
        static_cast<int>(lhs) |
        static_cast<int>(rhs)};
}
inline constexpr auto operator|(ShmLockFlag lhs, ShmLockFlag rhs) -> ShmLockFlag
{
    return ShmLockFlag{
        static_cast<int>(lhs) |
        static_cast<int>(rhs)};
}

} // namespace calicodb

#endif // CALICODB_ENV_H

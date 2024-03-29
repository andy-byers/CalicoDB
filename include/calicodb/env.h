// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_ENV_H
#define CALICODB_ENV_H

#include "slice.h"
#include "status.h"
#include <cstdarg>
#include <cstdint>

namespace calicodb
{

// calicodb/env.h (below)
class Env;
class File;
class Logger;

// Return a reference to a singleton implementing the Env interface for this platform
[[nodiscard]] auto default_env() -> Env &;

// CalicoDB storage environment
// Handles platform-specific filesystem manipulations and file locking.
class Env
{
public:
    explicit Env();
    virtual ~Env();

    Env(Env &) = delete;
    void operator=(Env &) = delete;

    enum OpenMode : int {
        kCreate = 1,
        kReadOnly = 2,
        kReadWrite = 4,
    };
    virtual auto new_file(const char *filename, OpenMode mode, File *&out) -> Status = 0;
    virtual auto new_logger(const char *filename, Logger *&out) -> Status = 0;

    [[nodiscard]] virtual auto file_exists(const char *filename) const -> bool = 0;
    [[nodiscard]] virtual auto max_filename() const -> size_t = 0;
    virtual auto full_filename(const char *filename, char *out, size_t out_size) -> Status = 0;
    virtual auto remove_file(const char *filename) -> Status = 0;

    virtual void srand(unsigned seed) = 0;
    virtual auto rand() -> unsigned = 0;

    virtual void sleep(unsigned micros) = 0;
};

// Available modes for the file locking API
// NOTE: File locking modes and semantics are from SQLite.
enum FileLockMode {
    kFileShared = 1, // Any number of connections can hold a kShared lock
    kFileExclusive,  // Excludes all other locks
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
    virtual auto read(uint64_t offset, size_t size, char *scratch, Slice *out) -> Status = 0;

    // Read exactly `size` bytes from the file at the given offset.
    //
    // Return a "not found" status if there is not enough data remaining in the file.
    virtual auto read_exact(uint64_t offset, size_t size, char *scratch) -> Status;

    // Write `in` to the file at the given offset.
    virtual auto write(uint64_t offset, const Slice &in) -> Status = 0;

    // Determine the file size in bytes
    virtual auto get_size(uint64_t &size_out) const -> Status = 0;

    // Set the file `size` in bytes
    virtual auto resize(uint64_t size) -> Status = 0;

    // Synchronize with the underlying filesystem.
    virtual auto sync() -> Status = 0;

    // Take or upgrade a lock on the file
    virtual auto file_lock(FileLockMode mode) -> Status = 0;

    // Release a lock on the file
    virtual void file_unlock() = 0;

    // Size of a shared memory region, i.e. the number of bytes pointed to by `out`
    // when `shm_map()` returns successfully.
    static constexpr size_t kShmRegionSize = 1'024 * 32;
    static constexpr size_t kShmLockCount = 8;

    virtual auto shm_map(size_t r, bool extend, volatile void *&out) -> Status = 0;
    virtual auto shm_lock(size_t r, size_t n, ShmLockFlag flags) -> Status = 0;
    virtual void shm_unmap(bool unlink) = 0;
    virtual void shm_barrier() = 0;
};

class Logger
{
public:
    explicit Logger();
    virtual ~Logger();

    Logger(Logger &) = delete;
    void operator=(Logger &) = delete;

    virtual void append(const Slice &msg) = 0;
    virtual void logv(const char *fmt, std::va_list args) = 0;
};

void log(Logger *sink, const char *fmt, ...);

class EnvWrapper : public Env
{
public:
    explicit EnvWrapper(Env &target);
    [[nodiscard]] auto target() -> Env *;
    [[nodiscard]] auto target() const -> const Env *;

    ~EnvWrapper() override;

    auto new_logger(const char *filename, Logger *&out) -> Status override;
    auto new_file(const char *filename, OpenMode mode, File *&out) -> Status override;

    [[nodiscard]] auto file_exists(const char *filename) const -> bool override;
    [[nodiscard]] auto max_filename() const -> size_t override;
    auto full_filename(const char *filename, char *out, size_t out_size) -> Status override;
    auto remove_file(const char *filename) -> Status override;

    void srand(unsigned seed) override;
    auto rand() -> unsigned override;

    void sleep(unsigned micros) override;

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

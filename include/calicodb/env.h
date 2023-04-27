// Copyright (c) 2022, The CalicoDB Authors. All rights reserved.
// This source code is licensed under the MIT License, which can be found in
// LICENSE.md. See AUTHORS.md for a list of contributor names.

#ifndef CALICODB_ENV_H
#define CALICODB_ENV_H

#include "status.h"

namespace calicodb
{

class File;
class Sink;

// CalicoDB storage environment
//
// Handles platform-specific filesystem manipulations and file locking.
class Env
{
public:
    enum OpenMode : int {
        kCreate = 1,
        kReadOnly = 2,
        kReadWrite = 4,
    };

    // Return a heap-allocated handle to this platform's default Env implementation
    static auto default_env() -> Env *;

    explicit Env();
    virtual ~Env();

    Env(Env &) = delete;
    void operator=(Env &) = delete;

    [[nodiscard]] virtual auto new_sink(const std::string &filename, Sink *&out) -> Status = 0;
    [[nodiscard]] virtual auto new_file(const std::string &filename, OpenMode mode, File *&out) -> Status = 0;
    [[nodiscard]] virtual auto resize_file(const std::string &filename, std::size_t size) -> Status = 0;
    [[nodiscard]] virtual auto file_size(const std::string &filename, std::size_t &out) const -> Status = 0;
    [[nodiscard]] virtual auto remove_file(const std::string &filename) -> Status = 0;
    [[nodiscard]] virtual auto file_exists(const std::string &filename) const -> bool = 0;

    virtual auto srand(unsigned seed) -> void = 0;
    [[nodiscard]] virtual auto rand() -> unsigned = 0;
};

class File
{
public:
    explicit File();
    virtual ~File();

    File(File &) = delete;
    void operator=(File &) = delete;

    // Attempt to read "size" bytes from the file at the given offset.
    //
    // Reads into "scratch", which must point to at least "size" bytes of available
    // memory. On success, sets "*out" to point to the data that was read, which may
    // be less than what was requested, but only if "out" is not nullptr.
    [[nodiscard]] virtual auto read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status = 0;

    // Read exactly "size" bytes from the file at the given offset.
    //
    // Return a "not found" status if there is not enough data remaining in the file.
    [[nodiscard]] virtual auto read_exact(std::size_t offset, std::size_t size, char *scratch) -> Status;

    // Write "in" to the file at the given offset.
    [[nodiscard]] virtual auto write(std::size_t offset, const Slice &in) -> Status = 0;

    // Synchronize with the underlying filesystem.
    [[nodiscard]] virtual auto sync() -> Status = 0;

    // Available modes for the file locking API
    // NOTE: File locking modes and semantics are from SQLite.
    enum FileLockMode : int {
        kUnlocked, // TODO: May end up adding a FileLockMode parameter to file_unlock(), which would then support downgrading to either kUnlocked or kShared. Otherwise, this can be removed.
        kShared, // Any number of threads can hold a kShared lock
        kReserved, // Compatible with 1+ kShared, but incompatible with other kReserved
        kPending, // Compatible with 1+ kShared, but excludes new kShared
        kExclusive, // Excludes all locks
    };

    // Take or upgrade a file_lock on the file
    // Return Status::ok() if the file_lock is granted, Status::busy() if an
    // incompatible file_lock is already held by a different thread or process,
    // or a Status::io_error() if a system call otherwise fails. Compliant File
    // implementations must support the following state transitions:
    //     before -> (intermediate) -> after
    //    ----------------------------------------
    //     kUnlocked ----------------> kShared
    //     kShared ------------------> kReserved
    //     kShared ------------------> kExclusive
    //     kReserved --> (kPending) -> kExclusive
    //     kPending -----------------> kExclusive
    // NOTE: The result of a failure to transition from kReserved to kExclusive
    // always results in the caller being left in kPending. Attempting to lock a
    // file with a lower-priority lock is a NOOP.
    [[nodiscard]] virtual auto file_lock(FileLockMode mode) -> Status = 0;

    // Release or downgrade a file_lock on the given file
    //
    // Compliant Envs support the following state transitions (where "> X"
    // represents a file_lock mode with higher-priority than "X"):
    //
    //     Before -----------> After
    //    ------------------------------
    //     > kUnlocked ------> kUnlocked
    //     > kShared --------> kShared
    //
    // Also note that transitions "Y -> Y", where "Y" <= kShared, are
    // allowed, but are NOOPs.
    //
    virtual auto file_unlock() -> void = 0;

    // Size of a shared memory region, i.e. the number of bytes pointed to by "out"
    // when map() returns successfully.
    static constexpr std::size_t kShmRegionSize = 1'024 * 32;
    static constexpr std::size_t kShmLockCount = 8;

    enum ShmLockFlag : int {
        kUnlock = 1,
        kLock = 2,
        kReader = 4,
        kWriter = 8,
    };

    [[nodiscard]] virtual auto shm_map(std::size_t r, volatile void *&out) -> Status = 0;
    [[nodiscard]] virtual auto shm_lock(std::size_t r, std::size_t n, ShmLockFlag flags) -> Status = 0;
    virtual auto shm_unmap(bool unlink) -> void = 0;
    virtual auto shm_barrier() -> void = 0;
};

class Sink
{
public:
    explicit Sink();
    virtual ~Sink();

    Sink(Sink &) = delete;
    void operator=(Sink &) = delete;

    virtual auto sink(const Slice &in) -> void = 0;
};

auto logv(Sink *sink, const char *fmt, ...) -> void;

class EnvWrapper : public Env
{
public:
    explicit EnvWrapper(Env &target);
    [[nodiscard]] auto target() -> Env *;
    [[nodiscard]] auto target() const -> const Env *;

    ~EnvWrapper() override;

    [[nodiscard]] auto new_sink(const std::string &filename, Sink *&out) -> Status override;
    [[nodiscard]] auto new_file(const std::string &filename, OpenMode mode, File *&out) -> Status override;
    [[nodiscard]] auto resize_file(const std::string &filename, std::size_t size) -> Status override;
    [[nodiscard]] auto file_size(const std::string &filename, std::size_t &out) const -> Status override;
    [[nodiscard]] auto remove_file(const std::string &filename) -> Status override;
    [[nodiscard]] auto file_exists(const std::string &filename) const -> bool override;

    auto srand(unsigned seed) -> void override;
    [[nodiscard]] auto rand() -> unsigned override;

private:
    Env *m_target;
};

class FileWrapper : public File
{
public:
    explicit FileWrapper(File &target);
    ~FileWrapper() override;
    [[nodiscard]] auto target() -> File *;
    [[nodiscard]] auto target() const -> const File *;

    [[nodiscard]] auto read(std::size_t offset, std::size_t size, char *scratch, Slice *out) -> Status override;
    [[nodiscard]] auto read_exact(std::size_t offset, std::size_t size, char *scratch) -> Status override;
    [[nodiscard]] auto write(std::size_t offset, const Slice &in) -> Status override;
    [[nodiscard]] auto sync() -> Status override;

    [[nodiscard]] auto file_lock(FileLockMode mode) -> Status override;
    auto file_unlock() -> void override;

    [[nodiscard]] auto shm_map(std::size_t r, volatile void *&out) -> Status override;
    [[nodiscard]] auto shm_lock(std::size_t s, std::size_t n, ShmLockFlag flags) -> Status override;
    auto shm_unmap(bool unlink) -> void override;
    auto shm_barrier() -> void override;

private:
    File *m_target;
};

// For completeness and in case Sink ever gets additional methods.
class SinkWrapper : public Sink
{
public:
    explicit SinkWrapper(Sink &target);
    ~SinkWrapper() override;
    [[nodiscard]] auto target() -> Sink *;
    [[nodiscard]] auto target() const -> const Sink *;

    auto sink(const Slice &in) -> void override;

private:
    Sink *m_target;
};

// Allow composition of flags with OR
inline constexpr auto operator|(Env::OpenMode lhs, Env::OpenMode rhs) -> Env::OpenMode
{
    return Env::OpenMode{
        static_cast<int>(lhs) |
        static_cast<int>(rhs)};
}
inline constexpr auto operator|(File::ShmLockFlag lhs, File::ShmLockFlag rhs) -> File::ShmLockFlag
{
    return File::ShmLockFlag{
        static_cast<int>(lhs) |
        static_cast<int>(rhs)};
}

} // namespace calicodb

#endif // CALICODB_ENV_H
